#include "query_relational.hpp"
#include <algorithm>
#include <limits>

namespace edudb::query {
namespace {
using Run=QuerySpillStore::Run;
class Relational:public ExecNode {
protected:
    ExecContext* context=nullptr;
    json profile;
    bool active=false;
    explicit Relational(std::string name):profile{{"node",std::move(name)},{"loops",0},{"actual_rows",0}}{}
    void begin(ExecContext& c){context=&c;active=true;profile["loops"]=profile["loops"].get<std::uint64_t>()+1;}
    void work(){if(context->work)context->work();}
    void inc(const char* key,std::uint64_t n=1){profile[key]=profile.value(key,std::uint64_t(0))+n;}
    bool emitted(){work();inc("actual_rows");return true;}
    std::size_t bytes(const json& row)const{return context->row_bytes?context->row_bytes(row):row.dump().size()+128;}
    void allocate(const json& row){if(context->materialize)context->materialize(row);}
    void erase(Run& run)noexcept{if(run.id){try{context->spill->erase(run);}catch(...){}run={};}}
public:json diagnostics()const override{return profile;}
};
class Set final:public Relational {
    std::string operation;bool all,concatenate;
    std::unique_ptr<ExecNode> left,right;
    Compare compare;json a,b,current;bool have_a=false,have_b=false,left_done=false;
    std::uint64_t remaining=0;
    std::uint64_t consume(std::unique_ptr<ExecNode>& input,json& row,bool& present,const json& key){
        std::uint64_t count=0;
        do{work();++count;present=input->next(row);}while(present&&compare(row,key)==0);
        return count;
    }
public:
    Set(std::string op,bool multiplicity,std::unique_ptr<ExecNode> l,std::unique_ptr<ExecNode> r,Compare cmp)
        :Relational(op+(multiplicity?"All":"Distinct")),operation(std::move(op)),all(multiplicity),concatenate(operation=="UNION"&&all),left(std::move(l)),right(std::move(r)),compare(std::move(cmp)){
        if(!concatenate){left=query::sort(std::move(left),[](const json& r){return r;},compare);right=query::sort(std::move(right),[](const json& r){return r;},compare);}
    }
    void open(ExecContext& c)override{close();begin(c);left->open(c);if(concatenate){left_done=false;return;}right->open(c);have_a=left->next(a);have_b=right->next(b);}
    bool next(json& row)override{
        if(!active)return false;
        if(concatenate){if(!left_done){if(left->next(row))return emitted();left->close();left_done=true;right->open(*context);}return right->next(row)?emitted():false;}
        for(;;){
            if(remaining){--remaining;row=current;return emitted();}
            if(!have_a&&!have_b)return false;
            int cmp=have_a?(have_b?compare(a,b):-1):1;std::uint64_t l=0,r=0;
            current=cmp<=0?a:b;
            if(cmp<=0)l=consume(left,a,have_a,current);
            if(cmp>=0)r=consume(right,b,have_b,current);
            if(operation=="UNION")remaining=1;
            else if(operation=="INTERSECT")remaining=all?std::min(l,r):std::uint64_t(l&&r);
            else remaining=all?(l>r?l-r:0):std::uint64_t(l&&!r);
        }
    }
    void close()noexcept override{left->close();right->close();have_a=have_b=left_done=false;remaining=0;active=false;}
    json diagnostics()const override{auto p=profile;p["children"]=json::array({left->diagnostics(),right->diagnostics()});return p;}
    ~Set(){close();}
};
class Distinct final:public Relational {
    std::unique_ptr<ExecNode> child;Compare compare;json previous;bool has_previous=false;
public:
    Distinct(std::unique_ptr<ExecNode> n,Compare c):Relational("ExternalDistinct"),compare(std::move(c)){child=query::sort(std::move(n),[](const json&r){return r;},compare);}
    void open(ExecContext& c)override{close();begin(c);child->open(c);}
    bool next(json& row)override{if(!active)return false;while(child->next(row)){work();if(!has_previous||compare(row,previous)){previous=row;has_previous=true;return emitted();}}return false;}
    void close()noexcept override{child->close();previous=nullptr;has_previous=false;active=false;}
    json diagnostics()const override{auto p=profile;p["children"]=json::array({child->diagnostics()});return p;}
    ~Distinct(){close();}
};
class Outer final:public Relational {
    std::string kind;std::unique_ptr<ExecNode> left,right;JoinSpec spec;std::vector<int> left_slots;
    Run run;std::unique_ptr<QuerySpillStore::Reader> reader;
    std::vector<unsigned char> matched_right,matched_left;
    std::vector<json> block;json pending,b;
    bool has_pending=false,have_inner=false,unmatched_phase=false,right_phase=false,finished_left=false;
    std::size_t position=0,unmatched_position=0,max_row_bytes=0;std::uint64_t right_position=0,current_right=0;
    bool keep_left()const{return kind=="LEFT"||kind=="FULL";}
    bool keep_right()const{return kind=="RIGHT"||kind=="FULL";}
    bool fill(){
        block.clear();matched_left.clear();std::size_t used=0;json row;
        while(has_pending||left->next(row)){
            if(has_pending){row=std::move(pending);has_pending=false;}
            auto n=bytes(row)+1;max_row_bytes=std::max(max_row_bytes,n);if(n>context->memory_limit/16)throw DbError("QUERY_OPERATOR_MEMORY","One outer-join row exceeds bounded block/head memory");
            if(used+n>context->memory_limit/2){pending=std::move(row);has_pending=true;break;}
            allocate(row);block.push_back(std::move(row));matched_left.push_back(0);used+=n;
        }
        if(block.empty())return false;
        profile["memory_bytes_estimate_peak"]=std::max(profile.value("memory_bytes_estimate_peak",std::size_t(0)),used+matched_right.size()+3*max_row_bytes);
        reader=context->spill->open(run);right_position=0;have_inner=unmatched_phase=false;unmatched_position=0;inc("outer_blocks");inc("right_run_rescans");return true;
    }
    void right_tail(){reader.reset();block.clear();matched_left.clear();reader=context->spill->open(run);right_position=0;right_phase=true;}
public:
    Outer(std::string k,std::unique_ptr<ExecNode> l,std::unique_ptr<ExecNode> r,JoinSpec s,std::vector<int> slots):Relational("SpoolBlock"+k+"OuterJoin"),kind(std::move(k)),left(std::move(l)),right(std::move(r)),spec(std::move(s)),left_slots(std::move(slots)){}
    void open(ExecContext& c)override{
        close();begin(c);right->open(c);auto writer=context->spill->create();json row;
        while(right->next(row)){work();max_row_bytes=std::max(max_row_bytes,bytes(row));if(bytes(row)>context->memory_limit/16)throw DbError("QUERY_OPERATOR_MEMORY","One outer-join spool row exceeds head memory");writer->append(row);}
        right->close();run=writer->finish();profile["spool_bytes"]=run.bytes;
        if(keep_right()){if(run.rows>std::uint64_t(context->memory_limit/4)*8)throw DbError("QUERY_OPERATOR_MEMORY","Outer-join matched-row bitmap exceeds work memory");matched_right.assign(std::size_t((run.rows+7)/8),0);}
        left->open(c);
    }
    bool next(json& row)override{
        if(!active)return false;
        for(;;){
            if(right_phase){while(reader->next(row)){work();auto i=right_position++;if(!(matched_right[std::size_t(i/8)]&(1u<<(i%8)))){for(int slot:left_slots)row[slot]=nullptr;inc("null_extended_right");return emitted();}}return false;}
            if(finished_left)return false;
            if(block.empty()&&!fill()){left->close();finished_left=true;if(keep_right()){right_tail();continue;}return false;}
            if(unmatched_phase){while(unmatched_position<block.size()){auto i=unmatched_position++;if(keep_left()&&!matched_left[i]){row=block[i];for(int slot:spec.right_slots)row[slot]=nullptr;inc("null_extended_left");return emitted();}}block.clear();reader.reset();continue;}
            if(!have_inner){if(!reader->next(b)){unmatched_phase=true;continue;}current_right=right_position++;position=0;have_inner=true;}
            while(position<block.size()){
                auto i=position++;work();inc("pairs_examined");row=block[i];for(int slot:spec.right_slots)row[slot]=b[slot];
                if(!spec.predicate||spec.predicate(row)){matched_left[i]=1;if(keep_right())matched_right[std::size_t(current_right/8)]|=static_cast<unsigned char>(1u<<(current_right%8));return emitted();}
            }
            have_inner=false;
        }
    }
    void close()noexcept override{reader.reset();left->close();right->close();erase(run);block.clear();matched_left.clear();matched_right.clear();pending=nullptr;has_pending=have_inner=unmatched_phase=right_phase=finished_left=false;max_row_bytes=0;active=false;}
    json diagnostics()const override{auto p=profile;p["children"]=json::array({left->diagnostics(),right->diagnostics()});return p;}
    ~Outer(){close();}
};
class Apply final:public Relational {
    std::unique_ptr<ExecNode> outer,inner;ApplySpec spec;
public:
    Apply(std::unique_ptr<ExecNode> n,ApplySpec s):Relational("CorrelatedApply"),outer(std::move(n)),spec(std::move(s)){profile["mode"]=spec.mode;}
    void open(ExecContext& c)override{close();begin(c);outer->open(c);}
    bool next(json& row)override{
        if(!active||!outer->next(row))return false;
        work();inner=spec.inner(row);inner->open(*context);inc("inner_executions");json r,result=nullptr;
        if(spec.mode=="EXISTS"||spec.mode=="NOT_EXISTS"){bool exists=inner->next(r);result=spec.mode=="EXISTS"?exists:!exists;}
        else if(spec.mode=="SCALAR"){
            if(inner->next(r)){if(!r.is_array()||r.size()!=1)throw DbError("SUBQUERY_COLUMN_COUNT","Scalar subquery requires exactly one output column");result=r[0];if(inner->next(r))throw DbError("CARDINALITY_VIOLATION","Scalar subquery returned more than one row");}
        }else{
            auto value=spec.value(row);bool match=false,unknown=false;
            while(inner->next(r)){work();if(!r.is_array()||r.size()!=1)throw DbError("SUBQUERY_COLUMN_COUNT","IN subquery requires exactly one output column");if(value.is_null()||r[0].is_null())unknown=true;else if(spec.scalar_compare(value,r[0])==0){match=true;break;}}
            if(match)result=true;else if(unknown)result=nullptr;else result=false;
            if(spec.mode=="NOT_IN"&&!result.is_null())result=!result.get<bool>();
        }
        inner->close();inner.reset();if(spec.output_slot<0)throw DbError("INTERNAL_ERROR","Apply output slot was not bound");while(row.size()<=std::size_t(spec.output_slot))row.push_back(nullptr);row[spec.output_slot]=std::move(result);return emitted();
    }
    void close()noexcept override{if(inner)inner->close();inner.reset();outer->close();active=false;}
    json diagnostics()const override{auto p=profile;p["children"]=json::array({outer->diagnostics()});return p;}
    ~Apply(){close();}
};
}
std::unique_ptr<ExecNode> set_operation(const std::string& op,bool all,std::unique_ptr<ExecNode> l,std::unique_ptr<ExecNode> r,Compare cmp){
    if(op!="UNION"&&op!="INTERSECT"&&op!="EXCEPT")throw DbError("UNSUPPORTED_SQL","Unknown set operation");
    return std::make_unique<Set>(op,all,std::move(l),std::move(r),std::move(cmp));
}
std::unique_ptr<ExecNode> distinct(std::unique_ptr<ExecNode> n,Compare compare){return std::make_unique<Distinct>(std::move(n),std::move(compare));}
std::unique_ptr<ExecNode> outer_join(const std::string& kind,std::unique_ptr<ExecNode> l,std::unique_ptr<ExecNode> r,JoinSpec spec,std::vector<int> slots){
    if(kind!="LEFT"&&kind!="RIGHT"&&kind!="FULL")throw DbError("UNSUPPORTED_SQL","Unknown outer join kind");
    return std::make_unique<Outer>(kind,std::move(l),std::move(r),std::move(spec),std::move(slots));
}
std::unique_ptr<ExecNode> apply(std::unique_ptr<ExecNode> n,ApplySpec spec){
    if(spec.mode!="EXISTS"&&spec.mode!="NOT_EXISTS"&&spec.mode!="IN"&&spec.mode!="NOT_IN"&&spec.mode!="SCALAR")throw DbError("UNSUPPORTED_SQL","Unknown correlated Apply mode");
    if(!spec.inner||((spec.mode=="IN"||spec.mode=="NOT_IN")&&(!spec.value||!spec.scalar_compare)))throw DbError("INTERNAL_ERROR","Apply callbacks are not bound");
    return std::make_unique<Apply>(std::move(n),std::move(spec));
}
} // namespace edudb::query
