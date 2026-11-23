#include "query_operators.hpp"
#include <algorithm>
#include <cmath>
#include <map>
#include <queue>
#include <unordered_map>

namespace edudb::query {
std::string equality_key(json key){
    for(auto& v:key)if(v.is_number_float()){
        const double x=v.get<double>();
        if(x>=-9223372036854775808.0&&x<9223372036854775808.0&&std::trunc(x)==x)v=static_cast<std::int64_t>(x);
    }
    return key.dump();
}
namespace {
using Run=QuerySpillStore::Run;
void work(ExecContext* c){if(c->work)c->work();}
std::size_t bytes(ExecContext* c,const json& r){return c->row_bytes?c->row_bytes(r):r.dump().size()+128;}
void allocated(ExecContext* c,const json& r){if(c->materialize)c->materialize(r);}
bool has_null(const json& key){for(const auto& v:key)if(v.is_null())return true;return false;}
std::uint64_t hash_key(const std::string& key){std::uint64_t h=1469598103934665603ull;for(unsigned char c:key){h^=c;h*=1099511628211ull;}return h;}
class Base:public ExecNode{
protected:
    ExecContext* c=nullptr;json stats;bool active=false;
    explicit Base(const std::string& name):stats{{"node",name},{"loops",0},{"actual_rows",0},{"spill_bytes",0},{"memory_bytes_estimate_peak",0}}{}
    void begin(ExecContext& ctx){c=&ctx;active=true;stats["loops"]=stats["loops"].get<std::uint64_t>()+1;}
    bool emitted(){work(c);stats["actual_rows"]=stats["actual_rows"].get<std::uint64_t>()+1;return true;}
    void inc(const char* n,std::uint64_t v=1){stats[n]=stats.value(n,std::uint64_t(0))+v;}
    void peak(std::size_t v){stats["memory_bytes_estimate_peak"]=std::max(stats["memory_bytes_estimate_peak"].get<std::size_t>(),v);}
    void erase(Run& r)noexcept{if(!r.id)return;try{c->spill->erase(r);}catch(...){}r={};}
public:json diagnostics()const override{return stats;}
};
class Scan final:public Base{
    Open factory;Pull pull;
public:Scan(Open f,const std::string& n):Base(n),factory(std::move(f)){}
    void open(ExecContext& ctx)override{close();begin(ctx);pull=factory();}
    bool next(json& r)override{if(!active||!pull||!pull(r))return false;return emitted();}
    void close()noexcept override{pull={};active=false;}
    ~Scan(){close();}
};
class RunScan final:public Base{
    Run run;std::unique_ptr<QuerySpillStore::Reader> reader;
public:explicit RunScan(Run r):Base("TemporaryRunScan"),run(r){}
    void open(ExecContext& ctx)override{close();begin(ctx);reader=c->spill->open(run);}
    bool next(json& r)override{return active&&reader->next(r)?emitted():false;}
    void close()noexcept override{reader.reset();active=false;}
};
class Sort final:public Base{
    std::unique_ptr<ExecNode> child;Key key;Compare compare;
    std::vector<json> memory;std::vector<Run> runs;std::unique_ptr<QuerySpillStore::Reader> output;std::size_t position=0,max_row=1;
    bool less(const json& a,const json& b){work(c);inc("comparisons");return compare(key(a),key(b))<0;}
    void flush(){
        if(memory.empty())return;
        std::stable_sort(memory.begin(),memory.end(),[&](const json&a,const json&b){return less(a,b);});
        auto w=c->spill->create();for(auto& r:memory)w->append(r);auto run=w->finish();inc("spill_bytes",run.bytes);runs.push_back(run);memory.clear();
    }
    Run merge_runs(const std::vector<Run>& group){
        std::vector<std::unique_ptr<QuerySpillStore::Reader>> readers;std::vector<json> heads(group.size());
        auto cmp=[&](std::size_t a,std::size_t b){int v=compare(key(heads[a]),key(heads[b]));return v?v>0:a>b;};
        std::priority_queue<std::size_t,std::vector<std::size_t>,decltype(cmp)> queue(cmp);
        for(std::size_t i=0;i<group.size();++i){readers.push_back(c->spill->open(group[i]));if(readers.back()->next(heads[i]))queue.push(i);}
        auto w=c->spill->create();while(!queue.empty()){work(c);auto i=queue.top();queue.pop();w->append(heads[i]);if(readers[i]->next(heads[i]))queue.push(i);}
        auto r=w->finish();inc("spill_bytes",r.bytes);readers.clear();for(auto old:group)c->spill->erase(old);return r;
    }
public:Sort(std::unique_ptr<ExecNode> n,Key k,Compare cmp):Base("ExternalSort"),child(std::move(n)),key(std::move(k)),compare(std::move(cmp)){}
    void open(ExecContext& ctx)override{
        close();begin(ctx);child->open(ctx);json row;std::size_t used=0;max_row=1;
        while(child->next(row)){auto n=bytes(c,row);max_row=std::max(max_row,n);if(n>c->memory_limit/4)throw DbError("QUERY_OPERATOR_MEMORY","One sortable row exceeds bounded merge-head memory budget");if(used+n>c->memory_limit){flush();used=0;}allocated(c,row);memory.push_back(std::move(row));used+=n;peak(used);}
        child->close();position=0;
        if(runs.empty())std::stable_sort(memory.begin(),memory.end(),[&](const json&a,const json&b){return less(a,b);});
        else{flush();auto fan=std::max<std::size_t>(2,std::min<std::size_t>(16,c->memory_limit/max_row-2));stats["merge_fan_in"]=fan;peak((fan+2)*max_row);while(runs.size()>1){std::vector<Run> next;for(std::size_t i=0;i<runs.size();i+=fan){auto e=std::min(runs.size(),i+fan);std::vector<Run> group(runs.begin()+i,runs.begin()+e);next.push_back(group.size()==1?group[0]:merge_runs(group));}runs=std::move(next);inc("merge_passes");}output=c->spill->open(runs[0]);}
    }
    bool next(json& row)override{if(!active)return false;if(output){if(!output->next(row))return false;}else{if(position>=memory.size())return false;row=memory[position++];}return emitted();}
    void close()noexcept override{output.reset();child->close();for(auto& r:runs)erase(r);runs.clear();memory.clear();position=0;active=false;}
    json diagnostics()const override{auto d=stats;d["children"]=json::array({child->diagnostics()});return d;}
    ~Sort(){close();}
};
class JoinBase:public Base{
protected:
    std::unique_ptr<ExecNode> left,right;JoinSpec spec;
    JoinBase(std::string n,std::unique_ptr<ExecNode> l,std::unique_ptr<ExecNode> r,JoinSpec s):Base(n),left(std::move(l)),right(std::move(r)),spec(std::move(s)){}
    bool pair(const json& a,const json& b,json& out){work(c);inc("pairs_examined");out=a;for(int slot:spec.right_slots)out[slot]=b[slot];return !spec.predicate||spec.predicate(out);}
public:json diagnostics()const override{auto d=stats;d["children"]=json::array({left->diagnostics(),right?right->diagnostics():json(nullptr)});return d;}
};
class Nested final:public JoinBase{
    bool index,have_left=false;json row;std::unique_ptr<ExecNode> probe;
public:Nested(std::unique_ptr<ExecNode>l,std::unique_ptr<ExecNode>r,JoinSpec s,bool ix=false):JoinBase(ix?"IndexNestedLoop":"NestedLoop",std::move(l),std::move(r),std::move(s)),index(ix){}
    void open(ExecContext& ctx)override{close();begin(ctx);left->open(ctx);have_left=false;}
    bool next(json& out)override{
        if(!active)return false;
        for(;;){
            if(!have_left){if(!left->next(row))return false;have_left=true;if(index){auto k=spec.left_key(row);if(has_null(k)){have_left=false;continue;}probe=spec.index_probe(row);probe->open(*c);inc("index_probes");}else{right->open(*c);inc("inner_rescans");}}
            json b;auto* inner=index?probe.get():right.get();while(inner->next(b))if(pair(row,b,out))return emitted();inner->close();probe.reset();have_left=false;
        }
    }
    void close()noexcept override{if(probe)probe->close();probe.reset();left->close();if(right)right->close();have_left=false;active=false;}
    ~Nested(){close();}
};
class Block final:public JoinBase{
    std::vector<json> block;json pending,inner;bool have_pending=false,have_inner=false;std::size_t at=0;
    bool fill(){block.clear();std::size_t used=0;json row;while(have_pending||left->next(row)){if(have_pending){row=std::move(pending);have_pending=false;}auto n=bytes(c,row);if(n>c->memory_limit/2)throw DbError("QUERY_OPERATOR_MEMORY","One join row exceeds block memory budget");if(used+n>c->memory_limit/2){pending=std::move(row);have_pending=true;break;}allocated(c,row);block.push_back(std::move(row));used+=n;}peak(used);if(block.empty())return false;right->open(*c);inc("outer_blocks");inc("inner_rescans");have_inner=false;at=0;return true;}
public:Block(std::unique_ptr<ExecNode>l,std::unique_ptr<ExecNode>r,JoinSpec s):JoinBase("BlockNestedLoop",std::move(l),std::move(r),std::move(s)){}
    void open(ExecContext& ctx)override{close();begin(ctx);left->open(ctx);}
    bool next(json& out)override{
        if(!active)return false;
        for(;;){if(block.empty()&&!fill())return false;if(!have_inner){if(!right->next(inner)){right->close();block.clear();continue;}have_inner=true;at=0;}while(at<block.size())if(pair(block[at++],inner,out))return emitted();have_inner=false;}
    }
    void close()noexcept override{left->close();right->close();block.clear();pending=nullptr;inner=nullptr;have_pending=have_inner=false;active=false;}
    ~Block(){close();}
};
class Merge final:public JoinBase{
    json a,b,group_key;bool has_a=false,has_b=false,in_group=false;std::vector<json> group;Run run;std::unique_ptr<QuerySpillStore::Reader> reader;std::size_t at=0;
    bool read_left(){has_a=left->next(a);return has_a;}
    bool read_right(){has_b=right->next(b);return has_b;}
    void drop_group(){reader.reset();erase(run);group.clear();in_group=false;at=0;}
    void start_group(){
        group_key=spec.right_key(b);std::size_t used=0;std::unique_ptr<QuerySpillStore::Writer> w;
        do{auto n=bytes(c,b);if(n>c->memory_limit/4)throw DbError("QUERY_OPERATOR_MEMORY","One duplicate row exceeds merge memory budget");allocated(c,b);if(!w&&used+n>c->memory_limit/4){w=c->spill->create();for(auto& r:group)w->append(r);group.clear();}if(w)w->append(b);else{group.push_back(b);used+=n;peak(used);}}while(read_right()&&spec.compare(spec.right_key(b),group_key)==0);
        if(w){run=w->finish();inc("spill_bytes",run.bytes);reader=c->spill->open(run);}at=0;in_group=true;inc("duplicate_groups");
    }
    void rewind_group(){at=0;if(run.id){reader.reset();reader=c->spill->open(run);inc("duplicate_run_rescans");}}
public:Merge(std::unique_ptr<ExecNode>l,std::unique_ptr<ExecNode>r,JoinSpec s):JoinBase("MergeJoin",std::move(l),std::move(r),std::move(s)){left=query::sort(std::move(left),spec.left_key,spec.compare);right=query::sort(std::move(right),spec.right_key,spec.compare);}
    void open(ExecContext& ctx)override{close();begin(ctx);left->open(ctx);right->open(ctx);read_left();read_right();}
    bool next(json& out)override{
        if(!active)return false;
        for(;;){
            if(in_group){json r;while(run.id?reader->next(r):(at<group.size()?(r=group[at++],true):false))if(pair(a,r,out))return emitted();if(read_left()&&!has_null(spec.left_key(a))&&spec.compare(spec.left_key(a),group_key)==0){rewind_group();continue;}drop_group();}
            while(has_a&&has_null(spec.left_key(a))){read_left();}
            while(has_b&&has_null(spec.right_key(b))){read_right();}
            if(!has_a||!has_b)return false;
            int cmp=spec.compare(spec.left_key(a),spec.right_key(b));if(cmp<0)read_left();else if(cmp>0)read_right();else start_group();
        }
    }
    void close()noexcept override{reader.reset();erase(run);group.clear();left->close();right->close();has_a=has_b=in_group=false;active=false;}
    ~Merge(){close();}
};
class Hash final:public JoinBase{
    static constexpr std::size_t partitions=16;
    std::unordered_map<std::string,std::vector<json>> table;std::vector<Run> lp,rp;std::unique_ptr<QuerySpillStore::Reader> probe_reader;std::unique_ptr<ExecNode> skew;
    json current;std::string current_key;std::size_t hit=0,part=0;bool have_probe=false,partitioned=false;std::size_t used=0;
    void add(json row){auto key=equality_key(spec.right_key(row));auto n=bytes(c,row)+key.size()+64;allocated(c,row);table[key].push_back(std::move(row));used+=n;peak(used);}
    void partition_inputs(json overflow){
        std::vector<std::unique_ptr<QuerySpillStore::Writer>> lw,rw;for(std::size_t i=0;i<partitions;++i){lw.push_back(c->spill->create());rw.push_back(c->spill->create());}
        auto write=[&](const json& row,bool l){work(c);auto k=l?spec.left_key(row):spec.right_key(row);if(has_null(k))return;auto i=hash_key(equality_key(k))%partitions;(l?lw[i]:rw[i])->append(row);};
        for(auto& item:table){for(auto& row:item.second){write(row,false);}}
        table.clear();used=0;write(overflow,false);json row;while(right->next(row))write(row,false);right->close();left->open(*c);while(left->next(row))write(row,true);left->close();
        for(std::size_t i=0;i<partitions;++i){lp.push_back(lw[i]->finish());rp.push_back(rw[i]->finish());inc("spill_bytes",lp.back().bytes+rp.back().bytes);}partitioned=true;part=0;inc("hash_partition_passes");
    }
    bool load_partition(){
        while(part<partitions){table.clear();used=0;if(!lp[part].rows||!rp[part].rows){erase(lp[part]);erase(rp[part]);++part;continue;}
            auto r=c->spill->open(rp[part]);json row;bool fits=true;while(r->next(row)){auto n=bytes(c,row)+equality_key(spec.right_key(row)).size()+64;if(used+n>c->memory_limit/2){fits=false;break;}add(std::move(row));}r.reset();
            if(!fits){table.clear();used=0;skew=std::make_unique<Nested>(std::make_unique<RunScan>(lp[part]),std::make_unique<RunScan>(rp[part]),spec);skew->open(*c);inc("skew_partition_nested_fallbacks");}else probe_reader=c->spill->open(lp[part]);inc("partitions_processed");return true;
        }return false;
    }
    void finish_partition(){probe_reader.reset();if(skew)skew->close();skew.reset();table.clear();used=0;erase(lp[part]);erase(rp[part]);++part;have_probe=false;}
public:Hash(std::unique_ptr<ExecNode>l,std::unique_ptr<ExecNode>r,JoinSpec s):JoinBase("HashJoin",std::move(l),std::move(r),std::move(s)){}
    void open(ExecContext& ctx)override{close();begin(ctx);right->open(ctx);json row;while(right->next(row)){if(has_null(spec.right_key(row)))continue;auto n=bytes(c,row)+equality_key(spec.right_key(row)).size()+64;if(used+n>c->memory_limit/2){partition_inputs(std::move(row));break;}add(std::move(row));}if(!partitioned){right->close();left->open(ctx);}}
    bool next(json& out)override{
        if(!active)return false;
        for(;;){
            if(partitioned){if(!probe_reader&&!skew&&!load_partition())return false;if(skew){if(skew->next(out))return emitted();finish_partition();continue;}}
            if(have_probe){auto found=table.find(current_key);if(found!=table.end())while(hit<found->second.size())if(pair(current,found->second[hit++],out))return emitted();have_probe=false;}
            bool got=partitioned?probe_reader->next(current):left->next(current);if(!got){if(!partitioned)return false;finish_partition();continue;}work(c);auto key=spec.left_key(current);if(has_null(key))continue;current_key=equality_key(key);hit=0;have_probe=true;
        }
    }
    void close()noexcept override{probe_reader.reset();if(skew)skew->close();skew.reset();left->close();right->close();for(auto& r:lp)erase(r);for(auto& r:rp)erase(r);lp.clear();rp.clear();table.clear();used=0;part=0;partitioned=have_probe=false;active=false;}
    ~Hash(){close();}
};
}
std::unique_ptr<ExecNode> scan(Open factory,const std::string& name){return std::make_unique<Scan>(std::move(factory),name);}
std::unique_ptr<ExecNode> sort(std::unique_ptr<ExecNode> n,Key key,Compare compare){return std::make_unique<Sort>(std::move(n),std::move(key),std::move(compare));}
std::unique_ptr<ExecNode> join(const std::string& method,std::unique_ptr<ExecNode> l,std::unique_ptr<ExecNode> r,JoinSpec s){
    if(method=="NESTED_LOOP")return std::make_unique<Nested>(std::move(l),std::move(r),std::move(s));
    if(method=="BLOCK_NESTED_LOOP")return std::make_unique<Block>(std::move(l),std::move(r),std::move(s));
    if(method=="INDEX_NESTED_LOOP"){if(!s.index_probe)throw DbError("INVALID_JOIN_METHOD","Index nested loop needs a parameterized index probe");return std::make_unique<Nested>(std::move(l),std::move(r),std::move(s),true);}
    if(!s.left_key||!s.right_key||!s.compare)throw DbError("INVALID_JOIN_METHOD","Hash/merge join requires equijoin keys");
    if(method=="MERGE")return std::make_unique<Merge>(std::move(l),std::move(r),std::move(s));
    if(method=="HASH")return std::make_unique<Hash>(std::move(l),std::move(r),std::move(s));
    throw DbError("INVALID_JOIN_METHOD","Unknown physical join method: "+method);
}
} // namespace edudb::query
