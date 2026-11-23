#include "query_relational.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <stdexcept>
using edudb::json;
namespace q=edudb::query;
static int checks=0;
static void check(bool v,const std::string& text){++checks;if(!v)throw std::runtime_error(text);}
static int scalar(const json& a,const json& b){
    if(a.is_null()||b.is_null())return a.is_null()?(b.is_null()?0:-1):1;
    if(a.is_number()&&b.is_number()){auto x=a.get<long double>(),y=b.get<long double>();return (x>y)-(x<y);}
    if(a.is_string()&&b.is_string()){auto x=a.get<std::string>(),y=b.get<std::string>();return (x>y)-(x<y);}
    throw std::runtime_error("Unexpected fixture type");
}
static int rows_compare(const json& a,const json& b){check(a.size()==b.size(),"Fixture compatible row widths");for(std::size_t i=0;i<a.size();++i){auto c=scalar(a[i],b[i]);if(c)return c;}return 0;}
static std::unique_ptr<q::ExecNode> source(json data){return q::scan([data](){auto at=std::make_shared<std::size_t>(0);return q::Pull([data,at](json& row){if(*at>=data.size())return false;row=data[(*at)++];return true;});},"IndependentInput");}
static json drain(std::unique_ptr<q::ExecNode>& node,q::ExecContext& context){node->open(context);json row,out=json::array();while(node->next(row))out.push_back(row);node->close();return out;}
static int multiplicity(const json& rows,const json& wanted){int n=0;for(const auto& r:rows)if(rows_compare(r,wanted)==0)++n;return n;}
static void set_cases(q::ExecContext& context){
    json a=json::array({json::array({nullptr,"n"}),json::array({nullptr,"n"}),json::array({1,"a"}),json::array({1.0,"a"}),json::array({1,"a"}),json::array({2,"b"})});
    json b=json::array({json::array({nullptr,"n"}),json::array({1.0,"a"}),json::array({1,"a"}),json::array({3,"c"}),json::array({3,"c"})});
    for(const auto& op:{"UNION","INTERSECT","EXCEPT"})for(bool all:{false,true}){
        auto node=q::set_operation(op,all,source(a),source(b),rows_compare);auto got=drain(node,context);
        const auto expected=std::string(op)=="UNION"?(all?11:4):std::string(op)=="INTERSECT"?(all?3:2):(all?3:1);
        check(got.size()==std::size_t(expected),std::string(op)+" independent expected multiplicity");
        check(multiplicity(got,json::array({nullptr,"n"}))==(std::string(op)=="UNION"?(all?3:1):std::string(op)=="INTERSECT"?1:(all?1:0)),"Set NULL equality differs from ordinary predicate UNKNOWN");
        check(multiplicity(got,json::array({1,"a"}))==(std::string(op)=="UNION"?(all?5:1):std::string(op)=="INTERSECT"?(all?2:1):(all?1:0)),"Mixed numeric keys preserve correct bag counts");
        auto again=drain(node,context);check(again.size()==got.size(),"Set repeated open resets cursors without nested sort growth");
    }
    auto node=q::distinct(source(a),rows_compare);auto got=drain(node,context);check(got.size()==3,"ExternalDistinct groups NULL and INTEGER/REAL equality correctly");
}
static q::JoinSpec outer_spec(){q::JoinSpec s;s.right_slots={2,3};s.predicate=[](const json&r){return !r[0].is_null()&&!r[2].is_null()&&scalar(r[0],r[2])==0;};return s;}
static void outer_cases(q::ExecContext& context){
    json a=json::array({json::array({1,10,nullptr,nullptr}),json::array({2,11,nullptr,nullptr}),json::array({nullptr,12,nullptr,nullptr}),json::array({1,13,nullptr,nullptr}),json::array({4,14,nullptr,nullptr})});
    json b=json::array({json::array({nullptr,nullptr,1,20}),json::array({nullptr,nullptr,1,21}),json::array({nullptr,nullptr,3,22}),json::array({nullptr,nullptr,nullptr,23})});
    for(const auto& kind:{"LEFT","RIGHT","FULL"}){
        auto node=q::outer_join(kind,source(a),source(b),outer_spec(),{0,1});auto got=drain(node,context);auto n=std::string(kind)=="LEFT"?7:std::string(kind)=="RIGHT"?6:9;check(got.size()==std::size_t(n),"Outer duplicates and unmatched NULL rows use independent expected count");
        int matched=0,left_null=0,right_null=0;for(const auto& r:got){if(r[1].is_null())++left_null;else if(r[3].is_null())++right_null;else ++matched;}
        check(matched==4,"Each of two left key1 records matches both right key1 records");check(left_null==(std::string(kind)=="LEFT"?0:2),"RIGHT/FULL emits unmatched NULL key separately");check(right_null==(std::string(kind)=="RIGHT"?0:3),"LEFT/FULL preserves every unmatched outer row");
        auto repeated=drain(node,context);check(repeated.size()==got.size(),"Outer repeated open resets matched bitmaps");
        auto empty_left=q::outer_join(kind,source(json::array()),source(b),outer_spec(),{0,1});check(drain(empty_left,context).size()==(std::string(kind)=="LEFT"?0:4),"Outer empty-left semantics");
        auto empty_right=q::outer_join(kind,source(a),source(json::array()),outer_spec(),{0,1});check(drain(empty_right,context).size()==(std::string(kind)=="RIGHT"?0:5),"Outer empty-right semantics");
    }
}
static void apply_cases(q::ExecContext& context){
    auto outer=json::array({json::array({nullptr}),json::array({1}),json::array({2})});
    for(const auto& mode:{"IN","NOT_IN"})for(bool empty:{false,true}){
        q::ApplySpec spec;spec.mode=mode;spec.output_slot=1;spec.value=[](const json&r){return r[0];};spec.scalar_compare=scalar;spec.inner=[empty](const json&){return source(empty?json::array():json::array({json::array({nullptr}),json::array({1})}));};
        auto node=q::apply(source(outer),spec);auto got=drain(node,context);
        if(empty)for(auto& r:got)check(r[1]==(std::string(mode)=="NOT_IN"),"NULL IN empty is FALSE; NULL NOT IN empty is TRUE");
        else{check(got[0][1].is_null()&&got[2][1].is_null(),"NULL-aware membership retains UNKNOWN");check(got[1][1]==(std::string(mode)=="IN"),"True membership overrides an unrelated NULL");}
    }
    for(const auto& mode:{"EXISTS","NOT_EXISTS"}){
        q::ApplySpec spec;spec.mode=mode;spec.output_slot=1;spec.inner=[](const json&r){return source(!r[0].is_null()&&r[0]==1?json::array({json::array({nullptr})}):json::array());};auto node=q::apply(source(outer),spec);auto got=drain(node,context);check(got[1][1]==(std::string(mode)=="EXISTS")&&got[0][1]==(std::string(mode)=="NOT_EXISTS"),"EXISTS tests rows, not projected NULL value");
    }
    q::ApplySpec spec;spec.mode="SCALAR";spec.output_slot=1;spec.inner=[](const json&r){return source(r[0].is_null()?json::array():r[0]==1?json::array({json::array({42})}):json::array({json::array({1}),json::array({2})}));};
    auto node=q::apply(source(outer),spec);node->open(context);json row;check(node->next(row)&&row[1].is_null(),"Empty scalar subquery produces NULL");check(node->next(row)&&row[1]==42,"One-row scalar subquery returns its value");bool rejected=false;try{node->next(row);}catch(const edudb::DbError& e){rejected=e.code=="CARDINALITY_VIOLATION";}node->close();check(rejected,"Multiple scalar rows reject instead of silently taking first");
}
int main(){
    auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB关系算子_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{std::filesystem::create_directories(directory);edudb::QuerySpillStore store(directory.u8string(),"0123456789abcdef0123456789abcdef");q::ExecContext context;context.spill=&store;context.memory_limit=65536;std::uint64_t work=0;context.work=[&](){if(++work>1000000)throw std::runtime_error("Functional fixture exceeded bounded work");};context.row_bytes=[](const json&r){return r.dump().size()+128;};set_cases(context);outer_cases(context);apply_cases(context);for(const auto& f:std::filesystem::directory_iterator(directory))check(f.path().filename().u8string().rfind("edudb-query-",0)!=0,"Operator cleanup removed every owned temporary run");std::cout<<"PASS "<<checks<<" pure relational operator checks; SQL integration not claimed\n";return 0;}
    catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<"; preserved "<<directory.u8string()<<"\n";return 1;}
}
