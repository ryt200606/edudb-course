#include "query_topk.hpp"
#include "access.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
using edudb::json;
namespace fs=std::filesystem;
static int checks=0;
static void check(bool value,const std::string& message){++checks;if(!value)throw std::runtime_error(message);}
static std::unique_ptr<edudb::query::ExecNode> input(const std::vector<json>& rows,int* opens=nullptr){
    return edudb::query::scan([&rows,opens](){if(opens)++*opens;auto at=std::make_shared<std::size_t>(0);return edudb::query::Pull([&rows,at](json& row){if(*at>=rows.size())return false;row=rows[(*at)++];return true;});},"IndependentTopKInput");
}
static json key(const json& row){return json::array({row[0],row[1]});}
static int compare(const json& a,const json& b){
    for(std::size_t i=0;i<2;++i){int c=a[i].is_null()?(b[i].is_null()?0:1):b[i].is_null()?-1:edudb::access::compare_keys(json::array({a[i]}),json::array({b[i]}));if(c)return i==0?c:-c;}return 0;
}
static json model(std::vector<json> rows,std::size_t k){std::stable_sort(rows.begin(),rows.end(),[](const json& a,const json& b){return compare(key(a),key(b))<0;});if(k<rows.size())rows.resize(k);return rows;}
static json execute(const std::vector<json>& rows,std::size_t k,edudb::QuerySpillStore& store,json& stats,std::function<void(const json&)> allocation={}){
    edudb::query::ExecContext context;context.spill=&store;context.memory_limit=65536;context.row_bytes=[](const json& r){return r.dump().size()+128;};context.materialize=allocation;
    auto node=edudb::query::topk(input(rows),key,compare,k);node->open(context);json result=json::array(),row;while(node->next(row))result.push_back(row);node->close();stats=node->diagnostics();return result;
}
int main(){auto directory=fs::temp_directory_path()/fs::u8path("EduDB有界排序_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));fs::create_directories(directory);try{
    edudb::QuerySpillStore store(directory.u8string(),"0123456789abcdef0123456789abcdef");json stats;
    std::vector<json> values={nullptr,-3,0,1,1.0,-0.0,9007199254740993ll,9007199254740992.0,"中文","甲",true,false};std::mt19937_64 random(20261102);
    for(int trial=0;trial<250;++trial){std::vector<json> rows;for(int n=0;n<int(random()%180);++n)rows.push_back(json::array({values[random()%values.size()],values[random()%values.size()],n,"payload"}));const auto k=std::size_t(random()%200);check(execute(rows,k,store,stats)==model(rows,k),"Independent stable typed-key sorting including NULL/INTEGER-REAL/UTF8 and ties");check(stats["memory_bytes_estimate_peak"].get<std::size_t>()<=65536,"Estimated heap and pending row stay within configured memory");}
    std::vector<json> rows;for(int i=0;i<500;++i)rows.push_back(json::array({i%17,i%3,i,std::string(i<150?8:1100,'x')}));
    check(execute(rows,50,store,stats)==model(rows,50),"Actual-width fallback after discards preserves first-K semantics");check(stats["fallback_external_sort"]==true&&stats["discarded_rows"].get<std::uint64_t>()>0,"Fallback really occurs after heap discarded dominated inputs");check(stats["spill_bytes"].get<std::uint64_t>()>0&&stats["external_sort_actual"]["node"]=="ExternalSort","Fallback uses real CRC external sorting");check(store.diagnostics()["live_runs"]==0,"Normal external fallback releases all owned runs");
    check(execute(rows,std::numeric_limits<std::size_t>::max(),store,stats)==model(rows,rows.size()),"Huge K falls back without reserving a huge vector");check(stats["fallback_external_sort"]==true,"Huge K explicitly explains fallback");
    {
        int opens=0;edudb::query::ExecContext c;c.spill=&store;c.memory_limit=65536;auto node=edudb::query::topk(input(rows,&opens),key,compare,0);node->open(c);json row;check(!node->next(row)&&opens==0,"K zero validates without opening input");node->close();
    }
    {
        edudb::query::ExecContext c;c.spill=&store;c.memory_limit=65536;c.row_bytes=[](const json&r){return r.dump().size()+128;};auto node=edudb::query::topk(input(rows),key,compare,50);
        for(int repeat=0;repeat<2;++repeat){node->open(c);json actual=json::array(),row;while(node->next(row))actual.push_back(row);node->close();check(actual==model(rows,50),"Repeated opens retain stable order and clear old fallback state");}
    }
    {
        edudb::QuerySpillStore quota(directory.u8string(),"0123456789abcdef0123456789abcdef",1024);bool caught=false;try{execute(rows,50,quota,stats);}catch(const edudb::DbError&e){caught=e.code=="QUERY_LIMIT_EXCEEDED";}check(caught,"Actual external fallback respects temporary byte quota");check(quota.diagnostics()["live_runs"]==0,"Quota failure cleans partial fallback writes");
    }
    {
        std::size_t charged=0;bool caught=false;try{execute(rows,50,store,stats,[&](const json&r){charged+=r.dump().size()+128;if(charged>8000)throw edudb::DbError("QUERY_LIMIT_EXCEEDED","Independent global allocation budget");});}catch(const edudb::DbError&e){caught=e.code=="QUERY_LIMIT_EXCEEDED";}check(caught&&store.diagnostics()["live_runs"]==0,"Cumulative materialization errors clean temporary files");
    }
    {
        edudb::query::ExecContext c;c.spill=&store;c.memory_limit=65536;c.row_bytes=[](const json&r){return r.size()==2?std::numeric_limits<std::size_t>::max():std::size_t(1);};bool caught=false;
        try{auto node=edudb::query::topk(input(rows),key,compare,1);node->open(c);}catch(const edudb::DbError&e){caught=e.code=="QUERY_OPERATOR_MEMORY";}check(caught,"Allocation-estimate addition rejects size_t overflow before an apparent small heap");
    }
    for(const auto& file:fs::directory_iterator(directory))check(file.path().filename().u8string().rfind("edudb-query-",0)!=0,"No owned temporary files survive tests");
    std::cout<<"PASS "<<checks<<" actual bounded Top-K, stable keys, external fallback and resource cleanup checks\n";
}catch(const std::exception&e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" directory="<<directory.u8string()<<"\n";return 1;}}
