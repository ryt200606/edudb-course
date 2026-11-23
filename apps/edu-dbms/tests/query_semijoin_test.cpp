#include "query_semijoin.hpp"
#include "access.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <stdexcept>
using edudb::json;
namespace fs=std::filesystem;
static int checks=0;
static void check(bool value,const std::string& text){++checks;if(!value)throw std::runtime_error(text);}
static std::unique_ptr<edudb::query::ExecNode> input(const std::vector<json>& data,int* opens=nullptr){return edudb::query::scan([&data,opens](){if(opens)++*opens;auto position=std::make_shared<std::size_t>(0);return edudb::query::Pull([&data,position](json& row){if(*position>=data.size())return false;row=data[(*position)++];return true;});},"IndependentFixtureScan");}
static json model(const std::vector<json>& outer,const std::vector<json>& inner,bool anti){json rows=json::array();for(const auto& row:outer){bool match=false;if(!row[1].is_null())for(const auto& other:inner)if(!other[0].is_null()&&edudb::access::compare_keys(json::array({row[1]}),json::array({other[0]}))==0){match=true;break;}if(match!=anti)rows.push_back(row);}return rows;}
static edudb::query::SemiJoinSpec specification(){edudb::query::SemiJoinSpec s;s.outer_key=[](const json& r){return json::array({r[1]});};s.inner_key=[](const json& r){return r;};return s;}
static json execute(const std::vector<json>& outer,const std::vector<json>& inner,bool anti,edudb::QuerySpillStore& store,std::size_t memory,json& diagnostics,int* inner_opens=nullptr,std::function<void()> fault={}){
    edudb::query::ExecContext context;context.spill=&store;context.memory_limit=memory;context.row_bytes=[](const json& r){return r.dump().size()+128;};auto spec=specification();spec.after_build=std::move(fault);auto node=edudb::query::semi_join(input(outer),input(inner,inner_opens),spec,anti);node->open(context);json row,rows=json::array();while(node->next(row))rows.push_back(row);node->close();diagnostics=node->diagnostics();return rows;
}
int main(){const auto directory=fs::temp_directory_path()/fs::u8path("EduDB半连接算子_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));fs::create_directories(directory);try{
    edudb::QuerySpillStore store(directory.u8string(),"0123456789abcdef0123456789abcdef");json stats;
    const std::vector<json> outer={json::array({1,nullptr}),json::array({2,1}),json::array({3,1}),json::array({4,2}),json::array({5,9007199254740993ll}),json::array({6,0.0})};
    const std::vector<json> inner={json::array({nullptr}),json::array({1.0}),json::array({1.0}),json::array({9007199254740992.0}),json::array({-0.0})};
    for(bool anti:{false,true}){int opens=0;check(execute(outer,inner,anti,store,65536,stats,&opens)==model(outer,inner,anti),"NULL, duplicate outer rows and numeric keys match an independent typed comparator");check(opens==1&&stats["inner_loops"]==1,"Actual inner opens once");check(store.diagnostics()["live_runs"]==0,"Normal close removes owned runs");}
    int opens=0;check(execute({},inner,false,store,65536,stats,&opens).empty()&&opens==0,"Empty outer never opens inner");std::vector<json> null_outer={json::array({1,nullptr}),json::array({2,nullptr})};opens=0;check(execute(null_outer,inner,true,store,65536,stats,&opens)==json(null_outer)&&opens==0,"All-null NOT EXISTS preserves outer without building inner");
    std::mt19937_64 random(20261102);std::vector<json> values={nullptr,-3,-1,0,1,2,3,1.0,-0.0,9007199254740993ll,9007199254740992.0,"中文","甲",true,false};
    for(int round=0;round<200;++round){std::vector<json> a,b;for(int n=0;n<int(random()%50);++n)a.push_back(json::array({n,values[random()%values.size()]}));for(int n=0;n<int(random()%60);++n)b.push_back(json::array({values[random()%values.size()]}));for(bool anti:{false,true})check(execute(a,b,anti,store,65536,stats)==model(a,b,anti),"Fixed-seed bag/order oracle");}
    std::vector<json> a,b;for(int n=0;n<1000;++n){a.push_back(json::array({n,n}));for(int duplicate=0;duplicate<10;++duplicate)b.push_back(json::array({n}));}
    for(bool anti:{false,true}){opens=0;check(execute(a,b,anti,store,65536,stats,&opens)==model(a,b,anti),"Real partition spill preserves outer order/bag and anti semantics");check(stats["spilled"]==true&&stats["spill_bytes"].get<std::uint64_t>()>0&&opens==1,"CRC framed native spill is actually used with one base inner scan");check(stats["partition_keys_written"].get<std::uint64_t>()<b.size(),"Duplicate partition runs avoid repeated large writes");check(store.diagnostics()["live_runs"]==0,"All spilled runs cleaned");}
    {
        edudb::QuerySpillStore limited(directory.u8string(),"0123456789abcdef0123456789abcdef",1024);bool caught=false;
        try{execute(a,b,false,limited,65536,stats);}catch(const edudb::DbError& e){caught=e.code=="QUERY_LIMIT_EXCEEDED";}check(caught,"Spill byte quota fails safely");check(limited.diagnostics()["live_runs"]==0,"Quota failure closes all owned temporary runs");
    }
    {
        auto corrupt=[&](){for(const auto& file:fs::directory_iterator(directory)){if(file.path().filename().u8string().rfind("edudb-query-",0)!=0)continue;const auto length=fs::file_size(file.path());if(length<80)continue;std::fstream stream(file.path(),std::ios::binary|std::ios::in|std::ios::out);stream.seekg(std::streamoff(length-1));char byte=0;stream.read(&byte,1);byte^=1;stream.seekp(std::streamoff(length-1));stream.write(&byte,1);stream.flush();return;}throw std::runtime_error("No owned native run found for CRC fault");};
        bool caught=false;try{execute(a,b,false,store,65536,stats,nullptr,corrupt);}catch(const edudb::DbError& e){caught=e.code=="QUERY_SPILL_CORRUPT";}check(caught,"Actual temporary frame corruption rejected");check(store.diagnostics()["live_runs"]==0,"CRC failure removes all owned runs");
    }
    std::cout<<"PASS "<<checks<<" pure real semi/anti operator checks; SQL decorrelation not yet claimed\n";
}catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" directory="<<directory.u8string()<<"\n";return 1;}}
