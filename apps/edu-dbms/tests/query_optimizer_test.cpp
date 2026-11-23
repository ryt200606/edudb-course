#include "edudb.hpp"
#include "query_operators.hpp"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <set>

using edudb::json;
static int checks=0;
static void check(bool ok,const std::string& text){++checks;if(!ok)throw std::runtime_error(text);}
static std::unique_ptr<edudb::query::ExecNode> source(const std::vector<json>& rows){
    return edudb::query::scan([rows](){auto at=std::make_shared<std::size_t>(0);return edudb::query::Pull([rows,at](json& row){if(*at>=rows.size())return false;row=rows[(*at)++];return true;});},"IndependentFixture");
}
static void operator_cases(const std::filesystem::path& directory){
    std::vector<json> left,right;
    for(int i=0;i<180;++i)left.push_back(json::array({i%13,i,nullptr,nullptr,std::string(600,'l')}));
    left.push_back(json::array({nullptr,999,nullptr,nullptr,std::string(600,'l')}));
    for(int i=0;i<145;++i)right.push_back(json::array({nullptr,nullptr,double(i%13),i,std::string(600,'r')}));
    right.push_back(json::array({nullptr,nullptr,nullptr,999,std::string(600,'r')}));
    std::size_t expected=0;for(int a=0;a<180;++a)for(int b=0;b<145;++b)expected+=(a%13)==(b%13);
    check(edudb::query::equality_key(json::array({1}))==edudb::query::equality_key(json::array({1.0})),"Hash joins normalize equal INTEGER/REAL keys");
    check(edudb::query::equality_key(json::array({std::int64_t(9007199254740993ll)}))!=edudb::query::equality_key(json::array({9007199254740992.0})),"Hash canonicalization preserves large INTEGER distinction");
    for(const auto& method:{"NESTED_LOOP","BLOCK_NESTED_LOOP","INDEX_NESTED_LOOP","MERGE","HASH"}){
        auto data=directory/method;std::filesystem::create_directories(data);edudb::QuerySpillStore spill(data.u8string(),"0123456789abcdef0123456789abcdef");
        edudb::query::ExecContext context;context.spill=&spill;context.memory_limit=65536;std::size_t work=0;context.work=[&](){if(++work>2000000)throw std::runtime_error("Unbounded operator work");};context.row_bytes=[](const json&r){return r.dump().size()+192;};
        edudb::query::JoinSpec spec;spec.left_key=[](const json&r){return json::array({r[0]});};spec.right_key=[](const json&r){return json::array({r[2]});};
        spec.compare=[](const json&a,const json&b){if(a[0].is_null()||b[0].is_null())return a[0].is_null()?(b[0].is_null()?0:-1):1;auto x=a[0].get<long double>(),y=b[0].get<long double>();return (x>y)-(x<y);};
        spec.predicate=[](const json&r){return !r[0].is_null()&&!r[2].is_null()&&r[0].get<long double>()==r[2].get<long double>();};spec.right_slots={2,3};
        spec.index_probe=[right](const json& row){std::vector<json> matches;for(auto r:right)if(!r[2].is_null()&&r[2].get<long double>()==row[0].get<long double>())matches.push_back(std::move(r));return source(matches);};
        auto node=edudb::query::join(method,source(left),source(right),spec);
        for(int pass=0;pass<2;++pass){node->open(context);json row;std::size_t count=0;std::set<std::pair<int,int>> ids;while(node->next(row)){check(row[0].get<int>()==row[2].get<int>(),"Every emitted pair satisfies key equality");ids.insert({row[1].get<int>(),row[3].get<int>()});++count;}node->close();check(count==expected&&ids.size()==expected,std::string(method)+" preserves every duplicate pair without NULL matches");}
        auto metrics=node->diagnostics();if(std::string(method)=="HASH")check(metrics["hash_partition_passes"].get<int>()>=1,"Hash join actually partitions when build exceeds budget");
        if(std::string(method)=="MERGE"){check(metrics["children"][0]["spill_bytes"].get<std::uint64_t>()>0,"Merge actually uses external sort runs");check(metrics["children"][0]["children"][0]["node"]=="IndependentFixture","Repeated MERGE open does not nest sorts indefinitely");}
        if(std::string(method)=="BLOCK_NESTED_LOOP")check(metrics["inner_rescans"].get<std::uint64_t>()<left.size()*2,"Block join traverses inner once per actual block");
        if(std::string(method)=="INDEX_NESTED_LOOP")check(metrics["index_probes"].get<std::uint64_t>()==360,"Index join probes once per non-NULL outer row");
        node.reset();for(auto& f:std::filesystem::directory_iterator(data))check(f.path().extension()!=".tmp","Temporary files cleaned after operators close");
    }
}
#ifndef EDUDB_OPERATORS_ONLY
static json ok(edudb::Engine& db,const std::string& sid,const std::string& sql){auto r=db.execute(sid,sql);if(!r.value("ok",false))throw std::runtime_error(sql+" => "+r.dump());return r;}
static void sql_cases(const std::filesystem::path& directory){
    edudb::Engine db(directory.u8string(),4);auto sid=db.open_session();
    ok(db,sid,"CREATE TABLE a(id INTEGER PRIMARY KEY,k INTEGER,v VARCHAR(100))");ok(db,sid,"CREATE TABLE b(id INTEGER PRIMARY KEY,k REAL)");ok(db,sid,"CREATE TABLE c(id INTEGER PRIMARY KEY,k INTEGER)");
    ok(db,sid,"INSERT INTO a VALUES(1,1,'甲'),(2,1,'乙'),(3,2,'丙'),(4,NULL,'空')");ok(db,sid,"INSERT INTO b VALUES(10,1.0),(11,1.0),(12,2.0),(13,NULL)");ok(db,sid,"INSERT INTO c VALUES(20,1),(21,2)");
    ok(db,sid,"CREATE INDEX bk ON b(k)");
    auto analyzed=ok(db,sid,"ANALYZE a");check(analyzed["statistics"]["analyzed_rows"]==4,"ANALYZE sees real row count");check(analyzed["statistics"]["columns"][1]["null_count"]==1&&analyzed["statistics"]["columns"][1]["ndv"]==2,"Exact small-column NULL/NDV statistics");
    auto baseline=ok(db,sid,"SELECT a.id,b.id FROM a JOIN b ON a.k=b.k USE JOIN(NESTED_LOOP) ORDER BY a.id,b.id")["rows"];
    check(baseline==json::array({json::array({1,10}),json::array({1,11}),json::array({2,10}),json::array({2,11}),json::array({3,12})}),"Independent expected SQL duplicate pairs");
    for(const auto& method:{"BLOCK_NESTED_LOOP","INDEX_NESTED_LOOP","MERGE","HASH"}){auto q=ok(db,sid,"SELECT a.id,b.id FROM a JOIN b ON a.k=b.k USE JOIN("+std::string(method)+") ORDER BY a.id,b.id");check(q["rows"]==baseline,"Forced physical method SQL matches independent rows");check(q["plan"]["join_algorithm"]==method&&q["plan"].contains("physical_actual"),"Chosen method is actually executed and diagnosed");}
    auto three=ok(db,sid,"SELECT a.id,b.id,c.id FROM a JOIN b ON a.k=b.k JOIN c ON b.k=c.k USE JOIN(HASH) ORDER BY a.id,b.id,c.id");check(three["rows"].size()==5,"Three-table executable costed plan");check(three["plan"]["optimizer"]["search"]=="subset dynamic programming with interesting orders","Multi-table DP is active");

    auto forward=db.execute(sid,"SELECT a.id FROM a JOIN b ON a.k=c.k JOIN c ON b.k=c.k");
    check(!forward["ok"].get<bool>()&&forward["error"]["code"]=="COLUMN_NOT_FOUND","ON cannot bind a relation introduced by a later JOIN");
    auto parameterized=ok(db,sid,"SELECT a.id,b.id FROM a JOIN b USE INDEX(bk) ON a.k=b.k USE JOIN(INDEX_NESTED_LOOP) ORDER BY a.id,b.id");
    check(parameterized["rows"]==baseline&&parameterized["plan"]["physical_actual"]["node"]=="IndexNestedLoop","Explicit join index without constant bounds executes real parameterized probes");
    check(parameterized["plan"]["sources"][1]["index"]=="bk"&&parameterized["plan"]["physical_actual"]["index_probes"].get<int>()>0,"Deferred hint binds its exact declared index");
    auto complete=ok(db,sid,"SELECT a.id,b.id FROM a JOIN b ON a.k=b.k USE JOIN(NESTED_LOOP)");
    auto limited=ok(db,sid,"SELECT a.id,b.id FROM a JOIN b ON a.k=b.k USE JOIN(NESTED_LOOP) LIMIT 1");
    check(limited["rows"].size()==1&&std::find(baseline.begin(),baseline.end(),limited["rows"][0])!=baseline.end(),"Streaming join LIMIT returns a valid pair");
    check(limited["metrics"]["streaming"]==true&&limited["plan"]["actual"]["early_stop"]==true&&limited["plan"]["actual"]["join_pairs"].get<int>()<complete["plan"]["actual"]["join_pairs"].get<int>(),"LIMIT stops pulling physical nested-loop pairs before full execution");
    auto zero=ok(db,sid,"SELECT COUNT(*) FROM a JOIN b ON a.k=b.k USE JOIN(HASH) LIMIT 0");
    check(zero["rows"].empty()&&zero["metrics"]["rows_scanned"]==0&&zero["plan"]["physical_actual"]["opened"]==false,"LIMIT0 opens neither blocking aggregate nor join children");
    for(const auto& source:zero["plan"]["sources"])check(source["loops"]==0,"Every LIMIT0 scan is unopened");
    auto bad=db.execute(sid,"SELECT a.id,b.id FROM a JOIN b ON a.k>b.k USE JOIN(HASH)");check(!bad["ok"].get<bool>()&&bad["error"]["code"]=="INVALID_JOIN_METHOD","Infeasible hash hint rejects without fake fallback");
    ok(db,sid,"BEGIN");ok(db,sid,"INSERT INTO a VALUES(5,9,'回滚')");ok(db,sid,"ANALYZE a");ok(db,sid,"ROLLBACK");auto visible=ok(db,sid,"SELECT COUNT(*) FROM a");check(visible["rows"][0][0]==4,"ANALYZE statistics and DML share rollback boundary");db.close_session(sid);
}

struct OperatorMemoryEnvironment{
    std::string name,previous;bool present=false;
    OperatorMemoryEnvironment(std::string key="EDUDB_QUERY_OPERATOR_MEMORY_BYTES",const char* value="65536"):name(std::move(key)){auto p=std::getenv(name.c_str());present=p!=nullptr;if(p)previous=p;
#ifdef _WIN32
        _putenv_s(name.c_str(),value);
#else
        setenv(name.c_str(),value,1);
#endif
    }
    ~OperatorMemoryEnvironment(){
#ifdef _WIN32
        _putenv_s(name.c_str(),present?previous.c_str():"");
#else
        if(present)setenv(name.c_str(),previous.c_str(),1);else unsetenv(name.c_str());
#endif
    }
};
static void final_sort_cases(const std::filesystem::path& directory){
    // Preserve the prior real external-sort/spill contract explicitly. Separate
    // Top-K tests exercise the new default and compare it with this exact path.
    OperatorMemoryEnvironment memory,full_sort("EDUDB_OPTIMIZER_TOPK","OFF");edudb::Engine db(directory.u8string(),4);auto sid=db.open_session();
    ok(db,sid,"CREATE TABLE sortable(id INTEGER PRIMARY KEY,k INTEGER,payload VARCHAR(1200))");
    std::string sql="INSERT INTO sortable VALUES";const std::string payload="中文演示"+std::string(850,'x');
    for(int i=0;i<200;++i){if(i)sql+=",";sql+="("+std::to_string(i)+","+(i%9==0?"NULL":std::to_string(i%13))+",'"+payload+"')";}ok(db,sid,sql);
    auto sorted=ok(db,sid,"SELECT id,payload FROM sortable ORDER BY k DESC,id LIMIT 7 OFFSET 2");
    std::vector<int> ids;for(int i=0;i<200;++i)ids.push_back(i);
    std::sort(ids.begin(),ids.end(),[](int a,int b){bool an=a%9==0,bn=b%9==0;if(an!=bn)return an;if(an)return a<b;if(a%13!=b%13)return a%13>b%13;return a<b;});
    json wanted=json::array();for(int i=2;i<9;++i)wanted.push_back(json::array({ids[i],payload}));
    check(sorted["rows"]==wanted,"Final SQL external ORDER BY preserves DESC NULL-first, ties and LIMIT/OFFSET");
    check(sorted["plan"]["final_sort_actual"]["node"]=="ExternalSort"&&sorted["plan"]["final_sort_actual"]["spill_bytes"].get<std::uint64_t>()>0,"Final ORDER BY really writes external runs under bounded memory");
    check(sorted["metrics"]["io"]["files"]["temp"]["write_bytes"].get<std::uint64_t>()>0&&sorted["metrics"]["io"]["files"]["temp"]["read_calls"].get<std::uint64_t>()>0,"SQL sort exposes real native temporary-file I/O");
    auto zero=ok(db,sid,"SELECT 1/(id-id) FROM sortable ORDER BY id LIMIT 0");check(zero["rows"].empty()&&zero["metrics"]["rows_scanned"]==0,"ORDER BY LIMIT0 binds names/types without evaluating row arithmetic");
    auto distinct=ok(db,sid,"SELECT DISTINCT k FROM sortable ORDER BY k DESC");json keys=json::array({json::array({nullptr})});for(int k=12;k>=0;--k)keys.push_back(json::array({k}));check(distinct["rows"]==keys&&distinct["plan"]["final_sort_actual"]["node"]=="ExternalSort","DISTINCT output uses the same real final ordering operator");
    auto grouped=ok(db,sid,"SELECT k,COUNT(*) FROM sortable GROUP BY k ORDER BY k");json groups=json::array();for(int k=0;k<13;++k){int count=0;for(int i=0;i<200;++i)count+=i%9!=0&&i%13==k;groups.push_back(json::array({k,count}));}groups.push_back(json::array({nullptr,23}));check(grouped["rows"]==groups,"Grouped final external ordering preserves counts and ASC NULL-last");
    auto failure=db.execute(sid,"SELECT payload,1/(id-117) FROM sortable ORDER BY id");check(!failure["ok"].get<bool>()&&failure["error"]["code"]=="DIVISION_BY_ZERO","Projection failure after run creation returns the real error");
    check(failure["metrics"]["io"]["files"]["temp"]["write_bytes"].get<std::uint64_t>()>0,"Failure fixture reached actual spilled-run writes");
    for(const auto& file:std::filesystem::directory_iterator(directory))check(file.path().filename().u8string().rfind("edudb-query-",0)!=0,"Failed SQL sort cleans only its own temporary runs");
    ok(db,sid,"SELECT 40+2");db.close_session(sid);
}
#endif
int main(){
    auto path=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB查询优化_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{std::filesystem::create_directories(path);operator_cases(path/"operators");
#ifndef EDUDB_OPERATORS_ONLY
        sql_cases(path/"SQL中文");
        final_sort_cases(path/"排序中文");
#endif
        std::cout<<"PASS "<<checks<<" checks; functional algorithms/statistics only, no performance claim\n";return 0;
    }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<"; preserved "<<path.u8string()<<"\n";return 1;}
}
