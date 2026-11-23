#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& text){++checks;if(!value)throw std::runtime_error(text);}
struct Environment{
    std::string name,previous;bool existed=false;
    Environment(std::string key,const std::string& value):name(std::move(key)){const char* old=std::getenv(name.c_str());existed=old!=nullptr;if(old)previous=old;set(value);}
    void set(const std::string& value){
#ifdef _WIN32
        _putenv_s(name.c_str(),value.c_str());
#else
        setenv(name.c_str(),value.c_str(),1);
#endif
    }
    ~Environment(){
#ifdef _WIN32
        _putenv_s(name.c_str(),existed?previous.c_str():"");
#else
        if(existed)setenv(name.c_str(),previous.c_str(),1);else unsetenv(name.c_str());
#endif
    }
};
static json ok(edudb::Engine& db,const std::string& session,const std::string& sql){auto result=db.execute(session,sql);check(result.value("ok",false),sql+" => "+result.dump());return result;}
static json twin(edudb::Engine& db,const std::string& session,const std::string& sql,bool chosen){
    json baseline;{Environment force("EDUDB_OPTIMIZER_DECORRELATE","OFF");baseline=ok(db,session,sql);}
    auto actual=ok(db,session,sql);check(actual["rows"]==baseline["rows"]&&actual["columns"]==baseline["columns"],"Actual semi/anti result/order/schema differs from original Apply: "+sql);
    check(actual["plan"]["decorrelation"]["chosen"]==chosen,"Expected decorrelation decision differs: "+actual.dump());return actual;
}
int main(){const auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB真实去相关_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));try{
    Environment enabled("EDUDB_OPTIMIZER_DECORRELATE","ON");edudb::Engine db(directory.u8string(),4);auto session=db.open_session();
    ok(db,session,"CREATE TABLE outer_rows(id INTEGER PRIMARY KEY,k INTEGER)");ok(db,session,"CREATE TABLE inner_rows(id INTEGER PRIMARY KEY,k REAL)");
    std::string insert="INSERT INTO inner_rows VALUES ";for(int n=0;n<32;++n){if(n)insert+=',';insert+='('+std::to_string(n)+','+std::to_string(n)+".0)";}ok(db,session,insert);
    insert="INSERT INTO outer_rows VALUES ";for(int n=0;n<12;++n){if(n)insert+=',';insert+='('+std::to_string(n)+','+(n<2?"999":n<4?"NULL":"31")+')';}ok(db,session,insert);
    const std::string exists="SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k AND i.id>=0) ORDER BY o.id";
    json baseline;{Environment force("EDUDB_OPTIMIZER_DECORRELATE","OFF");baseline=ok(db,session,exists);}
    const auto actual=twin(db,session,exists,true);check(actual["rows"].size()==8,"Outer duplicate keys preserve all eight distinct outer records");check(actual["plan"]["physical_actual"]["node"]=="HashSemiJoin","Real semi node executes");
    check(actual["plan"]["decorrelation_actual"][0]["inner_loops"]==1&&actual["plan"]["decorrelation_actual"][0]["rows_examined"]==32,"Actual native inner scans exactly once");
    check(baseline["plan"]["subquery_evaluations"]==12&&baseline["plan"]["subquery_total_rows_examined"]==384,"Original Apply's actual repeated scan work is measured, not inferred");
    check(actual["plan"]["subquery_evaluations"]==0,"Semi predicate is not also evaluated by Apply");
    const auto anti=twin(db,session,"SELECT o.id FROM outer_rows o WHERE NOT EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k) ORDER BY o.id",true);check(anti["rows"].size()==4&&anti["plan"]["physical_actual"]["node"]=="HashAntiJoin","NOT EXISTS preserves unmatched and NULL-key outer rows");
    twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1/0 FROM inner_rows i WHERE i.k=o.k) ORDER BY o.id",true);
    const auto null_only=twin(db,session,"SELECT o.id FROM outer_rows o WHERE o.k IS NULL AND NOT EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k) ORDER BY o.id",true);check(null_only["rows"].size()==2&&null_only["plan"]["decorrelation_actual"][0]["inner_loops"]==0,"All-null anti input never needs an inner build");
    const auto filtered=twin(db,session,"SELECT o.id FROM outer_rows o WHERE FALSE AND EXISTS(SELECT 1/0 FROM inner_rows i WHERE i.k=o.k)",true);check(filtered["rows"].empty()&&filtered["plan"]["decorrelation_actual"][0]["inner_loops"]==0,"Pure false outer predicate keeps inner lazy");
    const auto limited=twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k) LIMIT 3",true);check(limited["rows"].size()==3&&limited["plan"]["actual"]["early_stop"]==true,"Unsorted outer LIMIT remains a pull stop");
    const auto zero=twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k) LIMIT 0",true);check(zero["rows"].empty()&&zero["plan"]["decorrelation_actual"][0]["inner_loops"]==0,"LIMIT0 plans without opening inner");
    twin(db,session,"SELECT COUNT(*) FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k)",true);
    const auto explain=ok(db,session,"EXPLAIN "+exists);check(explain["plan"]["decorrelation"]["chosen"]==true&&explain["plan"]["decorrelation_actual"][0]["inner_loops"]==0,"Plain EXPLAIN reports real planned transformation, no data scan");
    ok(db,session,"CREATE TABLE spill_inner(k REAL)");ok(db,session,"CREATE TABLE spill_outer(id INTEGER PRIMARY KEY,k INTEGER)");
    insert="INSERT INTO spill_inner VALUES ";for(int n=0;n<160;++n){if(n)insert+=',';insert+='('+std::to_string(n)+".0)";}ok(db,session,insert);ok(db,session,"INSERT INTO spill_outer VALUES(1,159),(2,159),(3,159),(4,159)");
    {
        Environment tiny("EDUDB_QUERY_OPERATOR_MEMORY_BYTES","65536");const auto spilled=twin(db,session,"SELECT o.id FROM spill_outer o WHERE EXISTS(SELECT 1 FROM spill_inner i WHERE i.k=o.k) ORDER BY o.id",true);check(spilled["plan"]["physical_actual"]["spilled"]==true&&spilled["plan"]["temporary_io"]["bytes_written"].get<std::uint64_t>()>0,"Actual SQL semi path writes CRC framed temporary partitions");
    }
    const auto aggregate=twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT COUNT(*) FROM inner_rows i WHERE i.k=o.k) ORDER BY o.id",false);check(aggregate["rows"].size()==12,"EXISTS aggregate always has its global row, including no matches");
    twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k LIMIT 0)",false);
    twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k LIMIT 1 OFFSET 1)",false);
    twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT DISTINCT 1 FROM inner_rows i WHERE i.k=o.k)",false);
    twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k) OR o.k IS NULL ORDER BY o.id",false);
    twin(db,session,"SELECT o.id FROM outer_rows o WHERE (SELECT COUNT(*) FROM inner_rows i WHERE i.k=o.k)>0 ORDER BY o.id",false);
    twin(db,session,"SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k AND i.id=o.id)",false);
    ok(db,session,"INSERT INTO inner_rows VALUES(300,NULL)");twin(db,session,"SELECT o.id FROM outer_rows o WHERE o.k IN(SELECT k FROM inner_rows) ORDER BY o.id",false);const auto not_in=twin(db,session,"SELECT o.id FROM outer_rows o WHERE o.k NOT IN(SELECT k FROM inner_rows)",false);check(not_in["rows"].empty(),"NOT IN keeps NULL-aware UNKNOWN instead of being mislabelled anti join");
    ok(db,session,"CREATE TABLE large_outer(id INTEGER PRIMARY KEY,k INTEGER)");ok(db,session,"CREATE TABLE large_inner(k REAL)");ok(db,session,"INSERT INTO large_outer VALUES(1,9007199254740993),(2,9007199254740992),(3,9223372036854775807),(4,-9223372036854775808),(5,NULL)");ok(db,session,"INSERT INTO large_inner VALUES(9007199254740992.0),(9223372036854775808.0),(-9223372036854775808.0),(NULL)");
    auto edges=twin(db,session,"SELECT o.id FROM large_outer o WHERE EXISTS(SELECT 1 FROM large_inner i WHERE i.k=o.k) ORDER BY o.id",true);check(edges["rows"]==json::array({json::array({2}),json::array({4})}),"Exact mixed INTEGER/REAL key equality survives decorrelation");
    ok(db,session,"CREATE MATERIALIZED VIEW correlated_rows WITH(refresh='immediate') AS SELECT o.id FROM outer_rows o WHERE EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k)");ok(db,session,"DELETE FROM inner_rows WHERE k=31");const auto materialized=ok(db,session,"SELECT id FROM correlated_rows");check(materialized["rows"].empty(),"Logical WHERE stays intact: correlated MV remains FULL-dependent on inner table");
    db.close_session(session);
    {
        Environment quota("EDUDB_QUERY_MATERIALIZED_BYTE_LIMIT","60000");Environment tiny("EDUDB_QUERY_OPERATOR_MEMORY_BYTES","65536");
        const auto limited_directory=directory.u8string()+"_budget";edudb::Engine limited(limited_directory,4);auto sid=limited.open_session();
        ok(limited,sid,"CREATE TABLE qinner(k REAL)");ok(limited,sid,"CREATE TABLE qouter(id INTEGER PRIMARY KEY,k INTEGER)");std::string values="INSERT INTO qinner VALUES ";for(int n=0;n<160;++n){if(n)values+=',';values+='('+std::to_string(n)+".0)";}ok(limited,sid,values);ok(limited,sid,"INSERT INTO qouter VALUES(1,159),(2,159),(3,159),(4,159)");
        const auto rejected=limited.execute(sid,"SELECT o.id FROM qouter o WHERE EXISTS(SELECT 1 FROM qinner i WHERE i.k=o.k)");check(!rejected.value("ok",true)&&rejected["error"]["code"]=="QUERY_LIMIT_EXCEEDED","Global cumulative materialization guard aborts a partially spilled semi query");
        check(ok(limited,sid,"SELECT 42")["rows"]==json::array({json::array({42})}),"Session continues after bounded semi failure");check(ok(limited,sid,"SELECT COUNT(*) FROM qinner")["rows"]==json::array({json::array({160})}),"Read-only failure does not change committed source rows");
        for(const auto& file:std::filesystem::directory_iterator(std::filesystem::u8path(limited_directory))){check(file.path().filename().u8string().rfind("edudb-query-",0)!=0,"All partial native query runs cleaned after resource failure");}
        limited.close_session(sid);
    }
    std::cout<<"PASS "<<checks<<" actual SQL semi/anti and original Apply dual checks; no performance timing claim\n";
}catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" directory="<<directory.u8string()<<"\n";return 1;}}
