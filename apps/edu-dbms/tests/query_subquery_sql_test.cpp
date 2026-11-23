#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
using edudb::json;
static void option(const char* value){
#ifdef _WIN32
    _putenv_s("EDUDB_OPTIMIZER_DECORRELATE",value);
#else
    if(*value)setenv("EDUDB_OPTIMIZER_DECORRELATE",value,1);else unsetenv("EDUDB_OPTIMIZER_DECORRELATE");
#endif
}
struct OriginalApply {std::string previous;OriginalApply(){const auto* p=std::getenv("EDUDB_OPTIMIZER_DECORRELATE");previous=p?p:"";option("OFF");}~OriginalApply(){option(previous.c_str());}};
static int checks=0;
static void check(bool v,const std::string& s){++checks;if(!v)throw std::runtime_error(s);}
static json ok(edudb::Engine& db,const std::string& session,const std::string& sql){auto r=db.execute(session,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static void rows(edudb::Engine& db,const std::string& session,const std::string& sql,const json& expected){auto r=ok(db,session,sql);check(r["rows"]==expected,sql+" => actual "+r["rows"].dump()+" expected "+expected.dump());}
static void error(edudb::Engine& db,const std::string& session,const std::string& sql,const std::string& code){auto r=db.execute(session,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,sql+" => "+r.dump());rows(db,session,"SELECT 42",json::array({json::array({42})}));}
int main(){
    const auto dir=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB真实相关子查询_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{
        edudb::Engine db(dir.u8string(),4);auto sid=db.open_session();
        ok(db,sid,"CREATE TABLE l(id INTEGER PRIMARY KEY,k INTEGER,tag VARCHAR(8))");
        ok(db,sid,"CREATE TABLE r(id INTEGER PRIMARY KEY,k REAL,tag VARCHAR(8))");
        ok(db,sid,"CREATE TABLE e(id INTEGER,k INTEGER)");
        ok(db,sid,"INSERT INTO l VALUES(1,1,'甲'),(2,2,'乙'),(3,NULL,'空'),(4,1,'丁'),(5,4,'末')");
        ok(db,sid,"INSERT INTO r VALUES(11,1.0,'一'),(12,1.0,'二'),(13,3.0,'三'),(14,NULL,'空')");
        rows(db,sid,"SELECT EXISTS(SELECT 1/0),NOT EXISTS(SELECT 1 FROM e)",json::array({json::array({true,true})}));
        rows(db,sid,"SELECT EXISTS(SELECT 1 FROM e ORDER BY COUNT(*)),EXISTS(SELECT 1 FROM e HAVING TRUE)",json::array({json::array({true,true})}));
        rows(db,sid,"SELECT (SELECT k FROM e), (SELECT 7)",json::array({json::array({nullptr,7})}));
        rows(db,sid,"SELECT NULL IN(SELECT k FROM e),NULL NOT IN(SELECT k FROM e)",json::array({json::array({false,true})}));
        rows(db,sid,"SELECT 2 IN(SELECT k FROM r),2 NOT IN(SELECT k FROM r),1 IN(SELECT k FROM r),NULL IN(SELECT k FROM r)",json::array({json::array({nullptr,nullptr,true,nullptr})}));
        rows(db,sid,"SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r WHERE r.k=l.k) ORDER BY l.id",json::array({json::array({1}),json::array({4})}));
        rows(db,sid,"SELECT l.id FROM l WHERE NOT EXISTS(SELECT 1 FROM r WHERE r.k=l.k) ORDER BY l.id",json::array({json::array({2}),json::array({3}),json::array({5})}));
        rows(db,sid,"SELECT l.id, (SELECT COUNT(*) FROM r WHERE r.k=l.k) AS n FROM l ORDER BY l.id",json::array({json::array({1,2}),json::array({2,0}),json::array({3,0}),json::array({4,2}),json::array({5,0})}));
        rows(db,sid,"SELECT l.id FROM l WHERE l.k IN(SELECT r.k FROM r WHERE r.id>=12) ORDER BY l.id",json::array({json::array({1}),json::array({4})}));
        rows(db,sid,"SELECT l.id FROM l WHERE l.k NOT IN(SELECT r.k FROM r WHERE r.id<14) ORDER BY l.id",json::array({json::array({2}),json::array({5})}));
        rows(db,sid,"SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r WHERE EXISTS(SELECT 1 FROM e WHERE e.k=l.k))",json::array());
        ok(db,sid,"INSERT INTO e VALUES(101,1)");
        rows(db,sid,"SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r WHERE EXISTS(SELECT 1 FROM e WHERE e.k=l.k)) ORDER BY l.id",json::array({json::array({1}),json::array({4})}));
        rows(db,sid,"SELECT l.id,(SELECT r.id FROM r WHERE r.k=l.k ORDER BY r.id LIMIT 1) AS first_id FROM l ORDER BY l.id",json::array({json::array({1,11}),json::array({2,nullptr}),json::array({3,nullptr}),json::array({4,11}),json::array({5,nullptr})}));
        rows(db,sid,"SELECT l.id FROM l WHERE FALSE AND (SELECT 1/0)=1",json::array());
        rows(db,sid,"SELECT l.id FROM l WHERE TRUE OR (SELECT 1/0)=1 ORDER BY l.id",json::array({json::array({1}),json::array({2}),json::array({3}),json::array({4}),json::array({5})}));
        rows(db,sid,"SELECT l.id FROM l WHERE (SELECT 1/0)=1 LIMIT 0",json::array());
        auto explain=ok(db,sid,"EXPLAIN SELECT l.id FROM l WHERE l.k IN(SELECT r.k FROM r)");check(!explain["plan"].contains("subquery_actual"),"EXPLAIN binds and never executes Apply");
        {OriginalApply original;auto actual=ok(db,sid,"EXPLAIN ANALYZE SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r WHERE r.k=l.k)");check(actual["plan"]["subquery_evaluations"].get<int>()==5&&actual["plan"]["subquery_actual"][0]["physical_actual"]["node"]=="CorrelatedApply","Disabled decorrelation retains actual same-transaction Apply");rows(db,sid,"SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r WHERE r.k=l.k) ORDER BY l.id",json::array({json::array({1}),json::array({4})}));}
        auto optimized=ok(db,sid,"EXPLAIN ANALYZE SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r WHERE r.k=l.k)");check(optimized["plan"]["decorrelation"]["chosen"]==true&&optimized["plan"]["decorrelation_actual"][0]["inner_loops"]==1&&optimized["plan"]["subquery_evaluations"]==0,"Enabled decorrelation executes one real inner scan without duplicate Apply");
        error(db,sid,"SELECT (SELECT id FROM r)","CARDINALITY_VIOLATION");
        error(db,sid,"SELECT (SELECT id,k FROM r)","SUBQUERY_COLUMN_COUNT");
        error(db,sid,"SELECT 1 IN(SELECT id,k FROM r)","SUBQUERY_COLUMN_COUNT");
        error(db,sid,"SELECT 'a' IN(SELECT id FROM r)","TYPE_ERROR");
        error(db,sid,"SELECT EXISTS(SELECT missing FROM r)","COLUMN_NOT_FOUND");
        error(db,sid,"SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r l WHERE l.tag=l.missing)","COLUMN_NOT_FOUND");
        rows(db,sid,"SELECT COUNT(*) FROM l WHERE EXISTS(SELECT 1 FROM r WHERE r.k=l.k)",json::array({json::array({2})}));
        error(db,sid,"SELECT COUNT(*),(SELECT 1) FROM l","UNSUPPORTED_SQL");
        ok(db,sid,"BEGIN PROTOCOL SNAPSHOT READ ONLY");
        rows(db,sid,"SELECT l.id FROM l WHERE EXISTS(SELECT 1 FROM r WHERE r.k=l.k) ORDER BY l.id",json::array({json::array({1}),json::array({4})}));
        ok(db,sid,"COMMIT");db.close_session(sid);
        std::cout<<"PASS "<<checks<<" actual Engine SQL Apply/correlation/NULL/empty/scalar/cardinality/snapshot checks\n";
    }catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" directory="<<dir.u8string()<<"\n";return 1;}
}
