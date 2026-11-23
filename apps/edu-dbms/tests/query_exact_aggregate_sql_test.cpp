#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool v,const std::string& s){++checks;if(!v)throw std::runtime_error(s);}
static json ok(edudb::Engine& db,const std::string& session,const std::string& sql){auto r=db.execute(session,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static void rows(edudb::Engine& db,const std::string& session,const std::string& sql,const json& expected){auto r=ok(db,session,sql);check(r["rows"]==expected,sql+" => actual "+r["rows"].dump()+" expected "+expected.dump());}
static void error(edudb::Engine& db,const std::string& session,const std::string& sql,const std::string& code){auto r=db.execute(session,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,sql+" => "+r.dump());}
int main(){try{
    const auto dir=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB精确查询聚合_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));edudb::Engine db(dir.u8string(),4);auto sid=db.open_session();
    ok(db,sid,"CREATE TABLE ints(k INTEGER)");ok(db,sid,"INSERT INTO ints VALUES(9223372036854775807),(1),(-1),(NULL)");
    rows(db,sid,"SELECT SUM(k),COUNT(k),COUNT(*) FROM ints",json::array({json::array({std::numeric_limits<std::int64_t>::max(),3,4})}));
    rows(db,sid,"SELECT AVG(k) FROM ints",json::array({json::array({3074457345618258602.0})}));
    ok(db,sid,"INSERT INTO ints VALUES(1)");error(db,sid,"SELECT SUM(k) FROM ints","NUMERIC_OVERFLOW");
    ok(db,sid,"CREATE TABLE reals(k REAL)");ok(db,sid,"INSERT INTO reals VALUES(1.7976931348623157e308),(1.7976931348623157e308),(NULL)");
    rows(db,sid,"SELECT AVG(k),COUNT(k),COUNT(*) FROM reals",json::array({json::array({std::numeric_limits<double>::max(),2,3})}));
    error(db,sid,"SELECT SUM(k) FROM reals","NUMERIC_OVERFLOW");ok(db,sid,"TRUNCATE reals");ok(db,sid,"INSERT INTO reals VALUES(1e300),(1.0),(-1e300)");
    rows(db,sid,"SELECT SUM(k) FROM reals",json::array({json::array({1.0})}));
    rows(db,sid,"SELECT SUM(k),AVG(k),COUNT(k) FROM reals WHERE FALSE",json::array({json::array({nullptr,nullptr,0})}));
    ok(db,sid,"CREATE MATERIALIZED VIEW exact_rows WITH(refresh='immediate') AS SELECT SUM(k) AS total,AVG(k) AS mean,COUNT(k) AS n FROM reals");
    rows(db,sid,"SELECT total,n FROM exact_rows",json::array({json::array({1.0,3})}));ok(db,sid,"INSERT INTO reals VALUES(2.0)");rows(db,sid,"SELECT total,mean,n FROM exact_rows",json::array({json::array({3.0,0.75,4})}));
    db.close_session(sid);std::cout<<"PASS "<<checks<<" actual private SQL exact SUM/AVG and complete-refresh consistency checks\n";
}catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<"\n";return 1;}}
