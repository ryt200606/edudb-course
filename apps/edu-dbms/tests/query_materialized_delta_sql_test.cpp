#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& text){++checks;if(!value)throw std::runtime_error(text);}
static json ok(edudb::Engine& db,const std::string& s,const std::string& sql){auto r=db.execute(s,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static void rows(edudb::Engine& db,const std::string& s,const std::string& sql,const json& expected){auto r=ok(db,s,sql);check(r["rows"]==expected,sql+" => "+r["rows"].dump()+" expected "+expected.dump());}
static void error(edudb::Engine& db,const std::string& s,const std::string& sql,const std::string& code){auto r=db.execute(s,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,sql+" => "+r.dump());}
static void compare(edudb::Engine& db,const std::string& s){
    for(const auto& pair:{std::pair<std::string,std::string>{"SELECT g,x FROM source_rows WHERE x>1 ORDER BY g,x","SELECT g,x FROM selected_rows ORDER BY g,x"},{"SELECT g,COUNT(*) AS n,COUNT(x) AS present,SUM(x) AS total,AVG(x) AS mean FROM source_rows GROUP BY g ORDER BY g","SELECT g,n,present,total,mean FROM grouped_rows ORDER BY g"},{"SELECT COUNT(*) AS n,COUNT(x) AS present,SUM(x) AS total,AVG(x) AS mean FROM source_rows","SELECT n,present,total,mean FROM global_rows"}}){const auto expected=ok(db,s,pair.first),actual=ok(db,s,pair.second);check(expected["rows"]==actual["rows"],"Bound SQL and persistent delta result differ: "+pair.first+" expected="+expected["rows"].dump()+" actual="+actual["rows"].dump());}
}
static void incremental(const json& result){
    check(result.contains("materialized_refreshes"),"Actual native mutation invokes shared after-hook");bool sp=false,group=false;
    for(const auto& receipt:result["materialized_refreshes"]){if(receipt["view"]=="selected_rows"){sp=receipt["maintenance"]=="INCREMENTAL_SP";check(receipt["base_rescan_rows"]==0,"SP delta performs no base scan");}if(receipt["view"]=="grouped_rows"){group=receipt["maintenance"]=="INCREMENTAL_GROUP";check(receipt["base_rescan_rows"]==0,"Group delta performs no base scan");}if(receipt["view"]=="minimum_rows")check(receipt["maintenance"]=="FULL","MIN/MAX visibly use complete refresh");}
    check(sp&&group,"Both real SP/group delta paths were executed");
}
int main(){const auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB真实增量物化_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));try{
    {
        edudb::Engine db(directory.u8string(),4);auto s=db.open_session();ok(db,s,"CREATE TABLE source_rows(id INTEGER PRIMARY KEY,g VARCHAR(8),x REAL)");
        ok(db,s,"INSERT INTO source_rows VALUES(1,'甲',2.0),(2,'甲',2.0),(3,'乙',NULL),(4,NULL,8.0)");
        ok(db,s,"CREATE MATERIALIZED VIEW selected_rows WITH(refresh='immediate',maintenance='incremental') AS SELECT g,x FROM source_rows WHERE x>1");
        ok(db,s,"CREATE MATERIALIZED VIEW grouped_rows WITH(refresh='immediate',maintenance='incremental') AS SELECT g,COUNT(*) AS n,COUNT(x) AS present,SUM(x) AS total,AVG(x) AS mean FROM source_rows GROUP BY g");
        ok(db,s,"CREATE MATERIALIZED VIEW global_rows WITH(refresh='immediate') AS SELECT COUNT(*) AS n,COUNT(x) AS present,SUM(x) AS total,AVG(x) AS mean FROM source_rows");
        ok(db,s,"CREATE MATERIALIZED VIEW minimum_rows WITH(refresh='immediate') AS SELECT MIN(x) AS smallest FROM source_rows");compare(db,s);
        incremental(ok(db,s,"INSERT INTO source_rows VALUES(5,'乙',4.0),(6,'甲',NULL)"));compare(db,s);
        incremental(ok(db,s,"UPDATE source_rows SET g='乙',x=6.0 WHERE id=1"));compare(db,s);
        incremental(ok(db,s,"DELETE FROM source_rows WHERE id=2"));compare(db,s);
        incremental(ok(db,s,"UPDATE source_rows SET x=x+1 WHERE x IS NOT NULL"));compare(db,s);
        ok(db,s,"BEGIN");ok(db,s,"SAVEPOINT before_delta");incremental(ok(db,s,"INSERT INTO source_rows VALUES(7,'甲',10.0)"));compare(db,s);
        ok(db,s,"ROLLBACK TO before_delta");compare(db,s);incremental(ok(db,s,"INSERT INTO source_rows VALUES(8,'乙',3.0)"));compare(db,s);ok(db,s,"COMMIT");
        auto csv=db.import_data(s,{{"format","csv"},{"table","source_rows"},{"content","id,g,x\n9,甲,5.0\n10,甲,5.0\n"}});check(csv.value("ok",false),"CSV native delta => "+csv.dump());incremental(csv);compare(db,s);
        auto invalid=db.import_data(s,{{"format","csv"},{"table","source_rows"},{"content","id,g,x\n11,甲,7.0\n12,甲,wrong\n"}});check(!invalid.value("ok",true),"CSV failure is real");compare(db,s);
        auto scripted=db.import_data(s,{{"format","sql"},{"content","INSERT INTO source_rows VALUES(13,'甲',7.0); UPDATE source_rows SET x=9.0 WHERE id=13;"}});check(scripted.value("ok",false),"SQL import shared delta => "+scripted.dump());compare(db,s);
        error(db,s,"CREATE MATERIALIZED VIEW bad_incremental WITH(maintenance='incremental') AS SELECT MIN(x) AS m FROM source_rows","UNSUPPORTED_INCREMENTAL_VIEW");
        auto catalog=ok(db,s,"SHOW TABLES");for(const auto& row:catalog["rows"])check(row[0].get<std::string>().rfind("__mv_state_",0)!=0,"Auxiliary tables are hidden from normal catalog listing");
        error(db,s,"REORGANIZE grouped_rows","READ_ONLY_MATERIALIZED_VIEW");error(db,s,"REORGANIZE source_rows","DEPENDENT_OBJECT");
        std::uint64_t auxiliary_hash=1469598103934665603ull;for(unsigned char c:std::string("grouped_rows")){auxiliary_hash^=c;auxiliary_hash*=1099511628211ull;}std::ostringstream auxiliary;auxiliary<<"__mv_state_"<<std::hex<<auxiliary_hash;
        error(db,s,"REORGANIZE "+auxiliary.str(),"READ_ONLY_MATERIALIZED_VIEW");compare(db,s);
        const auto reset=ok(db,s,"TRUNCATE source_rows");for(const auto& receipt:reset["materialized_refreshes"])check(receipt["maintenance"]=="FULL","RESET always rebuilds complete consistent materialization/state");compare(db,s);
        rows(db,s,"SELECT n,present,total,mean FROM global_rows",json::array({json::array({0,0,nullptr,nullptr})}));
        incremental(ok(db,s,"INSERT INTO source_rows VALUES(1,'重启',3.0)"));compare(db,s);db.close_session(s);
    }
    {
        edudb::Engine db(directory.u8string(),4);auto s=db.open_session();compare(db,s);incremental(ok(db,s,"UPDATE source_rows SET x=4.0 WHERE id=1"));compare(db,s);
        ok(db,s,"DROP MATERIALIZED VIEW selected_rows");ok(db,s,"DROP MATERIALIZED VIEW grouped_rows");ok(db,s,"DROP MATERIALIZED VIEW global_rows");ok(db,s,"DROP MATERIALIZED VIEW minimum_rows");ok(db,s,"DROP TABLE source_rows");db.close_session(s);
    }
    std::cout<<"PASS "<<checks<<" actual feed-backed SP/group MV delta, CSV/SQL, RESET, savepoint and reopen checks\n";
}catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" directory="<<directory.u8string()<<"\n";return 1;}}
