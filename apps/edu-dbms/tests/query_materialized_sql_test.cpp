#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool v,const std::string& s){++checks;if(!v)throw std::runtime_error(s);}
static json ok(edudb::Engine& db,const std::string& session,const std::string& sql){auto r=db.execute(session,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static void rows(edudb::Engine& db,const std::string& session,const std::string& sql,const json& expected){auto r=ok(db,session,sql);check(r["rows"]==expected,sql+" => actual "+r["rows"].dump()+" expected "+expected.dump());}
static void error(edudb::Engine& db,const std::string& session,const std::string& sql,const std::string& code){auto r=db.execute(session,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,sql+" => "+r.dump());}
static bool fresh(edudb::Engine& db,const std::string& session,const std::string& name){auto r=ok(db,session,"SHOW MATERIALIZED VIEWS");for(const auto& row:r["rows"])if(row[0]==name)return row[2].get<bool>();throw std::runtime_error("view missing from real catalog");}
int main(){
    const auto dir=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB真实物化视图_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{
        {
            edudb::Engine db(dir.u8string(),4);auto sid=db.open_session();
            ok(db,sid,"CREATE TABLE base_rows(id INTEGER PRIMARY KEY,k INTEGER,tag VARCHAR(8))");
            ok(db,sid,"INSERT INTO base_rows VALUES(1,1,'甲'),(2,1,'甲'),(3,2,'乙')");
            auto create=ok(db,sid,"CREATE MATERIALIZED VIEW manual_rows AS SELECT id,tag FROM base_rows WHERE k=1");
            check(create["refresh"]["maintenance"]=="FULL"&&create["refresh"]["same_transaction"]==true,"Refresh is an actual complete same-transaction SELECT");
            ok(db,sid,"CREATE MATERIALIZED VIEW grouped_rows WITH(refresh='immediate') AS SELECT tag,COUNT(*) AS n,SUM(k) AS total FROM base_rows GROUP BY tag");
            rows(db,sid,"SELECT * FROM manual_rows ORDER BY id",json::array({json::array({1,"甲"}),json::array({2,"甲"})}));
            rows(db,sid,"SELECT tag,n,total FROM grouped_rows ORDER BY tag",json::array({json::array({"乙",1,2}),json::array({"甲",2,2})}));
            check(fresh(db,sid,"manual_rows")&&fresh(db,sid,"grouped_rows"),"Both materializations initially fresh");
            ok(db,sid,"INSERT INTO base_rows VALUES(4,1,'甲')");check(!fresh(db,sid,"manual_rows")&&fresh(db,sid,"grouped_rows"),"Manual staleness and immediate refresh are distinct");
            rows(db,sid,"SELECT COUNT(*) FROM manual_rows",json::array({json::array({2})}));
            rows(db,sid,"SELECT n,total FROM grouped_rows WHERE tag='甲'",json::array({json::array({3,3})}));
            ok(db,sid,"REFRESH MATERIALIZED VIEW manual_rows");check(fresh(db,sid,"manual_rows"),"Manual refresh persists fresh versions");
            rows(db,sid,"SELECT COUNT(*) FROM manual_rows",json::array({json::array({3})}));
            ok(db,sid,"BEGIN");ok(db,sid,"SAVEPOINT before_change");ok(db,sid,"UPDATE base_rows SET k=3 WHERE id=1");
            rows(db,sid,"SELECT total FROM grouped_rows WHERE tag='甲'",json::array({json::array({5})}));
            ok(db,sid,"ROLLBACK TO before_change");rows(db,sid,"SELECT total FROM grouped_rows WHERE tag='甲'",json::array({json::array({3})}));ok(db,sid,"COMMIT");
            auto imported=db.import_data(sid,{{"format","csv"},{"table","base_rows"},{"content","id,k,tag\n5,2,乙\n6,2,乙\n"}});check(imported.value("ok",false),"Actual CSV import => "+imported.dump());
            rows(db,sid,"SELECT n,total FROM grouped_rows WHERE tag='乙'",json::array({json::array({3,6})}));
            auto rejected=db.import_data(sid,{{"format","csv"},{"table","grouped_rows"},{"content","tag,n,total\n坏,1,1\n"}});check(!rejected.value("ok",true)&&rejected["error"]["code"]=="READ_ONLY_MATERIALIZED_VIEW","CSV cannot bypass read-only view protection");
            error(db,sid,"INSERT INTO manual_rows VALUES(99,'坏')","READ_ONLY_MATERIALIZED_VIEW");
            error(db,sid,"UPDATE grouped_rows SET n=99","READ_ONLY_MATERIALIZED_VIEW");
            error(db,sid,"DELETE FROM manual_rows","READ_ONLY_MATERIALIZED_VIEW");
            error(db,sid,"TRUNCATE manual_rows","READ_ONLY_MATERIALIZED_VIEW");
            error(db,sid,"ALTER TABLE base_rows DROP tag","DEPENDENT_OBJECT");
            error(db,sid,"DROP TABLE base_rows","DEPENDENT_OBJECT");
            error(db,sid,"DROP TABLE manual_rows","READ_ONLY_MATERIALIZED_VIEW");
            error(db,sid,"REFRESH MATERIALIZED VIEW base_rows","NOT_MATERIALIZED_VIEW");
            error(db,sid,"CREATE MATERIALIZED VIEW bad_names AS SELECT k,k FROM base_rows","DUPLICATE_COLUMN");
            error(db,sid,"CREATE MATERIALIZED VIEW nested_rows AS SELECT * FROM manual_rows","UNSUPPORTED_SQL");
            ok(db,sid,"CREATE TABLE divisor_rows(k INTEGER)");ok(db,sid,"INSERT INTO divisor_rows VALUES(1)");
            ok(db,sid,"CREATE MATERIALIZED VIEW quotient_rows WITH(refresh='immediate') AS SELECT 1.0/k AS q FROM divisor_rows");
            error(db,sid,"INSERT INTO divisor_rows VALUES(0)","DIVISION_BY_ZERO");
            rows(db,sid,"SELECT k FROM divisor_rows",json::array({json::array({1})}));rows(db,sid,"SELECT q FROM quotient_rows",json::array({json::array({1.0})}));
            ok(db,sid,"DROP MATERIALIZED VIEW quotient_rows");ok(db,sid,"DROP TABLE divisor_rows");
            auto stale=db.export_data(sid,{{"format","sql"}});check(!stale.value("ok",true)&&stale["error"]["code"]=="STALE_MATERIALIZED_EXPORT","Stale MV export does not silently lose historical contents");
            ok(db,sid,"REFRESH MATERIALIZED VIEW manual_rows");ok(db,sid,"CREATE INDEX mv_tag ON grouped_rows(tag)");
            auto dumped=db.export_data(sid,{{"format","sql"}});check(dumped.value("ok",false),"Logical SQL export => "+dumped.dump());check(dumped["content"].get<std::string>().find("CREATE MATERIALIZED VIEW")!=std::string::npos,"Dump keeps materialized definition rather than changing it to an ordinary table");
            {
                edudb::Engine restored(dir.u8string()+"_logical_copy",4);auto copy=restored.open_session();auto imported_copy=restored.import_data(copy,{{"format","sql"},{"content",dumped["content"]}});check(imported_copy.value("ok",false),"Logical materialized import => "+imported_copy.dump());
                rows(restored,copy,"SELECT n,total FROM grouped_rows WHERE tag='乙'",json::array({json::array({3,6})}));check(fresh(restored,copy,"grouped_rows"),"Logical definition/state reconstituted");ok(restored,copy,"INSERT INTO base_rows VALUES(7,2,'乙')");rows(restored,copy,"SELECT n,total FROM grouped_rows WHERE tag='乙'",json::array({json::array({4,8})}));restored.close_session(copy);
            }
            ok(db,sid,"BEGIN PROTOCOL SNAPSHOT");error(db,sid,"INSERT INTO base_rows VALUES(8,1,'拒')","UNSUPPORTED_MATERIALIZED_PROTOCOL");ok(db,sid,"ROLLBACK");rows(db,sid,"SELECT COUNT(*) FROM base_rows",json::array({json::array({6})}));

            db.close_session(sid);
        }
        {
            edudb::Engine db(dir.u8string(),4);auto sid=db.open_session();
            rows(db,sid,"SELECT n,total FROM grouped_rows WHERE tag='乙'",json::array({json::array({3,6})}));check(fresh(db,sid,"grouped_rows"),"Definition/state/rows survive actual engine reopen");
            ok(db,sid,"DELETE FROM base_rows WHERE id=6");rows(db,sid,"SELECT n,total FROM grouped_rows WHERE tag='乙'",json::array({json::array({2,4})}));
            ok(db,sid,"TRUNCATE base_rows");rows(db,sid,"SELECT * FROM grouped_rows",json::array());
            ok(db,sid,"DROP MATERIALIZED VIEW grouped_rows");ok(db,sid,"DROP MATERIALIZED VIEW manual_rows");ok(db,sid,"DROP TABLE base_rows");db.close_session(sid);
        }
        std::cout<<"PASS "<<checks<<" actual materialized-view SQL/CSV/savepoint/atomicity/reopen checks; FULL refresh only\n";
    }catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" directory="<<dir.u8string()<<"\n";return 1;}
}
