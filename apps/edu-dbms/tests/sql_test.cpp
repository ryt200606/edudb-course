#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
using edudb::Engine;
using edudb::json;
namespace fs=std::filesystem;
static int checks=0;
void check(bool value,const std::string& message){++checks;if(!value)throw std::runtime_error(message);}
json ok(Engine& db,const std::string& session,const std::string& sql){auto r=db.execute(session,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
json bad(Engine& db,const std::string& session,const std::string& sql,const std::string& code){auto r=db.execute(session,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,sql+" expected "+code+", got "+r.dump());return r;}
void rows(Engine& db,const std::string& session,const std::string& sql,const json& expected){auto r=ok(db,session,sql);check(r["rows"]==expected,sql+" result "+r["rows"].dump()+" != "+expected.dump());}
void wait_for_waiter(Engine& db){auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);while(std::chrono::steady_clock::now()<deadline){if(!db.diagnostics()["waits"].empty())return;std::this_thread::sleep_for(std::chrono::milliseconds(2));}throw std::runtime_error("Expected lock waiter was not observed");}

struct ScopedEnvironment {
    std::string name, previous;
    bool present;
    static void set(const std::string& name,const std::string& value,bool remove=false) {
#ifdef _WIN32
        _putenv_s(name.c_str(),remove?"":value.c_str());
#else
        if(remove)unsetenv(name.c_str());else setenv(name.c_str(),value.c_str(),1);
#endif
    }
    ScopedEnvironment(std::string key,std::string value):name(std::move(key)) {
        auto raw=std::getenv(name.c_str());present=raw!=nullptr;if(raw)previous=raw;
        set(name,value);
    }
    ~ScopedEnvironment(){set(name,previous,!present);}
};

void grouping_cases(Engine& db,const std::string& session) {
    ok(db,session,"CREATE TABLE grouped_rows(id INTEGER PRIMARY KEY,department VARCHAR(8),amount INTEGER,flag BOOLEAN)");
    ok(db,session,"INSERT INTO grouped_rows VALUES(1,'A',10,TRUE),(2,'A',20,FALSE),(3,'B',NULL,TRUE),(4,NULL,8,TRUE),(5,NULL,12,FALSE)");
    rows(db,session,"SELECT department,COUNT(*),COUNT(amount),SUM(amount),AVG(amount) FROM grouped_rows GROUP BY department ORDER BY department",
         json::array({json::array({"A",2,2,30,15.0}),json::array({"B",1,0,nullptr,nullptr}),json::array({nullptr,2,2,20,10.0})}));
    rows(db,session,"SELECT department AS dept,SUM(amount) AS total FROM grouped_rows GROUP BY department HAVING SUM(amount)>=20 ORDER BY total DESC",
         json::array({json::array({"A",30}),json::array({nullptr,20})}));
    rows(db,session,"SELECT department AS dept,COUNT(*) AS n FROM grouped_rows GROUP BY dept ORDER BY n DESC,dept",
         json::array({json::array({"A",2}),json::array({nullptr,2}),json::array({"B",1})}));
    rows(db,session,"SELECT flag,COUNT(*) FROM grouped_rows GROUP BY 1 ORDER BY 1",
         json::array({json::array({false,2}),json::array({true,3})}));
    rows(db,session,"SELECT amount+1 AS next_value,COUNT(*) FROM grouped_rows GROUP BY amount+1 HAVING COUNT(*)=1 ORDER BY next_value LIMIT 2",
         json::array({json::array({9,1}),json::array({11,1})}));
    rows(db,session,"SELECT COUNT(*) FROM grouped_rows WHERE id<0 HAVING COUNT(*)=0",json::array({json::array({0})}));
    rows(db,session,"SELECT department,COUNT(*) FROM grouped_rows WHERE id<0 GROUP BY department",json::array());
    rows(db,session,"SELECT department FROM grouped_rows GROUP BY department HAVING SUM(amount) IS NULL",json::array({json::array({"B"})}));
    bad(db,session,"SELECT id,COUNT(*) FROM grouped_rows GROUP BY department","INVALID_AGGREGATE");
    bad(db,session,"SELECT department,COUNT(*) FROM grouped_rows GROUP BY SUM(amount)","INVALID_AGGREGATE");
    bad(db,session,"SELECT department,COUNT(*) FROM grouped_rows GROUP BY department HAVING id>1","INVALID_AGGREGATE");
    bad(db,session,"SELECT SUM(COUNT(*)) FROM grouped_rows","INVALID_AGGREGATE");
    bad(db,session,"SELECT department,COUNT(*) FROM grouped_rows GROUP BY department ORDER BY id","INVALID_AGGREGATE");
    bad(db,session,"SELECT DISTINCT department FROM grouped_rows ORDER BY amount","INVALID_ORDER");
    rows(db,session,"SELECT SUM(amount)+1 AS total FROM grouped_rows HAVING SUM(amount)>0 ORDER BY total",json::array({json::array({51})}));
    rows(db,session,"SELECT department,COUNT(*) FROM grouped_rows GROUP BY department HAVING SUM(amount)=NULL",json::array());
    rows(db,session,"SELECT department,COUNT(*) AS n FROM grouped_rows GROUP BY department ORDER BY n DESC,department LIMIT 1 OFFSET 1",
         json::array({json::array({nullptr,2})}));
    ok(db,session,"CREATE TABLE grouped_join(department VARCHAR(8),label VARCHAR(8))");
    ok(db,session,"INSERT INTO grouped_join VALUES('A','alpha'),('B','beta')");
    rows(db,session,"SELECT j.label,SUM(g.amount),COUNT(*) FROM grouped_rows g JOIN grouped_join j ON g.department=j.department GROUP BY j.label ORDER BY j.label",
         json::array({json::array({"alpha",30,2}),json::array({"beta",nullptr,1})}));
    ok(db,session,"DROP TABLE grouped_join");ok(db,session,"DROP TABLE grouped_rows");
    ok(db,session,"CREATE TABLE real_zeros(value REAL)");
    ok(db,session,"INSERT INTO real_zeros VALUES(0.0),(-0.0),(0),(NULL)");
    rows(db,session,"SELECT value,COUNT(*) FROM real_zeros GROUP BY value ORDER BY value",
         json::array({json::array({0.0,3}),json::array({nullptr,1})}));
    rows(db,session,"SELECT DISTINCT value FROM real_zeros ORDER BY value",
         json::array({json::array({0.0}),json::array({nullptr})}));
    ok(db,session,"DROP TABLE real_zeros");
    ok(db,session,"CREATE TABLE alias_conflict(a INTEGER,b INTEGER)");
    ok(db,session,"INSERT INTO alias_conflict VALUES(1,7),(2,7)");
    bad(db,session,"SELECT a AS b,COUNT(*) FROM alias_conflict GROUP BY b","INVALID_AGGREGATE");
    rows(db,session,"SELECT b AS a,COUNT(*) FROM alias_conflict GROUP BY b ORDER BY a",json::array({json::array({7,2})}));
    rows(db,session,"SELECT a AS b FROM alias_conflict ORDER BY b DESC",json::array({json::array({2}),json::array({1})}));
    ok(db,session,"DROP TABLE alias_conflict");
}

void planner_and_incremental_cases(Engine& db,const std::string& session) {
    ok(db,session,"CREATE TABLE plan_rows(id INTEGER PRIMARY KEY,k INTEGER,m INTEGER,payload VARCHAR(1000))");
    std::string sql="INSERT INTO plan_rows VALUES";
    for(int i=0;i<600;++i){if(i)sql+=",";sql+="("+std::to_string(i)+","+std::to_string(i%6)+","+std::to_string(i)+",'x')";}
    ok(db,session,sql);ok(db,session,"CREATE INDEX ix_plan_k ON plan_rows(k)");
    ok(db,session,"CREATE UNIQUE INDEX ix_plan_m ON plan_rows(m)");
    auto explain=ok(db,session,"EXPLAIN SELECT id FROM plan_rows WHERE k>=0 AND m=321");
    check(explain["plan"]["sources"][0]["index"]=="ix_plan_m","Planner must enumerate and choose unique equality over first broad index");
    check(explain["plan"]["sources"][0]["candidates"].size()==3,"Seq and both qualifying index candidates are visible");
    check(explain["metrics"].value("rows_scanned",0)==0&&explain["plan"]["sources"][0].value("loops",0)==0&&
          !explain["plan"].contains("actual")&&!explain["plan"].contains("physical_actual"),"EXPLAIN leaves execution operators unopened");
    const auto& planning=explain["plan"]["planning_metadata_io"];
    check(planning.is_object()&&explain["metrics"]["planning_metadata_io"]==planning,"EXPLAIN reports planning metadata accesses separately");
    check(explain["metrics"]["heap_page_reads"].get<std::uint64_t>()>=planning.value("heap_page_reads",std::uint64_t(0))&&
          explain["metrics"]["index_page_reads"].get<std::uint64_t>()>=planning.value("index_page_reads",std::uint64_t(0)),"EXPLAIN total counters retain real planning I/O");
    auto actual=ok(db,session,"SELECT id FROM plan_rows WHERE k>=0 AND m=321");
    check(actual["rows"]==json::array({json::array({321})}),"Selected index returns correct row");
    check(actual["plan"]["sources"][0]["estimated_cost"]==explain["plan"]["sources"][0]["estimated_cost"],"EXPLAIN and execution share costed plan");
    auto bounded=ok(db,session,"SELECT id FROM plan_rows WHERE m>=400 AND m>401 AND m<405 AND m<=499 ORDER BY id");
    check(bounded["rows"]==json::array({json::array({402}),json::array({403}),json::array({404})}),"Merged index range rows");
    check(bounded["plan"]["sources"][0]["lower"]==401&&!bounded["plan"]["sources"][0]["lower_inclusive"].get<bool>(),"Tightest lower bound is exclusive 401");
    check(bounded["plan"]["sources"][0]["upper"]==405&&!bounded["plan"]["sources"][0]["upper_inclusive"].get<bool>(),"Tightest upper bound is exclusive 405");
    auto impossible=ok(db,session,"SELECT id FROM plan_rows WHERE m>3 AND m<3");
    check(impossible["rows"].empty()&&impossible["metrics"]["rows_scanned"]==0,"Contradictory bounds avoid scanning");
    rows(db,session,"SELECT id FROM plan_rows WHERE 598 < m AND 601 >= m",json::array({json::array({599})}));
    auto no_rows=ok(db,session,"SELECT id FROM plan_rows LIMIT 0");
    check(no_rows["rows"].empty()&&no_rows["metrics"]["rows_scanned"]==0,"LIMIT zero never pulls a row");
    auto limited=ok(db,session,"SELECT id FROM plan_rows WHERE m>=400 AND m<500 LIMIT 2");
    check(limited["rows"]==json::array({json::array({400}),json::array({401})})&&limited["metrics"]["rows_scanned"]==2,"Streaming LIMIT stops cursor early");
    check(limited["plan"]["actual"]["early_stop"]==true,"Plan records streaming early stop");
    auto analyzed=ok(db,session,"EXPLAIN ANALYZE SELECT id FROM plan_rows WHERE m>=400 AND m<500 LIMIT 2");
    check(analyzed["plan"]["actual"]["actual_rows"]==2&&analyzed["plan"]["sources"][0]["rows_examined"]==2,"EXPLAIN ANALYZE executes selected plan");
    check(analyzed["plan"]["actual"].contains("heap_page_reads"),"ANALYZE contains actual transaction IO");
    auto full=ok(db,session,"SELECT id FROM plan_rows WHERE k>=0");
    auto first=ok(db,session,"SELECT id FROM plan_rows WHERE k>=0 LIMIT 1");
    check(first["metrics"]["rows_scanned"]==1&&full["metrics"]["rows_scanned"]==600,"Early limit reduces tuples pulled");
    check(first["metrics"]["heap_page_reads"].get<std::uint64_t>()<full["metrics"]["heap_page_reads"].get<std::uint64_t>(),"Early limit reduces heap page accesses");
    auto update=ok(db,session,"UPDATE plan_rows SET m=m+1000 WHERE m>=0 AND m<10");
    check(update["affected_rows"]==10,"Halloween protection updates ten original RIDs exactly once");
    rows(db,session,"SELECT COUNT(*) FROM plan_rows WHERE m>=1000",json::array({json::array({10})}));
    auto update_index=ok(db,session,"UPDATE plan_rows SET k=k+10 WHERE k<3");
    check(update_index["affected_rows"]==300,"Nonunique index-key update visits frozen RID set");
    rows(db,session,"SELECT COUNT(*) FROM plan_rows WHERE k>=10",json::array({json::array({300})}));
    auto payload=std::string(900,'p');
    ok(db,session,"UPDATE plan_rows SET payload='"+payload+"' WHERE id=1");
    rows(db,session,"SELECT payload FROM plan_rows WHERE id=1",json::array({json::array({payload})}));
    auto del=ok(db,session,"DELETE FROM plan_rows WHERE k>=10");check(del["affected_rows"]==300,"Incremental indexed delete");
    rows(db,session,"SELECT COUNT(*) FROM plan_rows",json::array({json::array({300})}));
    ok(db,session,"BEGIN");ok(db,session,"UPDATE plan_rows SET payload='rollback' WHERE id=4");ok(db,session,"ROLLBACK");
    rows(db,session,"SELECT payload FROM plan_rows WHERE id=4",json::array({json::array({"x"})}));
    ok(db,session,"DROP TABLE plan_rows");
}

void timeout_cases(const fs::path& root) {
    {
        ScopedEnvironment timeout("EDUDB_LOCK_TIMEOUT_MS","80");
        Engine db((root/"lock-timeout").string());auto a=db.open_session(),b=db.open_session();
        ok(db,a,"CREATE TABLE t(id INTEGER PRIMARY KEY,value INTEGER)");ok(db,a,"INSERT INTO t VALUES(1,0)");
        ok(db,a,"BEGIN");ok(db,a,"UPDATE t SET value=99");ok(db,b,"BEGIN");
        auto start=std::chrono::steady_clock::now();bad(db,b,"SELECT * FROM t","LOCK_TIMEOUT");
        auto elapsed=std::chrono::steady_clock::now()-start;
        check(elapsed>=std::chrono::milliseconds(40)&&elapsed<std::chrono::seconds(2),"Injected lock deadline is actually enforced");
        bad(db,b,"SELECT 1","TRANSACTION_ABORTED");ok(db,b,"ROLLBACK");ok(db,a,"ROLLBACK");
        rows(db,b,"SELECT value FROM t",json::array({json::array({0})}));
    }
    {
        ScopedEnvironment idle("EDUDB_IDLE_TIMEOUT_MS","250");
        ScopedEnvironment lock("EDUDB_LOCK_TIMEOUT_MS","1500");
        Engine db((root/"idle-timeout").string());auto a=db.open_session(),b=db.open_session();
        ok(db,a,"CREATE TABLE t(id INTEGER PRIMARY KEY,value INTEGER)");ok(db,a,"INSERT INTO t VALUES(1,0)");
        ok(db,a,"BEGIN");ok(db,a,"UPDATE t SET value=99");
        auto waiting=std::async(std::launch::async,[&]{return db.execute(b,"SELECT value FROM t");});
        wait_for_waiter(db);
        check(waiting.wait_for(std::chrono::seconds(2))==std::future_status::ready,"Idle transaction releases conflicting lock");
        auto result=waiting.get();check(result["ok"]==true&&result["rows"]==json::array({json::array({0})}),"Idle rollback discards private update");
        bad(db,a,"SELECT 1","TRANSACTION_ABORTED");ok(db,a,"ROLLBACK");
    }
}
void adversarial_cases(Engine& db,const std::string& session) {
    rows(db,session,"SELECT 1e-308,1e-320,4.9406564584124654e-324,-4.9406564584124654e-324",
         json::array({json::array({1e-308,1e-320,std::numeric_limits<double>::denorm_min(),-std::numeric_limits<double>::denorm_min()})}));
    bad(db,session,"SELECT 1e309","NUMERIC_OVERFLOW");
    bad(db,session,"SELECT 1e-999","NUMERIC_OVERFLOW");
    ok(db,session,"CREATE TABLE edge_defaults(i INTEGER DEFAULT +1,r REAL DEFAULT +1.5,n INTEGER DEFAULT -NULL)");
    ok(db,session,"INSERT INTO edge_defaults DEFAULT VALUES");
    rows(db,session,"SELECT * FROM edge_defaults",json::array({json::array({1,1.5,nullptr})}));
    bad(db,session,"CREATE TABLE edge_default_bad(i INTEGER DEFAULT +TRUE)","TYPE_ERROR");
    ok(db,session,"DROP TABLE edge_defaults");
    ok(db,session,"CREATE TABLE edge_utf8(value VARCHAR(2))");
    ok(db,session,"INSERT INTO edge_utf8 VALUES('中😀')");
    rows(db,session,"SELECT value FROM edge_utf8",json::array({json::array({"中😀"})}));
    bad(db,session,"INSERT INTO edge_utf8 VALUES('中😀文')","VALUE_TOO_LONG");
    bad(db,session,"INSERT INTO edge_utf8 VALUES('"+std::string("\xc0\xaf",2)+"')","INVALID_UTF8");
    rows(db,session,"SELECT COUNT(*) FROM edge_utf8",json::array({json::array({1})}));
    ok(db,session,"DROP TABLE edge_utf8");
    ok(db,session,"CREATE TABLE edge_assign(id INTEGER PRIMARY KEY,s VARCHAR(3),b BOOLEAN,r REAL,i INTEGER)");
    bad(db,session,"UPDATE edge_assign SET s=1","TYPE_ERROR");
    bad(db,session,"UPDATE edge_assign SET b=1","TYPE_ERROR");
    bad(db,session,"UPDATE edge_assign SET i=1.5","TYPE_ERROR");
    bad(db,session,"UPDATE edge_assign SET r=TRUE","TYPE_ERROR");
    ok(db,session,"UPDATE edge_assign SET s=NULL,b=FALSE,r=1,i=2");
    ok(db,session,"INSERT INTO edge_assign VALUES(1,'甲',FALSE,0,0),(2,'乙',TRUE,1,1)");
    bad(db,session,"UPDATE edge_assign SET i=i+9223372036854775807","NUMERIC_OVERFLOW");
    rows(db,session,"SELECT i FROM edge_assign ORDER BY id",json::array({json::array({0}),json::array({1})}));
    bad(db,session,"UPDATE edge_assign SET s=1 WHERE FALSE","TYPE_ERROR");
    bad(db,session,"INSERT INTO edge_assign VALUES(3,'丙',FALSE,2,2),(1,'重复',TRUE,3,3)","UNIQUE_VIOLATION");
    rows(db,session,"SELECT id FROM edge_assign ORDER BY id",json::array({json::array({1}),json::array({2})}));
    ok(db,session,"BEGIN");ok(db,session,"UPDATE edge_assign SET i=10 WHERE id=1");
    bad(db,session,"INSERT INTO edge_assign VALUES(3,'丙',FALSE,2,2),(1,'重复',TRUE,3,3)","UNIQUE_VIOLATION");
    bad(db,session,"COMMIT","TRANSACTION_ABORTED");ok(db,session,"ROLLBACK");
    rows(db,session,"SELECT id,i FROM edge_assign ORDER BY id",json::array({json::array({1,0}),json::array({2,1})}));
    ok(db,session,"DROP TABLE edge_assign");

    ok(db,session,"CREATE TABLE edge_rename(id INTEGER PRIMARY KEY,k INTEGER UNIQUE)");
    ok(db,session,"INSERT INTO edge_rename VALUES(1,7)");
    ok(db,session,"ALTER TABLE edge_rename RENAME COLUMN k TO j");
    ok(db,session,"ALTER TABLE edge_rename ADD COLUMN k INTEGER UNIQUE");
    ok(db,session,"INSERT INTO edge_rename VALUES(2,8,9)");
    bad(db,session,"INSERT INTO edge_rename VALUES(3,7,10)","UNIQUE_VIOLATION");
    bad(db,session,"INSERT INTO edge_rename VALUES(3,10,9)","UNIQUE_VIOLATION");
    ok(db,session,"CREATE INDEX edge_user_index ON edge_rename(j)");
    ok(db,session,"ALTER TABLE edge_rename RENAME COLUMN j TO j2");
    ok(db,session,"ALTER TABLE edge_rename RENAME TO edge_renamed");
    auto indexes=ok(db,session,"SHOW INDEXES FROM edge_renamed");
    json legacy_indexes=json::array();for(const auto& row:indexes["rows"]){check(row.size()==7&&row[3]=="BPLUS","SHOW INDEXES retains method and extended metadata");legacy_indexes.push_back(json::array({row[0],row[1],row[2]}));}
    check(legacy_indexes==json::array({json::array({"__pk_edge_renamed","id",true}),
          json::array({"__uq_edge_renamed_j2","j2",true}),json::array({"__uq_edge_renamed_k","k",true}),
          json::array({"edge_user_index","j2",false})}),"Constraint rename follows column/table; user index name remains stable");
    rows(db,session,"SELECT id,j2,k FROM edge_renamed ORDER BY id",json::array({json::array({1,7,nullptr}),json::array({2,8,9})}));
    ok(db,session,"DROP TABLE edge_renamed");

    ok(db,session,"CREATE TABLE edge_alias(a INTEGER,b INTEGER)");
    ok(db,session,"INSERT INTO edge_alias VALUES(1,9),(2,8)");
    bad(db,session,"SELECT a,b AS a FROM edge_alias ORDER BY a","AMBIGUOUS_COLUMN");
    rows(db,session,"SELECT a AS x,a AS x FROM edge_alias ORDER BY x",json::array({json::array({1,1}),json::array({2,2})}));
    rows(db,session,"SELECT a,a FROM edge_alias ORDER BY a",json::array({json::array({1,1}),json::array({2,2})}));
    rows(db,session,"SELECT b AS a FROM edge_alias ORDER BY a",json::array({json::array({8}),json::array({9})}));
    rows(db,session,"SELECT COUNT(*) FROM edge_alias ORDER BY count",json::array({json::array({2})}));
    bad(db,session,"SELECT a AS b,COUNT(*) FROM edge_alias GROUP BY b","INVALID_AGGREGATE");
    ok(db,session,"DROP TABLE edge_alias");

    std::string addition="SELECT 1";
    for(int i=0;i<127;++i)addition+="+1";
    rows(db,session,addition,json::array({json::array({128})}));
    bad(db,session,addition+"+1","QUERY_TOO_COMPLEX");
    std::string negation="SELECT ";for(int i=0;i<127;++i)negation+="NOT ";negation+="TRUE";
    rows(db,session,negation,json::array({json::array({false})}));
    bad(db,session,"SELECT NOT "+negation.substr(7),"QUERY_TOO_COMPLEX");
    rows(db,session,"SELECT "+std::string(127,'(')+"1"+std::string(127,')'),json::array({json::array({1})}));
    bad(db,session,"SELECT "+std::string(128,'(')+"1"+std::string(128,')'),"QUERY_TOO_COMPLEX");
    std::string excessive_not="SELECT ",excessive_add="SELECT 1",excessive_function="SELECT ";
    for(int i=0;i<5000;++i){excessive_not+="NOT ";excessive_add+="+1";excessive_function+="MIN(";}
    excessive_not+="TRUE";excessive_function+="1"+std::string(5000,')');
    bad(db,session,excessive_not,"QUERY_TOO_COMPLEX");
    bad(db,session,excessive_add,"QUERY_TOO_COMPLEX");
    bad(db,session,excessive_function,"QUERY_TOO_COMPLEX");
    ok(db,session,"BEGIN");bad(db,session,excessive_add,"QUERY_TOO_COMPLEX");
    bad(db,session,"SELECT 1","TRANSACTION_ABORTED");ok(db,session,"ROLLBACK");
    rows(db,session,"SELECT 1",json::array({json::array({1})}));

    // Limits apply to one expression's depth, not the width of VALUES batches.
    ok(db,session,"CREATE TABLE edge_batch(id INTEGER PRIMARY KEY)");
    std::string batch="INSERT INTO edge_batch VALUES";
    for(int i=0;i<1500;++i){if(i)batch+=",";batch+="("+std::to_string(i)+")";}
    check(ok(db,session,batch)["affected_rows"]==1500,"Wide VALUES batch remains supported");
    rows(db,session,"SELECT COUNT(*),MIN(id),MAX(id) FROM edge_batch",json::array({json::array({1500,0,1499})}));
    ok(db,session,"DROP TABLE edge_batch");
}

void query_budget_cases(const fs::path& path) {
    {
        ScopedEnvironment work("EDUDB_QUERY_WORK_LIMIT","1000");
        Engine db((path/"work").string(),3);auto s=db.open_session();
        ok(db,s,"CREATE TABLE t(id INTEGER)");
        std::string batch="INSERT INTO t VALUES";
        for(int i=0;i<100;++i){if(i)batch+=",";batch+="("+std::to_string(i)+")";}ok(db,s,batch);
        auto failure=bad(db,s,"SELECT COUNT(*) FROM t a JOIN t b ON TRUE","QUERY_LIMIT_EXCEEDED");
        check(failure["transaction_state"]=="idle","Implicit query budget error rolls back and stays usable");
        rows(db,s,"SELECT 1",json::array({json::array({1})}));
        ok(db,s,"BEGIN");bad(db,s,"SELECT COUNT(*) FROM t a JOIN t b ON TRUE","QUERY_LIMIT_EXCEEDED");
        bad(db,s,"SELECT 1","TRANSACTION_ABORTED");ok(db,s,"ROLLBACK");
        rows(db,s,"SELECT 1",json::array({json::array({1})}));
    }
    {
        ScopedEnvironment rows_limit("EDUDB_QUERY_RESULT_ROW_LIMIT","3");
        Engine db((path/"result-rows").string(),3);auto s=db.open_session();
        ok(db,s,"CREATE TABLE t(id INTEGER)");ok(db,s,"INSERT INTO t VALUES(1),(2),(3),(4),(5)");
        bad(db,s,"SELECT * FROM t","QUERY_LIMIT_EXCEEDED");
        rows(db,s,"SELECT id FROM t LIMIT 3",json::array({json::array({1}),json::array({2}),json::array({3})}));
    }
    {
        ScopedEnvironment bytes_limit("EDUDB_QUERY_RESULT_BYTE_LIMIT","32");
        Engine db((path/"result-bytes").string(),3);auto s=db.open_session();
        ok(db,s,"CREATE TABLE t(name VARCHAR(40))");ok(db,s,"INSERT INTO t VALUES('12345678901234567890'),('12345678901234567890')");
        bad(db,s,"SELECT * FROM t","QUERY_LIMIT_EXCEEDED");
        rows(db,s,"SELECT 1",json::array({json::array({1})}));
    }
    {
        ScopedEnvironment rows_limit("EDUDB_QUERY_MATERIALIZED_ROW_LIMIT","10");
        Engine db((path/"materialized-rows").string(),3);auto s=db.open_session();
        ok(db,s,"CREATE TABLE t(id INTEGER)");ok(db,s,"INSERT INTO t VALUES(1),(2),(3),(4),(5),(6)");
        bad(db,s,"SELECT COUNT(*) FROM t a JOIN t b ON TRUE","QUERY_LIMIT_EXCEEDED");
        bad(db,s,"SELECT id,COUNT(*) FROM t GROUP BY id","QUERY_LIMIT_EXCEEDED");
        rows(db,s,"SELECT id FROM t LIMIT 1",json::array({json::array({1})}));
    }
    {
        ScopedEnvironment bytes_limit("EDUDB_QUERY_MATERIALIZED_BYTE_LIMIT","512");
        Engine db((path/"materialized-bytes").string(),3);auto s=db.open_session();
        ok(db,s,"CREATE TABLE t(id INTEGER,name VARCHAR(40))");
        ok(db,s,"INSERT INTO t VALUES(1,'12345678901234567890'),(2,'12345678901234567890'),(3,'12345678901234567890'),(4,'12345678901234567890')");
        bad(db,s,"SELECT * FROM t ORDER BY id","QUERY_LIMIT_EXCEEDED");
        rows(db,s,"SELECT id FROM t LIMIT 1",json::array({json::array({1})}));
    }
    {
        ScopedEnvironment timeout("EDUDB_QUERY_TIMEOUT_MS","1");
        Engine db((path/"timeout").string(),3);auto s=db.open_session();
        ok(db,s,"CREATE TABLE t(id INTEGER)");
        std::string batch="INSERT INTO t VALUES";
        for(int i=0;i<600;++i){if(i)batch+=",";batch+="("+std::to_string(i)+")";}ok(db,s,batch);
        auto failure=bad(db,s,"SELECT COUNT(*) FROM t a JOIN t b ON TRUE","QUERY_LIMIT_EXCEEDED");
        check(failure["error"]["message"].get<std::string>().find("execution time")!=std::string::npos,"SELECT timeout is distinct from work/row limits");
    }
}

void numeric_reopen_cases(const fs::path& path) {
    {
        Engine db(path.string(),2);auto s=db.open_session();
        ok(db,s,"CREATE TABLE extremes(id INTEGER PRIMARY KEY,r REAL)");
        ok(db,s,"INSERT INTO extremes VALUES(-9223372036854775808,4.9406564584124654e-324),(9223372036854775807,1.7976931348623157e308)");
        check(db.checkpoint()["status"]=="PASS","Numeric boundaries checkpoint successfully");
    }
    {
        Engine db(path.string(),2);auto s=db.open_session();
        rows(db,s,"SELECT id,r FROM extremes ORDER BY id",json::array({
            json::array({std::numeric_limits<std::int64_t>::min(),std::numeric_limits<double>::denorm_min()}),
            json::array({std::numeric_limits<std::int64_t>::max(),std::numeric_limits<double>::max()})}));
        rows(db,s,"SELECT id FROM extremes WHERE id=-9223372036854775808",json::array({json::array({std::numeric_limits<std::int64_t>::min()})}));
    }
}

int main(){
    auto path=fs::temp_directory_path()/("edudb-sql-test-"+std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count()));
    std::string first_instance_id,first_session_id;
    if(fs::exists(path))return 2;
    try{
        {
            Engine db(path.string(),3);auto s=db.open_session();
            first_instance_id=db.instance_id();first_session_id=s;
            check(s.rfind(first_instance_id+"-s",0)==0,"Session ID is bound to Engine instance");
            rows(db,s,"SELECT 1 + 2 * 3 AS result, NOT NULL AS unknown, NULL = NULL AS unknown2",json::array({json::array({7,nullptr,nullptr})}));
            rows(db,s,"SELECT 'NULL','TRUE','WHERE','OR','*'",json::array({json::array({"NULL","TRUE","WHERE","OR","*"})}));
            rows(db,s,"SELECT -9223372036854775808, 9223372036854775807",json::array({json::array({std::numeric_limits<std::int64_t>::min(),std::numeric_limits<std::int64_t>::max()})}));
            bad(db,s,"SELECT 9223372036854775807 + 1","NUMERIC_OVERFLOW");
            bad(db,s,"SELECT -9223372036854775808 - 1","NUMERIC_OVERFLOW");
            bad(db,s,"SELECT 3037000500 * 3037000500","NUMERIC_OVERFLOW");
            bad(db,s,"SELECT 1 / 0","DIVISION_BY_ZERO");
            ok(db,s,"CREATE TABLE student (id INTEGER PRIMARY KEY, name VARCHAR(8) NOT NULL, grade REAL, passed BOOLEAN DEFAULT FALSE, tag INTEGER UNIQUE)");
            check(ok(db,s,"INSERT INTO student (id,name,grade,tag) VALUES (1,'张三',85,10),(2,'O''Neil',90.5,20),(3,'李四',NULL,NULL)")["affected_rows"]==3,"INSERT affected row count");
            rows(db,s,"SELECT id,name,passed FROM student ORDER BY id",json::array({json::array({1,"张三",false}),json::array({2,"O'Neil",false}),json::array({3,"李四",false})}));
            rows(db,s,"SELECT id FROM student WHERE grade IS NULL OR (grade >= 90 AND NOT passed) ORDER BY id",json::array({json::array({2}),json::array({3})}));
            rows(db,s,"SELECT id FROM student WHERE grade = NULL",json::array());
            rows(db,s,"SELECT COUNT(*),COUNT(grade),SUM(grade),AVG(grade),MIN(name),MAX(grade) FROM student",json::array({json::array({3,2,175.5,87.75,"O'Neil",90.5})}));
            rows(db,s,"SELECT COUNT(*),SUM(grade),AVG(grade),MIN(grade),MAX(grade) FROM student WHERE id > 100",json::array({json::array({0,nullptr,nullptr,nullptr,nullptr})}));
            ok(db,s,"CREATE TABLE aggregate_extreme(value INTEGER)");
            ok(db,s,"INSERT INTO aggregate_extreme VALUES(9223372036854775807),(9223372036854775807)");
            rows(db,s,"SELECT AVG(value) FROM aggregate_extreme",json::array({json::array({static_cast<double>(std::numeric_limits<std::int64_t>::max())})}));
            bad(db,s,"SELECT SUM(value) FROM aggregate_extreme","NUMERIC_OVERFLOW");
            ok(db,s,"DROP TABLE aggregate_extreme");
            rows(db,s,"SELECT id AS student_id FROM student ORDER BY student_id DESC LIMIT 1 OFFSET 1",json::array({json::array({2})}));
            rows(db,s,"SELECT DISTINCT passed FROM student",json::array({json::array({false})}));
            bad(db,s,"INSERT INTO student(id,name) VALUES(1,'dup')","UNIQUE_VIOLATION");
            bad(db,s,"INSERT INTO student(id,name,tag) VALUES(4,'甲',20)","UNIQUE_VIOLATION");
            bad(db,s,"INSERT INTO student(id,name) VALUES(4,NULL)","NOT_NULL_VIOLATION");
            bad(db,s,"INSERT INTO student(id,name) VALUES(4,'123456789')","VALUE_TOO_LONG");
            bad(db,s,"INSERT INTO student(id,name) VALUES(4.5,'甲')","TYPE_ERROR");
            bad(db,s,"INSERT INTO student(id,name) VALUES(4,'甲'),(1,'乙')","UNIQUE_VIOLATION");
            rows(db,s,"SELECT COUNT(*) FROM student",json::array({json::array({3})}));
            bad(db,s,"SELECT missing FROM student WHERE FALSE","COLUMN_NOT_FOUND");
            bad(db,s,"SELECT * FROM student WHERE 1","TYPE_ERROR");
            bad(db,s,"SELECT id,COUNT(*) FROM student","INVALID_AGGREGATE");
            bad(db,s,"SELECT COUNT(*) FROM student WHERE COUNT(*) > 0","INVALID_AGGREGATE");
            bad(db,s,"SELECT 1; SELECT 2","SYNTAX_ERROR");
            ok(db,s,"UPDATE student SET grade = grade + 2, passed = TRUE WHERE id = 1");
            rows(db,s,"SELECT grade,passed FROM student WHERE id=1",json::array({json::array({87.0,true})}));
            ok(db,s,"BEGIN");ok(db,s,"UPDATE student SET name='回滚' WHERE id=1");ok(db,s,"ROLLBACK");
            rows(db,s,"SELECT name FROM student WHERE id=1",json::array({json::array({"张三"})}));
            ok(db,s,"BEGIN");bad(db,s,"SELECT FROM","SYNTAX_ERROR");bad(db,s,"SELECT 1","TRANSACTION_ABORTED");bad(db,s,"SELECT 1","TRANSACTION_ABORTED");ok(db,s,"ROLLBACK");
            ok(db,s,"ALTER TABLE student ADD COLUMN age INTEGER DEFAULT 18");
            rows(db,s,"SELECT age FROM student ORDER BY id",json::array({json::array({18}),json::array({18}),json::array({18})}));
            ok(db,s,"ALTER TABLE student RENAME COLUMN age TO years");
            ok(db,s,"CREATE INDEX ix_years ON student(years)");
            bad(db,s,"ALTER TABLE student DROP COLUMN years","DEPENDENT_OBJECT");
            ok(db,s,"DROP INDEX ix_years");ok(db,s,"ALTER TABLE student DROP COLUMN years");
            bad(db,s,"ALTER TABLE student DROP COLUMN id","DEPENDENT_OBJECT");
            bad(db,s,"ALTER TABLE student ADD COLUMN required INTEGER NOT NULL","NOT_NULL_VIOLATION");
            ok(db,s,"ALTER TABLE student RENAME TO students");
            bad(db,s,"SELECT * FROM student","TABLE_NOT_FOUND");
            rows(db,s,"SELECT COUNT(*) FROM students",json::array({json::array({3})}));
            ok(db,s,"CREATE TABLE course (student_id INTEGER, title VARCHAR(10))");
            ok(db,s,"INSERT INTO course VALUES(2,'数据库'),(1,'操作系统')");
            rows(db,s,"SELECT s.name,c.title FROM students AS s INNER JOIN course c ON s.id=c.student_id WHERE s.grade > 80 ORDER BY s.id",json::array({json::array({"张三","操作系统"}),json::array({"O'Neil","数据库"})}));
            bad(db,s,"SELECT id FROM students a JOIN students b ON a.id=b.id","AMBIGUOUS_COLUMN");
            auto explanation=ok(db,s,"EXPLAIN SELECT * FROM students USE INDEX(__pk_students) WHERE id >= 2 ORDER BY id");
            check(explanation["plan"]["sources"][0]["scan"]=="IndexScan"&&explanation["plan"]["sources"][0]["index"]=="__pk_students","EXPLAIN binds the explicitly requested primary index");
            auto scan=ok(db,s,"SELECT id FROM students USE INDEX(__pk_students) WHERE id>=2 ORDER BY id");
            const auto& indexed_source=scan["plan"]["sources"][0];
            check(indexed_source["index"]=="__pk_students"&&indexed_source["scan"]!="SeqScan"&&
                  indexed_source["loops"]==1&&indexed_source["rows_examined"]==2&&indexed_source["index_page_reads"].get<std::uint64_t>()>0,"Forced primary index actually traverses its pages and returns two rows");
            auto sequential=ok(db,s,"SELECT id FROM students USE SCAN WHERE id>=2 ORDER BY id");
            check(sequential["plan"]["sources"][0]["scan"]=="SeqScan","USE SCAN executes an explicit sequential path");
            const json expected_ids=json::array({json::array({2}),json::array({3})});
            check(scan["rows"]==expected_ids&&sequential["rows"]==scan["rows"],"Forced index and sequential paths preserve exact original results");
            auto automatic=ok(db,s,"SELECT id FROM students WHERE id>=2 ORDER BY id");
            check(automatic["rows"]==expected_ids,"Automatic cost selection preserves original SQL result");
            const auto& auto_source=automatic["plan"]["sources"][0];
            check(auto_source["candidates"].size()>=2,"Automatic planning exposes sequential and qualifying index alternatives");
            for(const auto& candidate:auto_source["candidates"])
                check(auto_source["estimated_cost"].get<double>()<=candidate["estimated_cost"].get<double>()+1e-12,"Unhinted range plan costs no more than each disclosed legal candidate");
            ok(db,s,"CREATE TABLE bulk (id INTEGER PRIMARY KEY, category INTEGER, label VARCHAR(20))");
            std::string insert="INSERT INTO bulk VALUES";for(int i=149;i>=0;--i){if(i!=149)insert+=",";insert+="("+std::to_string(i)+","+std::to_string(i%5)+",'row"+std::to_string(i)+"')";}ok(db,s,insert);
            ok(db,s,"CREATE INDEX ix_category ON bulk(category)");
            rows(db,s,"SELECT COUNT(*) FROM bulk WHERE category=3",json::array({json::array({30})}));
            ok(db,s,"UPDATE bulk SET id=id+1000 WHERE id<10");
            rows(db,s,"SELECT id FROM bulk WHERE id>=1000 ORDER BY id LIMIT 2",json::array({json::array({1000}),json::array({1001})}));
            check(ok(db,s,"DELETE FROM bulk WHERE category=3")["affected_rows"]==30,"DELETE affected row count");
            rows(db,s,"SELECT COUNT(*) FROM bulk",json::array({json::array({120})}));
            ok(db,s,"CREATE TABLE alpha(id INTEGER PRIMARY KEY,value INTEGER)");ok(db,s,"CREATE TABLE beta(id INTEGER PRIMARY KEY,value INTEGER)");
            ok(db,s,"INSERT INTO alpha VALUES(1,0)");ok(db,s,"INSERT INTO beta VALUES(1,0)");
            auto a=db.open_session(),b=db.open_session();
            ok(db,a,"BEGIN");rows(db,a,"SELECT value FROM alpha",json::array({json::array({0})}));
            auto writer=std::async(std::launch::async,[&]{return db.execute(b,"UPDATE alpha SET value=1");});wait_for_waiter(db);
            check(writer.wait_for(std::chrono::milliseconds(0))!=std::future_status::ready,"Writer must wait for S lock");
            rows(db,a,"SELECT value FROM alpha",json::array({json::array({0})}));ok(db,a,"COMMIT");check(writer.get()["ok"]==true,"Writer succeeds after read commits");
            ok(db,a,"BEGIN");ok(db,b,"BEGIN");ok(db,a,"UPDATE alpha SET value=10");ok(db,b,"UPDATE beta SET value=20");
            auto first=std::async(std::launch::async,[&]{return db.execute(a,"UPDATE beta SET value=30");});wait_for_waiter(db);
            bad(db,b,"UPDATE alpha SET value=40","DEADLOCK");check(first.get()["ok"]==true,"Survivor must finish after victim releases locks");ok(db,a,"COMMIT");
            bad(db,b,"SELECT 1","TRANSACTION_ABORTED");bad(db,b,"SELECT 1","TRANSACTION_ABORTED");ok(db,b,"ROLLBACK");
            rows(db,b,"SELECT value FROM alpha",json::array({json::array({10})}));rows(db,b,"SELECT value FROM beta",json::array({json::array({30})}));
            ok(db,a,"BEGIN");ok(db,a,"UPDATE alpha SET value=99");db.close_session(a);
            rows(db,b,"SELECT value FROM alpha",json::array({json::array({10})}));
            ok(db,s,"BEGIN");ok(db,s,"CREATE TABLE transient(id INTEGER)");ok(db,s,"ROLLBACK");bad(db,s,"SELECT * FROM transient","TABLE_NOT_FOUND");
            ok(db,s,"BEGIN");ok(db,s,"DROP TABLE course");ok(db,s,"ROLLBACK");rows(db,s,"SELECT COUNT(*) FROM course",json::array({json::array({2})}));
            grouping_cases(db,s);
            planner_and_incremental_cases(db,s);
            adversarial_cases(db,s);
            db.close_session(b);db.close_session(s);check(db.checkpoint()["status"]=="PASS","Checkpoint");
        }
        {
            Engine db(path.string(),2);auto s=db.open_session();
            check(db.instance_id()!=first_instance_id&&s!=first_session_id,"Reopened Engine cannot reuse an earlier session namespace");
            bad(db,first_session_id,"SELECT 1","SESSION_NOT_FOUND");
            rows(db,s,"SELECT id,name FROM students ORDER BY id",json::array({json::array({1,"张三"}),json::array({2,"O'Neil"}),json::array({3,"李四"})}));
            rows(db,s,"SELECT COUNT(*) FROM bulk WHERE category=3",json::array({json::array({0})}));
            rows(db,s,"SELECT id FROM bulk WHERE id>=1000 ORDER BY id LIMIT 2",json::array({json::array({1000}),json::array({1001})}));
            check(db.diagnostics()["tables"].size()==5,"Diagnostics table metadata");
            ok(db,s,"DROP TABLE course");bad(db,s,"DESCRIBE course","TABLE_NOT_FOUND");db.close_session(s);
        }
        timeout_cases(path/"timeouts");
        query_budget_cases(path/"query-budgets");
        numeric_reopen_cases(path/"numeric-boundaries");
        fs::remove_all(path);std::cout<<"SQL_TEST_PASS checks="<<checks<<"\n";return 0;
    }catch(const std::exception& e){std::cerr<<"SQL_TEST_FAIL "<<e.what()<<"\nFixture kept at "<<path.string()<<"\n";return 1;}
}
