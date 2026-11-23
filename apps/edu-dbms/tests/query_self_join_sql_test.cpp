#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#ifdef _WIN32
#include <windows.h>
#endif
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& text){++checks;if(!value)throw std::runtime_error(text);}
struct Environment {std::string key,previous;bool present=false;Environment(std::string name,const char* value):key(std::move(name)){const char* old=std::getenv(key.c_str());present=old!=nullptr;if(old)previous=old;
#ifdef _WIN32
    _putenv_s(key.c_str(),value);
#else
    setenv(key.c_str(),value,1);
#endif
}~Environment(){
#ifdef _WIN32
    _putenv_s(key.c_str(),present?previous.c_str():"");
#else
    if(present)setenv(key.c_str(),previous.c_str(),1);else unsetenv(key.c_str());
#endif
}};
static json ok(edudb::Engine& db,const std::string& sid,const std::string& sql){auto r=db.execute(sid,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static json twin(edudb::Engine& db,const std::string& sid,const std::string& sql,bool chosen){json before;{Environment disabled("EDUDB_OPTIMIZER_SELF_JOIN","OFF");before=ok(db,sid,sql);}auto after=ok(db,sid,sql);check(after["rows"]==before["rows"]&&after["columns"]==before["columns"],"Same-row elimination differs from original join: "+sql);check(after["plan"]["self_join_elimination"]["chosen"]==chosen,"Wrong elimination choice: "+after.dump());return after;}
int main(){
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
#endif
    auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB主键自连接_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{Environment enabled("EDUDB_OPTIMIZER_SELF_JOIN","ON");edudb::Engine db(directory.u8string(),8);auto sid=db.open_session();ok(db,sid,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER,label VARCHAR(64),b BLOB(64))");std::string sql="INSERT INTO t VALUES";for(int n=0;n<1000;++n){if(n)sql+=',';sql+='('+std::to_string(n)+','+(n%9==0?"NULL":std::to_string(n%7))+",'中文"+std::to_string(n%5)+"',X'00FF')";}ok(db,sid,sql);ok(db,sid,"INSERT INTO t VALUES(-9223372036854775808,NULL,'最小',NULL),(9223372036854775807,NULL,'最大',X'AA')");ok(db,sid,"ANALYZE t");
    auto all=twin(db,sid,"SELECT a.id,b.label FROM t a JOIN t b ON a.id=b.id ORDER BY a.id",true);check(all["rows"].size()==1002&&all["plan"]["physical_actual"]["node"]=="SelfJoinAliasCopy","Actual physical node reconstructs aliases after one source");auto source=all["plan"]["self_join_elimination"]["selected_source"].get<int>();check(all["plan"]["sources"][source]["loops"]==1&&all["plan"]["sources"][1-source]["loops"]==0,"Only one real base cursor opens: "+all["plan"].dump());check(all["plan"]["self_join_elimination"]["single_scan_estimated_cost"].get<double>()<all["plan"]["self_join_elimination"]["original_estimated_cost"].get<double>(),"Rule wins a real normalized cost comparison");check(all["plan"]["actual"]["join_pairs"]==0,"No hidden nested pair enumeration remains");
    auto point=twin(db,sid,"SELECT a.label,b.id FROM t a JOIN t b ON a.id=b.id WHERE b.id=777 ORDER BY b.id",true);check(point["rows"]==json::array({json::array({"中文2",777})})&&point["plan"]["self_join_elimination"]["selected_source"]==1,"Second alias selective access fetches first-alias required fields correctly");check(point["plan"]["sources"][0]["loops"]==0&&point["plan"]["sources"][1]["loops"]==1,"Selective alternate source really replaces the first scan");
    for(const auto& q:{"SELECT a.v,COUNT(*),SUM(b.id),AVG(b.v) FROM t a JOIN t b ON a.id=b.id GROUP BY a.v ORDER BY a.v","SELECT DISTINCT a.label,b.v FROM t a JOIN t b ON b.id=a.id ORDER BY a.label,b.v LIMIT 8 OFFSET 2","SELECT a.id,b.b FROM t a JOIN t b ON a.id=b.id AND a.v=b.v WHERE a.id<10 ORDER BY a.id","SELECT a.id FROM t a JOIN t b ON a.id=b.id AND a.v IS NULL WHERE b.id<20 ORDER BY a.id","SELECT b.* FROM t a JOIN t b ON a.id=b.id WHERE a.id=3 ORDER BY b.id","SELECT COUNT(*) FROM t a JOIN t b ON a.id=b.id","SELECT a.id FROM t a JOIN t b ON a.id=b.id ORDER BY a.id LIMIT 0"})twin(db,sid,q,true);
    twin(db,sid,"SELECT a.id FROM t a JOIN t b ON a.id=b.id WHERE a.v+1>0 ORDER BY a.id",false);
    ok(db,sid,"CREATE TABLE small(id INTEGER PRIMARY KEY,ref INTEGER)");ok(db,sid,"INSERT INTO small VALUES(1,2),(2,1),(3,NULL)");
    for(const auto& q:{"SELECT a.id,b.id FROM small a JOIN small b ON a.id=b.ref ORDER BY a.id,b.id","SELECT a.id,b.id FROM small a JOIN small b ON a.id=b.id OR a.id=b.ref ORDER BY a.id,b.id","SELECT a.id,b.id FROM small a LEFT JOIN small b ON a.id=b.id ORDER BY a.id,b.id","SELECT a.id,b.id FROM small a USE SCAN JOIN small b ON a.id=b.id ORDER BY a.id","SELECT a.id,b.id FROM small a USE INDEX(__pk_small) JOIN small b ON a.id=b.id WHERE a.id>=1 ORDER BY a.id","SELECT a.id FROM small a JOIN small b ON a.id=b.id JOIN small c ON b.id=c.id ORDER BY a.id"})twin(db,sid,q,false);
    for(const auto& method:{"NESTED_LOOP","BLOCK_NESTED_LOOP","INDEX_NESTED_LOOP","MERGE","HASH"}){auto q=std::string("SELECT a.id FROM small a JOIN small b ON a.id=b.id USE JOIN(")+method+") ORDER BY a.id";auto result=twin(db,sid,q,false);check(result["plan"]["join_algorithm"]==method,"Explicit method remains the actual original algorithm");}
    ok(db,sid,"CREATE TABLE nullable_u(id INTEGER UNIQUE,v INTEGER)");ok(db,sid,"INSERT INTO nullable_u VALUES(1,1),(NULL,2),(NULL,3)");check(twin(db,sid,"SELECT a.v,b.v FROM nullable_u a JOIN nullable_u b ON a.id=b.id ORDER BY a.v,b.v",false)["rows"].size()==1,"Nullable UNIQUE does not justify removing null rows or multiplication");
    ok(db,sid,"CREATE TABLE composite(id INTEGER,x INTEGER,PRIMARY KEY(id,x))");ok(db,sid,"INSERT INTO composite VALUES(1,1),(1,2),(2,1)");check(twin(db,sid,"SELECT a.x,b.x FROM composite a JOIN composite b ON a.id=b.id ORDER BY a.id,a.x,b.x",false)["rows"].size()==5,"Composite prefix equality can multiply rows and is never eliminated");
    ok(db,sid,"CREATE TABLE table_pk(id INTEGER,v INTEGER,PRIMARY KEY(id))");ok(db,sid,"INSERT INTO table_pk VALUES(1,2),(2,3),(3,4)");twin(db,sid,"SELECT a.id,b.v FROM table_pk a JOIN table_pk b ON a.id=b.id ORDER BY a.id",true);
    auto null_primary=db.execute(sid,"INSERT INTO table_pk VALUES(NULL,9)");check(!null_primary.value("ok",true),"The table-level PRIMARY KEY genuinely rejects NULL, rather than merely carrying an optimizer label");check(ok(db,sid,"SELECT COUNT(*) FROM table_pk")["rows"][0][0]==3,"Rejected NULL key changes no rows");
    json original_error;{Environment disabled("EDUDB_OPTIMIZER_SELF_JOIN","OFF");original_error=db.execute(sid,"SELECT 1/(a.id-a.id) FROM small a JOIN small b ON a.id=b.id");}auto actual_error=db.execute(sid,"SELECT 1/(a.id-a.id) FROM small a JOIN small b ON a.id=b.id");check(!original_error.value("ok",true)&&!actual_error.value("ok",true)&&original_error["error"]["code"]==actual_error["error"]["code"]&&actual_error["error"]["code"]=="DIVISION_BY_ZERO","Projection errors retain the same failure and resource cleanup after the proved physical substitution");check(ok(db,sid,"SELECT 42")["rows"][0][0]==42,"Session survives alias-copy projection failure");
    twin(db,sid,"SELECT 1/(a.id-a.id) FROM small a JOIN small b ON a.id=b.id LIMIT 0",true);
    auto explained=ok(db,sid,"EXPLAIN SELECT a.id,b.label FROM t a JOIN t b ON a.id=b.id");check(explained["plan"]["self_join_elimination"]["chosen"]==true&&!explained["plan"].contains("physical_actual"),"Plain EXPLAIN reports the proved choice without executing it");
    ok(db,sid,"CREATE MATERIALIZED VIEW same_rows WITH(refresh='immediate') AS SELECT a.id,b.ref FROM small a JOIN small b ON a.id=b.id AND a.ref<>b.id");ok(db,sid,"INSERT INTO small VALUES(4,4)");check(ok(db,sid,"SELECT COUNT(*) FROM same_rows")["rows"][0][0]==2,"Physical optimization leaves original two-source FULL MV definition and maintenance intact");
    ok(db,sid,"BEGIN");ok(db,sid,"INSERT INTO small VALUES(5,5)");twin(db,sid,"SELECT a.ref,b.id FROM small a JOIN small b ON a.id=b.id ORDER BY a.id",true);ok(db,sid,"ROLLBACK");check(ok(db,sid,"SELECT COUNT(*) FROM small")["rows"][0][0]==4,"Same transaction private rows/rollback retain original SQL semantics");check(ok(db,sid,"VERIFY")["verification"]["ok"]==true,"Kernel invariants remain valid");db.close_session(sid);std::cout<<"PASS "<<checks<<" actual costed one-scan self-join/constraints/hints/MV/transaction checks\n";
    }catch(const std::exception&e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" data="<<directory.u8string()<<"\n";return 1;}
}
