#include "edudb.hpp"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
using namespace edudb;
namespace fs=std::filesystem;
namespace {
void check(bool x,const std::string& m){if(!x)throw std::runtime_error(m);}
template<class F>void error(const std::string& code,F f){try{f();}catch(const DbError& e){check(e.code==code,"expected "+code+", got "+e.code+": "+e.what());return;}throw std::runtime_error("missing error "+code);}
TransactionOptions options(TransactionProtocol p,bool rc=false,bool ro=false){TransactionOptions o;o.protocol=p;o.read_committed=rc;o.read_only=ro;return o;}
void access(Storage& db,const std::shared_ptr<Transaction>& tx,const std::string& table,bool read,bool write,bool blind=false){db.refresh_read_view(tx,{{table,read,write,blind}});}
json definition(int value=10){return {{"columns",json::array({{{"name","id"},{"type","INTEGER"},{"not_null",true}},{{"name","v"},{"type","INTEGER"}},{{"name","padding"},{"type","VARCHAR"},{"length",8000}}})},{"indexes",json::array({{{"name","ix_id"},{"column","id"},{"columns",json::array({"id"})},{"method","BPLUS"},{"unique",true},{"options",{{"max_keys",3}}}}})},{"rows",json::array({json::array({1,value,"row-one"}),json::array({2,value,"row-two"})})}};}
std::vector<StoredRow> rows(Storage& db,const std::shared_ptr<Transaction>& tx,const std::string& table){auto c=db.scan(tx,table);std::vector<StoredRow> out;StoredRow row;while(c->next(row))out.push_back(row);return out;}
std::int64_t value(Storage& db,const std::shared_ptr<Transaction>& tx,const std::string& table,int id=1){for(auto& r:rows(db,tx,table))if(r.values[0]==id)return r.values[1].get<std::int64_t>();throw std::runtime_error("missing row");}
void update(Storage& db,const std::shared_ptr<Transaction>& tx,const std::string& table,int id,int v){for(auto r:rows(db,tx,table))if(r.values[0]==id){r.values[1]=v;db.update_row(tx,table,r.id,r.values);return;}throw std::runtime_error("missing update target");}
void seed(Storage& db){auto tx=db.begin();for(auto name:{"t","doctor_a","doctor_b"}){access(db,tx,name,false,true);db.write_table(tx,name,definition(10));}db.commit(tx);}
void storage_cases(const fs::path& directory){
 Storage db(directory.u8string(),4);seed(db);
 {auto a=db.begin(options(TransactionProtocol::Snapshot));auto b=db.begin(options(TransactionProtocol::Snapshot));access(db,a,"t",true,false);check(value(db,a,"t")==10,"snapshot baseline");access(db,b,"t",true,true);update(db,b,"t",1,11);db.commit(b);access(db,a,"t",true,false);check(value(db,a,"t")==10,"fixed snapshot must read committed old heap/index/descriptor versions");db.commit(a);}
 std::cout<<"PASS real persistent fixed page snapshot survives another commit\n";
 {auto a=db.begin(options(TransactionProtocol::Snapshot,true));auto b=db.begin(options(TransactionProtocol::Snapshot,true));access(db,a,"t",true,false);check(value(db,a,"t")==11,"RC initial");access(db,b,"t",true,true);update(db,b,"t",1,12);db.commit(b);access(db,a,"t",true,false);check(value(db,a,"t")==12,"RC must refresh committed descriptor and heap view");db.commit(a);}
 std::cout<<"PASS per-statement READ COMMITTED view advances\n";
 {auto a=db.begin(options(TransactionProtocol::Snapshot));auto b=db.begin(options(TransactionProtocol::Snapshot));access(db,a,"t",true,true);access(db,b,"t",true,true);update(db,a,"t",1,13);update(db,b,"t",2,99);db.commit(a);error("SERIALIZATION_FAILURE",[&]{db.commit(b);});db.rollback(b);auto c=db.begin();access(db,c,"t",true,false);check(value(db,c,"t",1)==13&&value(db,c,"t",2)==10,"page-conflict loser must not overwrite committed rows");db.commit(c);}
 std::cout<<"PASS same-page disjoint logical writes reject lost-update publication\n";
 for(auto p:{TransactionProtocol::Snapshot,TransactionProtocol::Occ}) {
  auto reset=db.begin();for(auto name:{"doctor_a","doctor_b"}){access(db,reset,name,true,true);update(db,reset,name,1,1);}db.commit(reset);
  auto a=db.begin(options(p));auto b=db.begin(options(p));for(auto t:{a,b})for(auto name:{"doctor_a","doctor_b"}){access(db,t,name,true,false);check(value(db,t,name)==1,"write-skew initial invariant");}
  access(db,a,"doctor_a",true,true);update(db,a,"doctor_a",1,0);access(db,b,"doctor_b",true,true);update(db,b,"doctor_b",1,0);db.commit(a);
  if(p==TransactionProtocol::Snapshot)db.commit(b);else{error("SERIALIZATION_FAILURE",[&]{db.commit(b);});db.rollback(b);}
  auto c=db.begin();access(db,c,"doctor_a",true,false);access(db,c,"doctor_b",true,false);auto sum=value(db,c,"doctor_a")+value(db,c,"doctor_b");check(sum==(p==TransactionProtocol::Snapshot?0:1),"SI write skew versus OCC read validation");db.commit(c);
 }
 std::cout<<"PASS real cross-table SI write skew and OCC prevention\n";
 {auto a=db.begin(options(TransactionProtocol::Timestamp));auto b=db.begin(options(TransactionProtocol::Timestamp));access(db,b,"t",false,true,true);db.write_table(b,"t",definition(30));db.commit(b);error("TIMESTAMP_CONFLICT",[&]{access(db,a,"t",true,false);});db.rollback(a);}
 {auto a=db.begin(options(TransactionProtocol::Timestamp));auto b=db.begin(options(TransactionProtocol::Timestamp));access(db,b,"t",true,false);check(value(db,b,"t")==30,"timestamp read");error("TIMESTAMP_CONFLICT",[&]{access(db,a,"t",false,true,true);});db.rollback(a);db.commit(b);}
 {auto a=db.begin(options(TransactionProtocol::Thomas));auto b=db.begin(options(TransactionProtocol::Thomas));access(db,b,"t",false,true,true);db.write_table(b,"t",definition(40));db.commit(b);auto v=db.refresh_read_view(a,{{"t",false,true,true}});check(v.ignore_write,"Thomas obsolete blind write must be explicitly ignored");db.commit(a);auto c=db.begin();access(db,c,"t",true,false);check(value(db,c,"t")==40,"ignored blind write changed persisted data");db.commit(c);}
 std::cout<<"PASS timestamp read/write ordering and Thomas obsolete blind write\n";
 {auto a=db.begin(options(TransactionProtocol::Snapshot));error("PROTOCOL_MODE_BUSY",[&]{db.begin(options(TransactionProtocol::Occ));});db.rollback(a);auto b=db.begin(options(TransactionProtocol::Occ));db.commit(b);}
 {auto a=db.begin(options(TransactionProtocol::Snapshot,false,true));error("READ_ONLY_TRANSACTION",[&]{access(db,a,"t",true,true);});error("READ_ONLY_TRANSACTION",[&]{db.insert_row(a,"t",json::array({99,1,"x"}));});db.rollback(a);}
 std::cout<<"PASS protocol epoch and real read-only rejection\n";
 {auto a=db.begin();access(db,a,"t",true,true);const auto before=db.read_table(a,"t");db.savepoint(a,"first");update(db,a,"t",1,77);for(int i=10;i<30;++i)db.insert_row(a,"t",json::array({i,i,std::string(180,'x')}));db.savepoint(a,"second");db.drop_table(a,"doctor_a");db.rollback_to(a,"first");check(db.read_table(a,"t")["rows"]==before["rows"],"rollback_to must restore heap/index and descriptor contents");check(db.catalog(a).contains("doctor_a"),"rollback_to must restore catalog DDL");error("SAVEPOINT_NOT_FOUND",[&]{db.release_savepoint(a,"second");});db.release_savepoint(a,"first");check(db.verify(a)["ok"].get<bool>(),"savepoint graph verification");db.commit(a);}
 std::cout<<"PASS savepoint restores split pages/new allocation/catalog/descriptor state\n";
 db.checkpoint();
}
json sql(Engine& db,const std::string& session,const std::string& text,bool okay=true){auto r=db.execute(session,text);check(r.value("ok",false)==okay,text+" -> "+r.dump());return r;}
void sql_number(Engine& db,const std::string& session,const std::string& statement,int expected,const std::string& context){auto actual=sql(db,session,statement)["rows"];check(actual==json::array({json::array({expected})}),context+"; actual typed rows="+actual.dump()+", expected="+std::to_string(expected));std::cout<<"PASS "<<context<<" observed typed rows="<<actual.dump()<<"\n";}
void wait_for_lock(Engine& db){auto until=std::chrono::steady_clock::now()+std::chrono::seconds(3);while(std::chrono::steady_clock::now()<until){if(!db.diagnostics().value("waits",json::array()).empty())return;std::this_thread::sleep_for(std::chrono::milliseconds(10));}throw std::runtime_error("expected real lock wait not observed");}
void sql_cases(const fs::path& directory){
 Engine db(directory.u8string(),4);auto a=db.open_session(),b=db.open_session();sql(db,a,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)");sql(db,a,"INSERT INTO t VALUES(1,10),(2,20)");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT");sql(db,a,"SELECT v FROM t WHERE id=1");sql(db,b,"BEGIN PROTOCOL SNAPSHOT");sql(db,b,"UPDATE t SET v=11 WHERE id=1");sql(db,b,"COMMIT");check(db.diagnostics().is_object(),"diagnostics must not throw default-mode busy while a snapshot remains active");sql_number(db,a,"SELECT v FROM t WHERE id=1",10,"actual SQL session snapshot");sql(db,a,"COMMIT");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT ISOLATION LEVEL READ COMMITTED");sql_number(db,a,"SELECT v FROM t WHERE id=1",11,"SQL RC initial");sql(db,b,"BEGIN PROTOCOL SNAPSHOT");sql(db,b,"UPDATE t SET v=12 WHERE id=1");sql(db,b,"COMMIT");sql_number(db,a,"SELECT v FROM t WHERE id=1",12,"actual SQL RC refresh");sql(db,a,"COMMIT");
 sql(db,a,"BEGIN");sql(db,a,"SAVEPOINT s");sql(db,a,"UPDATE t SET v=99 WHERE id=1");sql(db,a,"INSERT INTO t VALUES(1,0)",false);sql(db,a,"ROLLBACK TO SAVEPOINT s");sql_number(db,a,"SELECT v FROM t WHERE id=1",12,"SQL failed transaction savepoint recovery");sql(db,a,"RELEASE SAVEPOINT s");sql(db,a,"COMMIT");
 sql(db,a,"BEGIN READ ONLY");sql(db,a,"UPDATE t SET v=0",false);sql(db,a,"ROLLBACK");
 sql(db,a,"SET TRANSACTION READ ONLY");auto rejected=db.import_data(a,{{"format","csv"},{"table","t"},{"content","id,v\n50,50\n"},{"header",true}});check(!rejected.value("ok",true)&&rejected["error"]["code"]=="READ_ONLY_TRANSACTION","CSV import must consume/enforce NEXT READ ONLY");sql_number(db,a,"SELECT COUNT(*) FROM t WHERE id=50",0,"read-only CSV import left a row");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT");sql(db,a,"COMMIT");sql(db,b,"BEGIN");sql(db,b,"SELECT * FROM t");
 auto imported=std::async(std::launch::async,[&]{return db.import_data(a,{{"format","csv"},{"table","t"},{"content","id,v\n50,50\n"},{"header",true}});});wait_for_lock(db);check(imported.wait_for(std::chrono::milliseconds(0))!=std::future_status::ready,"default import bypassed a held strict shared table lock");sql(db,b,"COMMIT");check(imported.get().value("ok",false),"default CSV import after Snapshot must use a real Strict transaction");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT");sql(db,a,"COMMIT");sql(db,b,"BEGIN");sql(db,b,"UPDATE t SET v=77 WHERE id=1");
 auto exported=std::async(std::launch::async,[&]{return db.export_data(a,{{"format","csv"},{"table","t"},{"header",true}});});wait_for_lock(db);check(exported.wait_for(std::chrono::milliseconds(0))!=std::future_status::ready,"default export bypassed strict exclusive table lock");sql(db,b,"ROLLBACK");auto output=exported.get();check(output.value("ok",false)&&output["content"].get<std::string>().find("77")==std::string::npos,"export observed uncommitted update or old mode");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT ISOLATION LEVEL READ COMMITTED");sql(db,a,"SELECT v FROM t WHERE id=1");sql(db,a,"SAVEPOINT rc");sql(db,b,"BEGIN PROTOCOL SNAPSHOT");sql(db,b,"UPDATE t SET v=13 WHERE id=1");sql(db,b,"COMMIT");sql(db,a,"SELECT v FROM t WHERE id=1");sql(db,a,"ROLLBACK TO rc");sql_number(db,a,"SELECT v FROM t WHERE id=1",13,"RC rollback_to must not revive an old/garbage-collected read view");sql(db,a,"COMMIT");
 db.close_session(a);db.close_session(b);std::cout<<"PASS actual SQL sessions SNAPSHOT/RC/savepoint/READ ONLY and import/export mode-lock lifecycle\n";
}
void history_budget(const fs::path& directory){
 const char* raw=std::getenv("EDUDB_HISTORY_LIMIT_BYTES");const std::string prior=raw?raw:"";
 auto set=[](const std::string& value){
#ifdef _WIN32
 _putenv_s("EDUDB_HISTORY_LIMIT_BYTES",value.c_str());
#else
 if(value.empty())unsetenv("EDUDB_HISTORY_LIMIT_BYTES");else setenv("EDUDB_HISTORY_LIMIT_BYTES",value.c_str(),1);
#endif
 };
 set("8192");
 try{
 {Storage db(directory.u8string(),4);seed(db);auto a=db.begin(options(TransactionProtocol::Snapshot));access(db,a,"t",true,false);check(value(db,a,"t")==10,"budget baseline");auto b=db.begin(options(TransactionProtocol::Snapshot));access(db,b,"t",true,true);db.write_table(b,"t",definition(55));const auto wal=db.diagnostics()["wal_bytes"];error("VERSION_HISTORY_LIMIT",[&]{db.commit(b);});check(db.diagnostics()["wal_bytes"]==wal,"history budget failure appended WAL");db.rollback(b);db.commit(a);auto c=db.begin(options(TransactionProtocol::Snapshot));access(db,c,"t",true,true);db.write_table(c,"t",definition(55));db.commit(c);auto read=db.begin();access(db,read,"t",true,false);check(value(db,read,"t")==55,"self-only snapshot should not reserve obsolete history");db.commit(read);}
 }catch(...){set(prior);throw;}set(prior);
 std::cout<<"PASS history budget rejects before WAL and excludes committing reader itself\n";
}
}
int main(){try{const auto stamp=std::chrono::steady_clock::now().time_since_epoch().count();auto root=fs::temp_directory_path()/("edudb-transaction-protocols-"+std::to_string(stamp));check(!fs::exists(root),"fresh test root");fs::create_directory(root);storage_cases(root/"storage");sql_cases(root/"sql");history_budget(root/"budget");auto canonical=fs::canonical(root),parent=fs::canonical(fs::temp_directory_path());check(fs::equivalent(canonical.parent_path(),parent)&&canonical.filename().u8string().rfind("edudb-transaction-protocols-",0)==0,"owned cleanup guard");fs::remove_all(canonical);std::cout<<"PASS transaction_protocols completed\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL transaction_protocols: "<<e.what()<<"\n";return 1;}}
