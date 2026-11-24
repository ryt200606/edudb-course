#include "test_platform.hpp"
#include "edudb.hpp"
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
using namespace edudb;
namespace fs=std::filesystem;
static std::atomic<unsigned> checks{0};
static json events=json::array();static std::mutex events_mutex;
static void need(bool b,const std::string& m){++checks;if(!b)throw std::runtime_error(m);}
static json run(Engine& e,const std::string& s,const std::string& q,bool ok=true){auto r=e.execute(s,q);{std::lock_guard<std::mutex> g(events_mutex);events.push_back({{"session",s},{"sql",q},{"response",r}});}if(ok)need(r.value("ok",false),q+" => "+r.value("error",json()).dump());return r;}
static void val(Engine& e,const std::string& s,long long v,const std::string& table="t"){auto r=run(e,s,"SELECT v FROM "+table+" WHERE id=1");need(r.at("rows")==json::array({json::array({v})}),"actual SQL value "+std::to_string(v));}
static std::uint64_t begin(Engine& e,const std::string& s,bool ro=false){return run(e,s,"BEGIN PROTOCOL MV2PL READ "+std::string(ro?"ONLY":"WRITE")).at("tx_id").get<std::uint64_t>();}
static void error(Engine& e,const std::string& s,const std::string& q,const std::string& code){auto r=run(e,s,q,false);need(!r.value("ok",true)&&r.at("error").at("code")==code,"exact error "+code+" for "+q);}
static void locked(const json& d,std::uint64_t tx,const std::string& resource,const std::string& mode){bool found=false;for(auto& l:d.at("locks"))if(l.at("tx_id")==tx&&l.at("resource")==resource&&l.at("mode")==mode)found=true;need(found,"real held "+resource+" "+mode);}
static void ro_unlocked(const json& d,std::uint64_t tx){for(auto& l:d.at("locks"))if(l.at("tx_id")==tx){auto r=l.at("resource").get<std::string>();need(r.rfind("table:",0)!=0&&r.rfind("cluster:",0)!=0,"RO must not hold user-table/cluster lock");}locked(d,tx,"catalog","S");}
static json wait_queue(Engine& e,std::uint64_t tx,const std::string& resource){auto until=std::chrono::steady_clock::now()+std::chrono::seconds(3);while(std::chrono::steady_clock::now()<until){auto d=e.diagnostics();for(auto& q:d.at("queues"))if(q.at("tx_id")==tx&&q.at("resource")==resource){std::lock_guard<std::mutex> g(events_mutex);events.push_back({{"actual_wait",d}});return d;}std::this_thread::sleep_for(std::chrono::milliseconds(5));}throw std::runtime_error("missing actual lock queue "+resource);}
static void finish_future(std::future<json>& f){need(f.wait_for(std::chrono::seconds(3))==std::future_status::ready,"SQL request completed after lock release");auto r=f.get();need(r.value("ok",false),"async SQL success");}
static void write(const fs::path& p,const json& v){std::ofstream f(p,std::ios::binary|std::ios::trunc);f<<v.dump(2);f.flush();if(!f)throw std::runtime_error("receipt write");}
static void exercise(const fs::path& dir,json& evidence){
 Engine e(dir.u8string(),8);auto setup=e.open_session();run(e,setup,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)");run(e,setup,"INSERT INTO t VALUES(1,10),(2,20)");e.close_session(setup);
 auto ro=e.open_session(),w=e.open_session(),rnew=e.open_session(),w2=e.open_session(),other=e.open_session();
 auto roid=begin(e,ro,true);val(e,ro,10);auto d=e.diagnostics();ro_unlocked(d,roid);need(d.at("storage").at("transactions").at("protocol_epoch")=="MV2PL","distinct protocol epoch");
 auto not_yet_read=e.open_session();begin(e,not_yet_read,true);
 auto wid=begin(e,w);run(e,w,"UPDATE t SET v=11 WHERE id=1");locked(e.diagnostics(),wid,"table:t","X");val(e,ro,10);run(e,w,"COMMIT");val(e,ro,10);val(e,not_yet_read,10);run(e,not_yet_read,"COMMIT");e.close_session(not_yet_read);auto newer=begin(e,rnew,true);val(e,rnew,11);ro_unlocked(e.diagnostics(),newer);
 auto h=e.diagnostics().at("storage").at("transactions");need(h.at("history_bytes").get<unsigned>()>=8192,"actual committed page history retained for old RO");evidence["version_history_active"]=h;
 error(e,other,"BEGIN PROTOCOL SNAPSHOT READ ONLY","PROTOCOL_MODE_BUSY");
 // Real updater S lock blocks a second updater X, while version readers stay free.
 wid=begin(e,w);auto read=run(e,w,"SELECT v FROM t WHERE id=1");need(read.at("locking").at("effective")=="STRICT_TABLE_2PL","updater plan reports real2PL");locked(e.diagnostics(),wid,"table:t","S");
 auto w2id=begin(e,w2);auto pending=std::async(std::launch::async,[&]{return run(e,w2,"UPDATE t SET v=v+1 WHERE id=1",false);});wait_queue(e,w2id,"table:t");need(pending.wait_for(std::chrono::milliseconds(0))!=std::future_status::ready,"blocked behind actual held S");val(e,ro,10);val(e,rnew,11);run(e,w,"COMMIT");finish_future(pending);
 locked(e.diagnostics(),w2id,"table:t","X");wid=begin(e,w);auto pending2=std::async(std::launch::async,[&]{return run(e,w,"UPDATE t SET v=v+1 WHERE id=1",false);});wait_queue(e,wid,"table:t");run(e,w2,"COMMIT");finish_future(pending2);run(e,w,"COMMIT");
 auto latest=e.open_session();begin(e,latest,true);val(e,latest,13);run(e,latest,"COMMIT");e.close_session(latest);val(e,ro,10);val(e,rnew,11);
 // Savepoint rolls back actual private pages, retaining strict locks until END.
 wid=begin(e,w);run(e,w,"SAVEPOINT keep");run(e,w,"UPDATE t SET v=900 WHERE id=1");val(e,w,900);run(e,w,"ROLLBACK TO keep");val(e,w,13);locked(e.diagnostics(),wid,"table:t","X");run(e,w,"RELEASE SAVEPOINT keep");run(e,w,"COMMIT");
 begin(e,w);run(e,w,"UPDATE t SET v=901 WHERE id=1");run(e,w,"ROLLBACK");begin(e,w,true);val(e,w,13);run(e,w,"COMMIT");
 error(e,ro,"UPDATE t SET v=100 WHERE id=1","READ_ONLY_TRANSACTION");run(e,ro,"ROLLBACK");run(e,rnew,"COMMIT");
 // An active updater is not a snapshot-retention consumer.
 begin(e,w);evidence["after_old_readers_finish"]=e.diagnostics().at("storage").at("transactions");need(evidence["after_old_readers_finish"].at("history_bytes")==0,"history GC despite active MV2PL updater");run(e,w,"ROLLBACK");
 error(e,other,"BEGIN PROTOCOL MV2PL ISOLATION LEVEL READ COMMITTED","INVALID_TRANSACTION_OPTIONS");
 run(e,other,"SET LOCK GRANULARITY RECORD");error(e,other,"BEGIN PROTOCOL MV2PL","INVALID_LOCK_GRANULARITY");run(e,other,"SET LOCK GRANULARITY TABLE");
 // A RO reader's catalog S must block true schema modification.
 roid=begin(e,ro,true);val(e,ro,13);wid=begin(e,w);auto ddl=std::async(std::launch::async,[&]{return run(e,w,"ALTER TABLE t ADD COLUMN note VARCHAR(24)",false);});wait_queue(e,wid,"catalog");run(e,ro,"COMMIT");finish_future(ddl);run(e,w,"COMMIT");
 for(auto& s:std::vector<std::string>{ro,w,rnew,w2,other})e.close_session(s);
 // Same protocol across two actual members of one shared physical cluster.
 auto s=e.open_session();run(e,s,"CREATE CLUSTER grouped");run(e,s,"CREATE TABLE ca(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER) CLUSTER grouped(k)");run(e,s,"CREATE TABLE cb(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER) CLUSTER grouped(k)");run(e,s,"INSERT INTO ca VALUES(1,7,40)");run(e,s,"INSERT INTO cb VALUES(1,7,50)");e.close_session(s);
 auto a=e.open_session(),b=e.open_session(),c=e.open_session();auto rid=begin(e,c,true);val(e,c,40,"ca");auto aid=begin(e,a);run(e,a,"UPDATE ca SET v=41 WHERE id=1");auto bid=begin(e,b);auto clustered=std::async(std::launch::async,[&]{return run(e,b,"UPDATE cb SET v=51 WHERE id=1",false);});
 auto until=std::chrono::steady_clock::now()+std::chrono::seconds(3);std::string resource;while(std::chrono::steady_clock::now()<until&&resource.empty()){auto st=e.diagnostics();for(auto& q:st.at("queues"))if(q.at("tx_id")==bid&&q.at("resource").get<std::string>().rfind("cluster:",0)==0)resource=q.at("resource");if(resource.empty())std::this_thread::sleep_for(std::chrono::milliseconds(5));}need(!resource.empty(),"actual shared cluster X lock queue");locked(e.diagnostics(),aid,resource,"X");ro_unlocked(e.diagnostics(),rid);val(e,c,40,"ca");val(e,c,50,"cb");run(e,a,"COMMIT");finish_future(clustered);run(e,b,"COMMIT");val(e,c,40,"ca");val(e,c,50,"cb");run(e,c,"COMMIT");
 begin(e,c,true);val(e,c,41,"ca");val(e,c,51,"cb");auto verify=run(e,c,"VERIFY");need(verify.at("verification").at("ok")==true,"SQL VERIFY real cluster/index storage");run(e,c,"COMMIT");for(auto& x:std::vector<std::string>{a,b,c})e.close_session(x);
 evidence["final_diagnostics"]=e.diagnostics();
}
static void resources(const fs::path& dir,json& evidence){
 edudb_test::set_env("EDUDB_HISTORY_LIMIT_BYTES","8192");
 {Engine e(dir.u8string(),8);auto setup=e.open_session();run(e,setup,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)");run(e,setup,"INSERT INTO t VALUES(1,10)");e.close_session(setup);auto ro=e.open_session(),w=e.open_session();begin(e,ro,true);val(e,ro,10);begin(e,w);run(e,w,"UPDATE t SET v=99 WHERE id=1");auto before=e.diagnostics().at("storage").at("wal_bytes");error(e,w,"COMMIT","VERSION_HISTORY_LIMIT");auto after=e.diagnostics();need(after.at("storage").at("wal_bytes")==before,"history budget rejected before WAL append/force");val(e,ro,10);run(e,w,"ROLLBACK");run(e,ro,"COMMIT");begin(e,w);run(e,w,"UPDATE t SET v=11 WHERE id=1");run(e,w,"COMMIT");begin(e,w,true);val(e,w,11);run(e,w,"COMMIT");evidence["history_limit_rejection"]=after;e.close_session(ro);e.close_session(w);}
 edudb_test::set_env("EDUDB_HISTORY_LIMIT_BYTES","");
}
int main(int argc,char** argv){fs::path parent=argc>1?fs::u8path(argv[1]):fs::temp_directory_path()/fs::path("edudb-mv2pl-"+std::to_string(edudb_test::process_id()));std::string mode=argc>2?argv[2]:"normal";fs::path receipt=argc>3?fs::u8path(argv[3]):parent/"result.json";json evidence={{"mode",mode},{"scope","isolated full source build from frozen5ce; no current-main acceptance"}};try{
 fs::create_directories(parent);
 if(mode=="normal"){
  need(!fs::exists(parent/"core"),"fresh owned test directory");exercise(parent/"core",evidence);resources(parent/"resource",evidence);
  {StorageOptions o;o.recovery_mode="ARIES_PAGE";Engine e((parent/"aries-reject").u8string(),8,32u*1024u*1024u,o);auto s=e.open_session();error(e,s,"BEGIN PROTOCOL MV2PL READ ONLY","ARIES_PROTOCOL");e.close_session(s);}
  {Engine e((parent/"core").u8string(),8);auto s=e.open_session();begin(e,s,true);val(e,s,13);val(e,s,41,"ca");val(e,s,51,"cb");run(e,s,"VERIFY");run(e,s,"COMMIT");e.close_session(s);}
 }else if(mode=="hold-commit"||mode=="hold-loser"){
  Engine e((parent/"core").u8string(),8);auto ro=e.open_session(),w=e.open_session();begin(e,ro,true);val(e,ro,13);begin(e,w);run(e,w,"UPDATE t SET v=77 WHERE id=1");if(mode=="hold-commit")run(e,w,"COMMIT");val(e,ro,13);evidence["pid"]=edudb_test::process_id();evidence["status"]="READY_FOR_EXTERNAL_TERMINATE";evidence["events"]=events;write(receipt,evidence);for(unsigned i=0;i<120;++i)std::this_thread::sleep_for(std::chrono::seconds(1));throw std::runtime_error("missing external kill");
 }else if(mode=="recover-commit"||mode=="recover-loser"){
  Engine e((parent/"core").u8string(),8);auto s=e.open_session();evidence["startup"]=e.diagnostics();begin(e,s,true);val(e,s,mode=="recover-commit"?77:13);val(e,s,41,"ca");val(e,s,51,"cb");run(e,s,"VERIFY");run(e,s,"COMMIT");e.close_session(s);
 }else throw std::runtime_error("unknown mode");
 evidence["status"]="PASS";evidence["checks"]=checks.load();evidence["events"]=events;write(receipt,evidence);std::cout<<"PASS MV2PL checks="<<checks<<"\n";return 0;
 }catch(const std::exception& ex){evidence["status"]="FAIL";evidence["error"]=ex.what();evidence["checks"]=checks.load();evidence["events"]=events;try{write(receipt,evidence);}catch(...){}std::cerr<<"FAIL "<<ex.what()<<"\n";return 1;}}
