#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <thread>
using namespace edudb;
using namespace std::chrono_literals;
namespace fs=std::filesystem;
namespace {
int checks=0;
void check(bool b,const std::string& why){++checks;if(!b)throw std::runtime_error(why);}
json sql(Engine& e,const std::string& s,const std::string& q){auto r=e.execute(s,q);check(r.value("ok",false),q+": "+r.dump());return r;}
void rows(Engine& e,const std::string& s,const std::string& q,const json& expect){auto r=sql(e,s,q);check(r["rows"]==expect,q+": "+r.dump());}
json finish(std::future<json>& f){check(f.wait_for(3s)==std::future_status::ready,"waiter did not finish within three seconds");auto r=f.get();check(r.value("ok",false),r.dump());return r;}
void queued(Engine& e,const std::string& resource){const auto end=std::chrono::steady_clock::now()+2s;while(std::chrono::steady_clock::now()<end){auto d=e.diagnostics();for(const auto& q:d["queues"])if(q["resource"]==resource||(resource=="cluster:"&&q["resource"].get<std::string>().rfind(resource,0)==0))return;std::this_thread::sleep_for(2ms);}throw std::runtime_error("missing actual lock queue: "+resource);}
void idle_wound(const fs::path& dir,const std::string& mode){
 StorageOptions o;o.recovery_mode=mode;Engine e(dir.u8string(),2,32u*1024u*1024u,o);auto seed=e.open_session(),old=e.open_session(),young=e.open_session();
 sql(e,seed,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)");sql(e,seed,"INSERT INTO t VALUES(1,10)");sql(e,seed,"SET LOCK POLICY WOUND_WAIT");
 sql(e,old,"BEGIN");sql(e,young,"BEGIN");sql(e,young,"UPDATE t SET v=20 WHERE id=1");
 auto waiter=std::async(std::launch::async,[&]{return e.execute(old,"UPDATE t SET v=v+1 WHERE id=1");});
 // The wounded session makes no further request. Its cancellation must make
 // the reaper undo first and release afterwards; the survivor must read10.
 finish(waiter);sql(e,old,"COMMIT");rows(e,seed,"SELECT v FROM t",json::array({json::array({11})}));
 auto denied=e.execute(young,"SELECT * FROM t");check(!denied.value("ok",true)&&denied["error"]["code"]=="TRANSACTION_ABORTED","idle wounded transaction did not require rollback acknowledgement: "+denied.dump());
 sql(e,young,"ROLLBACK");rows(e,young,"SELECT v FROM t",json::array({json::array({11})}));check(sql(e,seed,"VERIFY")["verification"]["ok"],"wound rollback verification");
 auto d=e.diagnostics();check(!d["storage"]["recovery_required"],"ordinary wound poisoned storage");check(d["queues"].empty(),"wound left a stale request");check(!d["storage"]["transactions"]["page_version_graph"]["disabled"],"wound invalidated observation graph");
 std::cout<<json{{"case","idle_wounded_owner_undo_before_release"},{"mode",mode},{"rows",11},{"events",d["events"]}}.dump()<<'\n';
}
void range_composition(const fs::path& dir){
 Engine e(dir.u8string(),3);auto seed=e.open_session(),a=e.open_session(),b=e.open_session();
 sql(e,seed,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)");sql(e,seed,"INSERT INTO t VALUES(1,10),(2,20)");sql(e,seed,"CREATE MATERIALIZED VIEW vsp WITH(refresh='immediate',maintenance='incremental') AS SELECT id,v FROM t");
 sql(e,a,"SET LOCK GRANULARITY RECORD");sql(e,b,"SET LOCK GRANULARITY RECORD");
 sql(e,a,"BEGIN");auto initial=sql(e,a,"SELECT v FROM t WHERE id=1");check(initial["locking"]["effective"]=="RECORD","eligible base read lost record access");
 auto write=std::async(std::launch::async,[&]{return e.execute(b,"UPDATE t SET v=21 WHERE id=2");});queued(e,"table:t");check(write.wait_for(0ms)!=std::future_status::ready,"immediate view maintenance ignored coarse source lock");sql(e,a,"COMMIT");auto changed=finish(write);check(changed["locking"]["effective"]=="TABLE","immediate maintenance used unsafe range-only access");rows(e,seed,"SELECT v FROM vsp WHERE id=2",json::array({json::array({21})}));
 auto viewread=sql(e,a,"SELECT v FROM vsp WHERE id=1");check(viewread["locking"]["effective"]=="TABLE","view access did not use coarse guard");
 sql(e,seed,"CREATE CLUSTER shared");sql(e,seed,"CREATE TABLE c1(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER) CLUSTER shared(k)");sql(e,seed,"CREATE TABLE c2(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER) CLUSTER shared(k)");sql(e,seed,"INSERT INTO c1 VALUES(1,7,30)");sql(e,seed,"INSERT INTO c2 VALUES(1,7,40)");
 sql(e,a,"BEGIN");auto clusterread=sql(e,a,"SELECT v FROM c1 WHERE id=1");check(clusterread["locking"]["effective"]=="TABLE","shared physical page read needs cluster/table fallback");
 auto clusterwrite=std::async(std::launch::async,[&]{return e.execute(b,"UPDATE c2 SET v=41 WHERE id=1");});queued(e,"cluster:");sql(e,a,"COMMIT");auto clusterchanged=finish(clusterwrite);check(clusterchanged["locking"]["effective"]=="TABLE","shared cluster writer lost coarse fallback");
 rows(e,seed,"SELECT v FROM c1",json::array({json::array({30})}));rows(e,seed,"SELECT v FROM c2",json::array({json::array({41})}));check(sql(e,seed,"VERIFY")["verification"]["ok"],"combined range/MV/cluster verify");
}
}
int main(){try{const auto root=fs::absolute(fs::path("concurrency-composition-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));idle_wound(root/"redo","REDO_PRIVATE");idle_wound(root/"aries","ARIES_PAGE");range_composition(root/"range");std::cout<<"PASS real SQL concurrency composition "<<checks<<" checks\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
