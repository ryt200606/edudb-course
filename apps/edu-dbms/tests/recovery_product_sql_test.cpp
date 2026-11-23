#include "edudb.hpp"
#include "recovery_archive.hpp"
#include "fencing.hpp"
#include <chrono>
#include <filesystem>
#include <future>
#include <fstream>
#include <iostream>
using namespace edudb;
using namespace std::chrono_literals;
namespace fs=std::filesystem;
namespace {
int checks=0;
void check(bool b,const std::string& why){++checks;if(!b)throw std::runtime_error(why);}
json sql(Engine& e,const std::string& s,const std::string& q){auto r=e.execute(s,q);check(r.value("ok",false),q+": "+r.dump());return r;}
void error(Engine& e,const std::string& s,const std::string& q,const std::string& code){auto r=e.execute(s,q);check(!r.value("ok",true)&&r["error"]["code"]==code,q+": "+r.dump());}
void rows(Engine& e,const std::string& s,const std::string& expected){auto r=sql(e,s,"SELECT v FROM t WHERE id=1");check(r["rows"]==json::array({json::array({expected})}),r.dump());}
void recovery_sql(const fs::path& root){
 StorageOptions options;options.fencing_directory=(root/"fence").u8string();Engine primary((root/"primary").u8string(),2,32u*1024u*1024u,options);auto a=primary.open_session();
 sql(primary,a,"CREATE TABLE t(id INTEGER PRIMARY KEY,v VARCHAR(80))");sql(primary,a,"INSERT INTO t VALUES(1,'原始中文'),(2,NULL)");
 check(sql(primary,a,"SHOW RECOVERY")["recovery"]["role"]=="PRIMARY","SQL recovery identity");sql(primary,a,"SET ARCHIVING ON");
 auto base=sql(primary,a,"BACKUP DATABASE AS 'class_demo'")["recovery"];check(base.contains("directory"),"SQL backup missing real path");
 sql(primary,a,"UPDATE t SET v='目标提交' WHERE id=1");auto first=sql(primary,a,"ARCHIVE WAL")["recovery"];
 sql(primary,a,"UPDATE t SET v='后续提交' WHERE id=1");auto second=sql(primary,a,"ARCHIVE WAL")["recovery"];
 recovery::restore_backup(base.at("directory"),{first.at("manifest_path"),second.at("manifest_path")},first.at("end_lsn"),(root/"standby").u8string());
 Engine standby((root/"standby").u8string(),2,32u*1024u*1024u,options);auto b=standby.open_session();rows(standby,b,"目标提交");error(standby,b,"UPDATE t SET v='不得写' WHERE id=999","RECOVERY_READ_ONLY");
 sql(standby,b,"BECOME STANDBY");check(sql(standby,b,"SHOW RECOVERY")["recovery"]["role"]=="STANDBY","SQL standby transition");
 error(standby,b,"DELETE FROM t WHERE id=999","STANDBY_READ_ONLY");
 sql(standby,b,"BEGIN PROTOCOL SNAPSHOT READ ONLY");rows(standby,b,"目标提交");bool busy=false;try{standby.replay_archive(second.at("manifest_path"),0);}catch(const DbError& e){busy=e.code=="BUSY";}check(busy,"replay did not respect active snapshot barrier");sql(standby,b,"ROLLBACK");
 auto replay=standby.replay_archive(second.at("manifest_path"));check(replay.at("replayed_lsn")==second.at("end_lsn"),"real replay target mismatch");
 sql(standby,b,"BEGIN PROTOCOL SNAPSHOT READ ONLY");rows(standby,b,"后续提交");sql(standby,b,"COMMIT");
 auto graph=standby.diagnostics()["storage"]["transactions"]["page_version_graph"];check(graph["epoch_resets"]==1&&!graph["disabled"]&&graph["invalid_events"]==0,"replay left stale or invalid graph observations");
 auto duplicate=standby.replay_archive(second.at("manifest_path"));check(duplicate.at("appended_wal_bytes")==0&&duplicate.at("data_pages_written")==0,"duplicate replay changed bytes");
 check(standby.diagnostics()["storage"]["transactions"]["page_version_graph"]["epoch_resets"]==1,"duplicate reset observation epoch");
 sql(primary,a,"BEGIN");sql(primary,a,"UPDATE t SET v='失去写权的私有内容' WHERE id=1");
 sql(standby,b,"PROMOTE STANDBY");check(sql(standby,b,"SHOW RECOVERY")["recovery"]["role"]=="PRIMARY","promotion did not persist primary role");
 error(primary,a,"COMMIT","FENCED");sql(primary,a,"ROLLBACK");error(primary,a,"UPDATE t SET v='零行也不能确认写' WHERE id=999","FENCED");rows(standby,b,"后续提交");
 sql(standby,b,"UPDATE t SET v='新主提交' WHERE id=1");rows(standby,b,"新主提交");check(sql(standby,b,"VERIFY")["verification"]["ok"],"promoted storage verification");
 sql(standby,b,"BEGIN");error(standby,b,"BACKUP DATABASE AS bad","TRANSACTION_ACTIVE");sql(standby,b,"ROLLBACK");
}
void checkpoint_admission(const fs::path& root,const std::string& mode){
 StorageOptions options;options.recovery_mode=mode;options.fencing_directory=(root/"fence").u8string();Engine e((root/"db").u8string(),2,32u*1024u*1024u,options);auto s=e.open_session();sql(e,s,"CREATE TABLE t(id INTEGER PRIMARY KEY)");
 auto id=e.recovery_identity();recovery::FenceDomain domain(id.at("uuid"),options.fencing_directory);std::optional<recovery::FencePermit> held;held.emplace(domain.acquire_writer({id.at("instance_id"),id.at("timeline"),id.at("fence_epoch")}));
 auto waiting=std::async(std::launch::async,[&]{try{e.checkpoint();return std::string("UNEXPECTED_SUCCESS");}catch(const DbError& error){return error.code;}});
 check(waiting.wait_for(8s)==std::future_status::ready,"bounded checkpoint fencing wait");check(waiting.get()=="FENCE_BUSY","checkpoint did not report known pre-WAL admission failure");held.reset();
 check(!e.diagnostics()["storage"]["recovery_required"],"known pre-WAL FENCE_BUSY poisoned storage");e.checkpoint();sql(e,s,"INSERT INTO t VALUES(1)");check(sql(e,s,"VERIFY")["verification"]["ok"],"checkpoint admission recovery");
 if(mode=="ARIES_PAGE")error(e,s,"BACKUP DATABASE AS unsupported","BACKUP_MODE");
 else{auto conflict=root/"db/archives";{std::ofstream file(conflict,std::ios::binary);file<<"owned test conflict";}sql(e,s,"SET ARCHIVING ON");error(e,s,"CHECKPOINT","BACKUP_PATH");check(!e.diagnostics()["storage"]["recovery_required"],"known archive admission error poisoned storage before local mutation");check(fs::remove(conflict),"owned conflict cleanup");sql(e,s,"SET ARCHIVING OFF");e.checkpoint();}

}
}
int main(){try{auto root=fs::absolute(fs::path("recovery-product-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));recovery_sql(root/"sql");checkpoint_admission(root/"redo","REDO_PRIVATE");checkpoint_admission(root/"aries","ARIES_PAGE");std::cout<<"PASS recovery SQL/fencing/format4 composition "<<checks<<" checks\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
