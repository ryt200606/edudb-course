#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
using namespace edudb;
namespace fs=std::filesystem;
namespace {
void check(bool yes,const std::string& why){if(!yes)throw std::runtime_error(why);}
json sql(Engine& db,const std::string& session,const std::string& text){auto result=db.execute(session,text);check(result.value("ok",false),text+": "+result.dump());return result;}
StorageOptions options(){StorageOptions o;o.recovery_mode="ARIES_PAGE";return o;}
void marker(const fs::path& dir,const json& payload){std::ofstream out(dir/"ready.json",std::ios::binary);out<<payload.dump();out.flush();check(bool(out),"marker write");out.close();std::cout<<"READY\n"<<std::flush;std::this_thread::sleep_for(std::chrono::seconds(20));throw std::runtime_error("expected external parent termination");}
std::int64_t count(Engine& db,const std::string& s){return sql(db,s,"SELECT COUNT(*) FROM t")["rows"][0][0].get<std::int64_t>();}
json snapshot(const fs::path& dir){Engine db(dir.u8string(),2);auto s=db.open_session();auto rows=sql(db,s,"SELECT id,k,v FROM t ORDER BY id");auto verified=sql(db,s,"VERIFY");return {{"status","PASS"},{"rows",rows["rows"]},{"verify",verified},{"diagnostics",db.diagnostics()}};}
void seed(const fs::path& dir){Engine db(dir.u8string(),2,32u*1024u*1024u,options());auto s=db.open_session();sql(db,s,"CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v VARCHAR(200))");sql(db,s,"INSERT INTO t VALUES(1,10,'原始记录'),(2,20,'第二行')");sql(db,s,"CREATE INDEX ix ON t(k) WITH(max_keys=5)");db.checkpoint();}
json units(const fs::path& dir){fs::create_directories(dir);json results=json::array();auto path=dir/"actual-sql";{
 Engine db(path.u8string(),2,32u*1024u*1024u,options());auto s=db.open_session();
 sql(db,s,"CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v VARCHAR(200))");
 sql(db,s,"BEGIN");std::string statement="INSERT INTO t VALUES";for(int i=1;i<=72;++i){if(i!=1)statement+=",";statement+="("+std::to_string(i)+","+std::to_string(i%7)+",'"+std::string(90,'p')+"')";}auto inserted=sql(db,s,statement);
 auto uncommitted=db.diagnostics()["storage"];check(uncommitted["steal_flushes"].get<std::uint64_t>()>0,"actual SQL pages never stolen before COMMIT");check(uncommitted["last_lsn"].get<std::uint64_t>()>10,"SQL did not write UPDATE WAL before COMMIT");check(!uncommitted["legacy_buffer_constructed"].get<bool>(),"double authority buffers constructed");check(inserted["metrics"]["wal_bytes"].get<std::uint64_t>()>0,"SQL per-statement WAL metric not real");
 sql(db,s,"SAVEPOINT kept");sql(db,s,"UPDATE t SET k=99,v='保存点后修改' WHERE id<=12");sql(db,s,"DELETE FROM t WHERE id>60");check(count(db,s)==60,"SQL changed count before savepoint rollback");sql(db,s,"ROLLBACK TO kept");check(count(db,s)==72,"SQL savepoint CLR did not restore rows");sql(db,s,"COMMIT");
 sql(db,s,"CREATE INDEX ix ON t(k) WITH(max_keys=5)");sql(db,s,"BEGIN");sql(db,s,"SAVEPOINT before_ddl");sql(db,s,"ALTER TABLE t ADD extra INTEGER DEFAULT 7");sql(db,s,"ROLLBACK TO before_ddl");sql(db,s,"COMMIT");check(sql(db,s,"SELECT * FROM t LIMIT 1")["rows"][0].size()==3,"DDL savepoint left stale descriptor");
 sql(db,s,"BEGIN");sql(db,s,"UPDATE t SET k=123 WHERE id=1");sql(db,s,"ROLLBACK");check(sql(db,s,"SELECT k FROM t WHERE id=1")["rows"][0][0]==1,"full SQL rollback");
 auto wal=sql(db,s,"SHOW WAL");check(wal["diagnostic"]["mode"]=="ARIES_PAGE","SHOW WAL lost ARIES status");
 auto records=sql(db,s,"SHOW WAL RECORDS LIMIT 65536")["diagnostic"];bool clr=false;for(const auto& record:records["records"])if(record["type"]=="CLR"){clr=true;check(record["undo_next_lsn"].get<std::uint64_t>()<record["lsn"].get<std::uint64_t>(),"SQL CLR inspection lost UndoNextLSN");}check(clr,"real SQL WAL inspection has no savepoint/full rollback CLR");
 auto excessive=db.execute(s,"SHOW WAL RECORDS LIMIT 65537");check(!excessive.value("ok",true)&&excessive["error"]["code"]=="ARGUMENT","unbounded WAL inspection accepted");
 auto invalid=db.execute(s,"BEGIN PROTOCOL SNAPSHOT");check(!invalid.value("ok",true)&&invalid["error"]["code"]=="ARIES_PROTOCOL","unsupported protocol was only labeled rather than rejected");
 auto wrong=db.execute(s,"SET BUFFER POLICY MRU");check(!wrong.value("ok",true),"ARIES silently accepted unavailable legacy buffer policy");
 auto buffer=sql(db,s,"SHOW BUFFER")["diagnostic"];check(buffer["authority"]=="ARIES manager"&&buffer["buffer_policy"]=="LRU","SHOW BUFFER did not use ARIES authority diagnostics");
 auto checkpoint=db.checkpoint();check(checkpoint["mode"]=="ARIES_FUZZY","SQL Engine checkpoint still used stop-world redo generation switch");
 auto verify=sql(db,s,"VERIFY");results.push_back({{"case","actual_SQL_DML_DDL_index_and_savepoint_CLR"},{"status","PASS"},{"before_commit",uncommitted},{"verification",verify}});
 }{
 Engine reopened(path.u8string(),2);auto s=reopened.open_session();check(count(reopened,s)==72,"SQL reopen lost committed rows");check(sql(reopened,s,"SELECT k FROM t WHERE id=1")["rows"][0][0]==1,"SQL restart kept rolled back value");check(reopened.diagnostics()["storage"]["recovery_mode"]=="ARIES_PAGE","persistent mode not followed");results.push_back({{"case","actual_Engine_restart_follows_mode_and_checks_committed_rows"},{"status","PASS"}});
 }
 {auto invalid=options();invalid.recovery_mode="REDO_PRIVATE";try{Storage bad(path.u8string(),2,32u*1024u*1024u,invalid);throw std::runtime_error("mode switch accepted");}catch(const DbError& e){check(e.code=="RECOVERY_MODE","wrong mode switch error");}}
 {Engine baseline((dir/"default-mode").u8string(),2);auto s=baseline.open_session();sql(baseline,s,"CREATE TABLE normal(id INTEGER PRIMARY KEY)");sql(baseline,s,"INSERT INTO normal VALUES(1)");check(baseline.diagnostics()["storage"].contains("wal_generation"),"default redo-private path was replaced");results.push_back({{"case","default_redo_private_unchanged_and_cross_mode_rejected"},{"status","PASS"}});}
 {auto policy=options();policy.aries_steal=false;policy.aries_force_at_commit=true;
  const auto at=(dir/"persistent-policy").u8string();
  {Engine db(at,16,32u*1024u*1024u,policy);auto s=db.open_session();sql(db,s,"CREATE TABLE p(id INTEGER PRIMARY KEY)");sql(db,s,"INSERT INTO p VALUES(7)");}
  {Engine db(at,16);auto d=db.diagnostics()["storage"];check(!d["steal"].get<bool>()&&d["force_at_commit"].get<bool>(),"reopen lost persisted steal/force policy");auto s=db.open_session();check(sql(db,s,"SELECT id FROM p")["rows"][0][0]==7,"force/no-steal lost data");}
  auto changed=policy;changed.aries_steal=true;bool rejected=false;try{Storage db(at,16,32u*1024u*1024u,changed);}catch(const DbError& e){rejected=e.code=="RECOVERY_MODE";}check(rejected,"explicit conflicting persisted policy accepted");
  results.push_back({{"case","ARIES_policies_persist_and_explicit_changes_rejected"},{"status","PASS"}});
 }
 {const auto at=(dir/"aborted-commit").u8string();Storage db(at,2,32u*1024u*1024u,options());auto tx=db.begin();
  json table={{"columns",json::array({json{{"name","id"},{"type","INTEGER"},{"length",0},{"not_null",false}},json{{"name","v"},{"type","VARCHAR"},{"length",7800},{"not_null",false}}})},{"rows",json::array()},{"indexes",json::array()}};
  db.write_table(tx,"t",table);auto rid=db.insert_row(tx,"t",json::array({1,std::string(7000,'m')}));db.commit(tx);
  tx=db.begin();db.enable_changefeed(tx,{"t"});bool limited=false;for(int n=0;n<5000;++n){try{db.update_row(tx,"t",rid,json::array({1,std::string(7000,static_cast<char>('a'+n%20))}));}catch(const DbError& e){limited=e.code=="CHANGEFEED_LIMIT";break;}}
  check(limited,"ARIES feed overflow did not occur");bool rejected=false;std::string rejection;try{db.commit(tx);}catch(const DbError& e){rejection=e.code+": "+e.what();rejected=e.code=="TRANSACTION_ABORTED";}check(rejected&&!db.recovery_required(),"aborted ARIES commit incorrectly succeeded or fail-stopped: "+rejection+" diagnostics="+db.diagnostics().dump());
  tx=db.begin();check(db.read_table(tx,"t")["rows"][0][1]==std::string(7000,'m'),"ARIES rejected commit left uncommitted row/page owner");db.update_row(tx,"t",rid,json::array({1,"new transaction"}));db.commit(tx);
  check(db.diagnostics()["transaction_table"].empty(),"aborted ARIES transaction leaked manager entry");
  results.push_back({{"case","ARIES_aborted_commit_compensates_and_releases_page_owners"},{"status","PASS"}});
 }
 return {{"status","PASS"},{"checks",results},{"scope","Integrated native Windows Engine/Storage SQL; process crash scenarios use separate external-kill runner"}};
}
}
int main(int argc,char** argv){try{if(argc==1){auto root=fs::absolute(fs::path("aries_sql_test-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));if(fs::exists(root))throw std::runtime_error("test directory already exists");fs::create_directories(root);std::cout<<units(root).dump(2)<<"\n";return 0;}if(argc<3)throw std::runtime_error("usage --unit|--seed|--snapshot DIR or --worker committed|loser DIR");std::string command=argv[1];if(command=="--unit"){std::cout<<units(fs::u8path(argv[2])).dump(2)<<"\n";return 0;}if(command=="--seed"){seed(fs::u8path(argv[2]));std::cout<<"{\"status\":\"PASS_SEED\"}\n";return 0;}if(command=="--snapshot"){std::cout<<snapshot(fs::u8path(argv[2])).dump(2)<<"\n";return 0;}if(command=="--worker"&&argc==4){auto dir=fs::u8path(argv[3]);Engine db(dir.u8string(),2);auto s=db.open_session();sql(db,s,"BEGIN");sql(db,s,"UPDATE t SET k=111,v='崩溃前未提交' WHERE id=1");sql(db,s,"INSERT INTO t VALUES(3,30,'新增行')");if(std::string(argv[2])=="committed")sql(db,s,"COMMIT");auto d=db.diagnostics();marker(dir,{{"scenario",argv[2]},{"diagnostics",d}});return 2;}throw std::runtime_error("invalid command");}catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 5;}}
