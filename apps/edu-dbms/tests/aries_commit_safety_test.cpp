#include "aries_recovery.hpp"
#include "edudb.hpp"
#include "managed_block_device.hpp"
#include <filesystem>
#include <chrono>
#include <iostream>
#include <windows.h>
using namespace edudb;namespace a=edudb::aries;namespace fs=std::filesystem;
namespace {
const std::string uuid="0123456789abcdef0123456789abcdef";
void check(bool yes,const std::string& why){if(!yes)throw std::runtime_error(why);}
template<class F>void error(const std::string& code,F callback){try{callback();}catch(const DbError& e){check(e.code==code,"expected "+code+", actual "+e.code+": "+e.what());return;}throw std::runtime_error("missing "+code);}
std::uint64_t value(const a::Page& p){std::uint64_t n=0;for(int i=7;i>=0;--i)n=(n<<8)|p[64+i];return n;}
a::Page image(std::uint64_t id,std::uint64_t v){auto c=a::default_page_codec();auto p=c.empty(id);for(unsigned n=0;n<8;++n)p[64+n]=static_cast<unsigned char>(v>>(8*n));c.stamp(p,0);return p;}
json sql(Engine& db,const std::string& session,const std::string& text){auto result=db.execute(session,text);check(result.value("ok",false),text+": "+result.dump());return result;}
json unit(const fs::path& root){json checks=json::array();
 for(const std::string scenario:{"after_force","end_append","commit_bookkeeping","force_data"}){
  auto dir=root/scenario;fs::create_directories(dir);auto data=a::open_native_data_device((dir/"data.pages").u8string());bool armed=false,forced=false,hit=false;
  {a::Options o;o.force_at_commit=scenario=="force_data";o.io_fault=[&](const char* p){const std::string point=p;if(!armed)return;if(point=="after_commit_force")forced=true;bool match=(scenario=="after_force"&&point=="after_commit_force")||(scenario=="end_append"&&forced&&point=="before_wal_write")||(scenario=="commit_bookkeeping"&&point=="after_wal_write_before_bookkeeping")||(scenario=="force_data"&&forced&&point=="before_data_write");if(match){armed=false;hit=true;throw DbError("IO_ERROR","Controlled post-COMMIT failure: "+scenario);}};
   a::Manager m(dir.u8string(),*data,uuid,a::default_page_codec(),o);m.begin(1);m.update(1,1,image(1,10));m.commit(1);m.flush_all();m.begin(2);m.update(2,1,image(1,20));armed=true;
   error("IO_ERROR",[&]{m.commit(2);});check(hit&&m.diagnostics()["recovery_required"],"post-COMMIT failure not fail-stopped");
   error("RECOVERY_REQUIRED",[&]{m.abort(2);});error("RECOVERY_REQUIRED",[&]{m.begin(3);});error("RECOVERY_REQUIRED",[&]{m.read(0,1);});
  }
  json first;{a::Manager restarted(dir.u8string(),*data,uuid);check(value(restarted.read(0,1))==20,"complete COMMIT was undone on recovery");auto records=restarted.wal_records();for(const auto& record:records["records"])check(!(record["tx"]==2&&(record["type"]=="ABORT"||record["type"]=="CLR")),"committed transaction acquired abort/CLR");first=restarted.diagnostics();}
  {a::Manager again(dir.u8string(),*data,uuid);check(value(again.read(0,1))==20,"repeat recovery lost committed page");}
  checks.push_back({{"case",scenario},{"status","PASS"},{"recovered_value",20},{"first_restart",first}});
 }
 return {{"status","PASS"},{"checks",checks},{"scope","Actual native ARIES files and controlled exceptions, no power-loss claim"}};
}
json actual_sql(const fs::path& root){json checks=json::array();for(const char* phase:{"aries_after_commit_force_once","aries_after_manager_commit_once"}){
 auto dir=root/phase;StorageOptions options;options.recovery_mode="ARIES_PAGE";
 {Engine db(dir.u8string(),2,32u*1024u*1024u,options);auto s=db.open_session();sql(db,s,"CREATE TABLE t(id INTEGER PRIMARY KEY,v VARCHAR(60))");sql(db,s,"INSERT INTO t VALUES(1,'原始行')");db.checkpoint();}
 check(_putenv_s("EDUDB_TEST_IO_FAULT",phase)==0,"set CRT fault env");json outcome,diagnostic;
 try{Engine db(dir.u8string(),2);check(_putenv_s("EDUDB_TEST_IO_FAULT","")==0,"clear CRT fault env");auto s=db.open_session();sql(db,s,"BEGIN");sql(db,s,"UPDATE t SET v='提交应保留' WHERE id=1");sql(db,s,"INSERT INTO t VALUES(2,'新提交行')");outcome=db.execute(s,"COMMIT");check(!outcome.value("ok",true)&&outcome["error"]["code"]=="IO_ERROR","commit returned success or wrong injected error: "+outcome.dump());diagnostic=db.diagnostics();check(diagnostic["storage"]["recovery_required"],"Storage/Manager combined diagnostics hid post-commit failure");auto other=db.open_session();auto denied=db.execute(other,"INSERT INTO t VALUES(3,'故障后拒写')");check(!denied.value("ok",true)&&denied["error"]["code"]=="RECOVERY_REQUIRED","new writer not fail-stopped: "+denied.dump());}
 catch(...){_putenv_s("EDUDB_TEST_IO_FAULT","");throw;}
 json after;{Engine recovered(dir.u8string(),2);auto s=recovered.open_session();after=sql(recovered,s,"SELECT id,v FROM t ORDER BY id");check(after["rows"]==json::array({json::array({1,"提交应保留"}),json::array({2,"新提交行"})}),"real SQL restart discarded durable commit: "+after.dump());auto verified=sql(recovered,s,"VERIFY");check(verified["verification"]["ok"],"real SQL verify failed");check(recovered.diagnostics()["storage"]["clr_records"]==0,"durable SQL commit was later compensated");}
 {Engine recovered(dir.u8string(),2);auto s=recovered.open_session();check(sql(recovered,s,"SELECT id,v FROM t ORDER BY id")["rows"]==after["rows"],"SQL repeated restart changed result");}
 checks.push_back({{"case",phase},{"status","PASS"},{"commit_response",outcome},{"failure_diagnostics",diagnostic},{"recovered_rows",after["rows"]}});
 }return {{"status","PASS"},{"checks",checks},{"scope","Actual integrated Engine SQL and native Windows files; no power-loss claim"}};}
}
int main(int argc,char** argv){try{if(argc==1){auto root=fs::absolute(fs::path("commit-safety-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));std::cout<<json{{"unit",unit(root/"unit")},{"sql",actual_sql(root/"sql")}}.dump(2)<<"\n";return 0;}check(argc==3,"usage --unit|--sql NEWROOT");fs::path root=fs::u8path(argv[2]);fs::create_directories(root);auto mode=std::string(argv[1]);check(mode=="--unit"||mode=="--sql","bad mode");std::cout<<(mode=="--unit"?unit(root):actual_sql(root)).dump(2)<<"\n";return 0;}catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 5;}}
