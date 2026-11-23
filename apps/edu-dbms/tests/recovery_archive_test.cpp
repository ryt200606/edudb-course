#include <chrono>
#include "recovery_archive.hpp"
#include "fencing.hpp"
#include "format_copy.hpp"
#include "edudb.hpp"
#include "../src/media_native.hpp"
#include <fstream>
#include <iostream>
#include <thread>
using namespace edudb;
namespace r=edudb::recovery;namespace fs=std::filesystem;
namespace {
using namespace edudb::media_detail;
void check(bool yes,const std::string& why){if(!yes)throw std::runtime_error(why);}
template<class F>void error(const std::string& expected,F callback){try{callback();}catch(const DbError& e){check(e.code==expected,"expected "+expected+", got "+e.code+": "+e.what());return;}throw std::runtime_error("missing expected "+expected);}
json read_json(const fs::path& path){std::ifstream f(path,std::ios::binary);json j;f>>j;check(bool(f),"test JSON read");return j;}
std::vector<unsigned char> read_bytes(const fs::path& path){std::ifstream f(path,std::ios::binary);return {std::istreambuf_iterator<char>(f),std::istreambuf_iterator<char>()};}
void write_bytes(const fs::path& p,const std::vector<unsigned char>& bytes){File f(p,true,true);f.write(0,bytes.data(),bytes.size());f.sync();}
json sql(Engine& db,const std::string& s,const std::string& text){auto j=db.execute(s,text);check(j.value("ok",false),text+": "+j.dump());return j;}
fs::path marker(const fs::path& dir,const json& archived){return dir/("control-archive-"+std::to_string(archived["generation"].get<std::uint64_t>())+"-"+std::to_string(archived["end_lsn"].get<std::uint64_t>())+"-"+archived["timeline"].get<std::string>()+".json");}
json archive(const fs::path& db,const fs::path& dir){auto c=read_json(db/"control.json");auto input=r::open_read_only((db/("wal-"+std::to_string(c["wal_generation"].get<std::uint64_t>())+".log")).u8string());return r::archive_wal(*input,c["uuid"],c.value("timeline",c["uuid"].get<std::string>()),dir.u8string());}
json state(const fs::path& db){Engine engine(db.u8string(),2);auto session=engine.open_session();auto result=sql(engine,session,"SELECT id,v FROM t ORDER BY id");auto verified=sql(engine,session,"VERIFY");check(verified["verification"]["ok"],"restored core index verification");return result["rows"];}
struct Fixture {fs::path primary,backup,archives;json base,first,second,third;};
Fixture fixture(const fs::path& dir){Fixture f{dir/"primary",dir/"backup",dir/"archives",{},{},{},{}};
 {Engine db(f.primary.u8string(),2);auto s=db.open_session();sql(db,s,"CREATE TABLE t(id INTEGER PRIMARY KEY,v VARCHAR(80))");sql(db,s,"INSERT INTO t VALUES(1,'原始一'),(2,'原始二')");db.checkpoint();}
 auto control=read_json(f.primary/"control.json");auto input=r::open_read_only((f.primary/"data.pages").u8string());f.base=r::create_backup(*input,control,control["checkpoint_lsn"],f.backup.u8string());input.reset();
 {Engine db(f.primary.u8string(),2);auto s=db.open_session();sql(db,s,"INSERT INTO t VALUES(3,'第一次提交')");}f.first=archive(f.primary,f.archives);
 {Engine db(f.primary.u8string(),2);auto s=db.open_session();sql(db,s,"UPDATE t SET v='第二次提交' WHERE id=1");}f.second=archive(f.primary,f.archives);
 {Engine db(f.primary.u8string(),2);db.checkpoint();auto s=db.open_session();sql(db,s,"INSERT INTO t VALUES(4,'下一代提交')");}f.third=archive(f.primary,f.archives);
 return f;}
json unit(const fs::path& dir){fs::create_directories(dir);auto f=fixture(dir);json checks=json::array();
 auto first_marker=marker(f.archives,f.first),second_marker=marker(f.archives,f.second),third_marker=marker(f.archives,f.third);
 auto target1=f.first["end_lsn"].get<std::uint64_t>(),target2=f.second["end_lsn"].get<std::uint64_t>();
 auto inspect=r::inspect_backup(f.backup.u8string());check(inspect["files"]["data.pages"]["sha256"].get<std::string>().size()==64,"SHA256 missing");
 for(const auto& name:{"control.json","data.pages","wal-1.log","backup-complete.json"}){
#ifdef _WIN32
   check(SetFileAttributesW((f.backup/name).c_str(),FILE_ATTRIBUTE_READONLY)!=0,"set readonly fixture attribute");
#endif
 }
 check(r::inspect_backup(f.backup.u8string())["state"]=="COMPLETE","read-only OS inputs failed");checks.push_back({{"case","physical_baseline_true_readonly_inputs_and_native_SHA256"},{"status","PASS"}});
 std::vector<std::string> archives{second_marker.u8string(),first_marker.u8string(),third_marker.u8string()};
 auto target1dir=dir/"target-first";auto receipt=r::restore_backup(f.backup.u8string(),archives,target1,target1dir.u8string());check(receipt["target_commit_lsn"]==target1,"target receipt");check(state(target1dir)==json::array({json::array({1,"原始一"}),json::array({2,"原始二"}),json::array({3,"第一次提交"})}),"PITR first target state includes future data");
 auto target2dir=dir/"target-second";r::restore_backup(f.backup.u8string(),archives,target2,target2dir.u8string());check(state(target2dir)[0][1]=="第二次提交"&&state(target2dir).size()==3,"second target state");
 auto latest=dir/"target-latest";r::restore_backup(f.backup.u8string(),archives,0,latest.u8string());check(state(latest).size()==4&&state(latest)[0][1]=="第二次提交","cross-generation latest recovery");
 auto a=read_json(target1dir/"control.json"),b=read_json(target2dir/"control.json"),c=read_json(f.backup/"control.json");check(a["instance_id"]!=b["instance_id"]&&a["instance_id"]!=c["instance_id"]&&a["recovery_role"]=="RESTORED_READ_ONLY","restore copied writable owner identity");checks.push_back({{"case","actual_SQL_two_exact_COMMIT_targets_and_next_generation_latest"},{"status","PASS"},{"first_target_lsn",target1},{"second_target_lsn",target2}});
 error("RECOVERY_TARGET",[&]{r::restore_backup(f.backup.u8string(),archives,f.base["end_lsn"].get<std::uint64_t>()-1,(dir/"too-old").u8string());});
 error("RECOVERY_TARGET",[&]{r::restore_backup(f.backup.u8string(),archives,target1-1,(dir/"non-commit").u8string());});
 error("ARCHIVE_GAP",[&]{r::restore_backup(f.backup.u8string(),{third_marker.u8string()},0,(dir/"missing-generation").u8string());});
 error("BACKUP_PATH",[&]{r::restore_backup(f.backup.u8string(),{},f.base["end_lsn"],(f.backup/"child").u8string());});
 error("BACKUP_EXISTS",[&]{r::restore_backup(f.backup.u8string(),archives,target1,target1dir.u8string());});
 checks.push_back({{"case","future_page_guard_COMMIT_boundaries_generation_gaps_and_new_target_paths"},{"status","PASS"}});
 auto control=read_json(f.primary/"control.json");auto input=r::open_read_only((f.primary/"data.pages").u8string());auto bad=control;bad["recovery_mode"]="ARIES_PAGE";error("BACKUP_MODE",[&]{r::create_backup(*input,bad,control["checkpoint_lsn"],(dir/"aries-rejected").u8string());});input.reset();
 auto forkdir=dir/"fork";fs::create_directory(forkdir);auto original=read_bytes(f.archives/f.second["file"].get<std::string>());auto changed=original;for(std::size_t at=64;at<changed.size();){auto length=get32(changed.data()+at+4);put64(changed.data()+at+24,get64(changed.data()+at+24)+1000);put32(changed.data()+at+36,0);put32(changed.data()+at+36,crc(changed.data()+at,48));at+=48+length;}write_bytes(forkdir/"valid-fork.log",changed);auto forkfile=r::open_read_only((forkdir/"valid-fork.log").u8string());auto fork=r::archive_wal(*forkfile,control["uuid"],f.second["timeline"],forkdir.u8string());
 error("ARCHIVE_FORK",[&]{r::archive_wal(*forkfile,control["uuid"],f.second["timeline"],f.archives.u8string());});
 error("ARCHIVE_FORK",[&]{r::restore_backup(f.backup.u8string(),{second_marker.u8string(),marker(forkdir,fork).u8string()},target2,(dir/"fork-rejected").u8string());});
 checks.push_back({{"case","ARIES_mode_rejected_and_valid_checksums_same_LSN_fork_rejected"},{"status","PASS"}});
 {auto legacy=read_json(f.backup/"control.json");auto modern=legacy;modern["version"]=4;modern["control_version"]=4;modern["page_format"]=3;auto readonly=r::open_read_only((f.backup/"data.pages").u8string());auto modern_backup=dir/"control4-backup";auto created=r::create_backup(*readonly,modern,modern["checkpoint_lsn"],modern_backup.u8string());check(created["source_control_version"]==4&&r::inspect_backup(modern_backup.u8string())["page_format"]==3,"control4 backup version lost");auto modern_restore=dir/"control4-restored";r::restore_backup(modern_backup.u8string(),{},created["end_lsn"],modern_restore.u8string());check(read_json(modern_restore/"control.json")["version"]==4,"restore silently downgraded control4");check(state(modern_restore)==json::array({json::array({1,"原始一"}),json::array({2,"原始二"})}),"integrated control4 restored rows");auto future=read_json(modern_restore/"control.json");future["minimum_reader_format"]=5;format_copy::write_control(modern_restore,future);IoScope probe("future reader compatibility guard");error("INCOMPATIBLE_FORMAT",[&]{Engine unsupported(modern_restore.u8string(),2);});check(probe.snapshot()["files"]["data"]["read_calls"]==0,"future control touched page/catalog before refusal");checks.push_back({{"case","control4_page3_version_preservation_and_future_reader_rejection"},{"status","PASS"},{"scope","integrated control4 SQL accepts current reader and rejects future minimum reader before data access"}});}
 return {{"status","PASS"},{"checks",checks},{"fixture",{{"primary",f.primary.u8string()},{"backup",f.backup.u8string()},{"archives",f.archives.u8string()},{"first_manifest",first_marker.u8string()},{"second_manifest",second_marker.u8string()},{"third_manifest",third_marker.u8string()}}},{"scope","Actual immutable native Engine SQL, cold module baseline; live barrier/role gate/fencing SQL integration not yet claimed"}};
}
}
int main(int argc,char** argv){try{if(argc==1){auto root=fs::absolute(fs::path("recovery_archive-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));std::cout<<unit(root).dump(2)<<"\n";return 0;}if(argc==3&&std::string(argv[1])=="--unit"){std::cout<<unit(fs::u8path(argv[2])).dump(2)<<"\n";return 0;}throw std::runtime_error("usage --unit NEW_TEST_DIRECTORY");}catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 5;}}
