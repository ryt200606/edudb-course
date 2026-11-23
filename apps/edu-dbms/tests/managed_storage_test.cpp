#include "edudb.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
using namespace edudb;
namespace fs=std::filesystem;
namespace {
std::size_t checks=0;
void require(bool b,const std::string& message){++checks;if(!b)throw std::runtime_error(message);}
json sql(Engine& db,const std::string& session,const std::string& statement){auto r=db.execute(session,statement);require(r.value("ok",false),statement+": "+r.dump());return r;}
fs::path member(const fs::path& dir,std::size_t i){std::ostringstream s;s<<"data-member-"<<std::setfill('0')<<std::setw(2)<<i<<".pages";return dir/s.str();}
template<class F>void rejects(F f,const std::string& reason){bool failed=false;try{f();}catch(const DbError&){failed=true;}require(failed,reason);}
void run(const fs::path& dir,const std::string& layout,std::size_t members){
    StorageOptions options{layout,members};json expected;
    {Engine db(dir.u8string(),4,32u*1024u*1024u,options);auto s=db.open_session();
        sql(db,s,"CREATE TABLE student(id INTEGER PRIMARY KEY,name VARCHAR(64),score INTEGER)");
        sql(db,s,"INSERT INTO student VALUES(1,'中文学生',81),(2,'周同学',NULL),(3,'李同学',95),(4,'王同学',70)");
        for(const auto& policy:{"MRU","CLOCK","TOSS_IMMEDIATE","LRU"}){
            sql(db,s,std::string("SET BUFFER POLICY ")+policy);sql(db,s,"SET BUFFER READ_AHEAD 2");
            require(sql(db,s,"SHOW BUFFER").at("diagnostic").at("buffer_policy")==policy,"buffer policy actual runtime SQL configuration");
            require(sql(db,s,"SELECT COUNT(*) FROM student").at("rows")[0][0]==4,"buffer policy preserves SQL results");
        }
        sql(db,s,"SET BUFFER READ_AHEAD 0");
        for(const auto& policy:{"WAIT_DIE","WOUND_WAIT","DETECT"}){sql(db,s,std::string("SET LOCK POLICY ")+policy);require(sql(db,s,"SHOW LOCKS").at("diagnostic").at("deadlock_policy")==policy,"runtime deadlock policy matches SHOW LOCKS");}
        sql(db,s,"CREATE INDEX ix_score ON student(score)");
        sql(db,s,"BEGIN");sql(db,s,"UPDATE student SET score=90 WHERE id=1");sql(db,s,"SAVEPOINT keep");sql(db,s,"DELETE FROM student WHERE id=4");sql(db,s,"ROLLBACK TO keep");sql(db,s,"COMMIT");
        sql(db,s,"BEGIN");sql(db,s,"INSERT INTO student VALUES(9,'撤销',10)");sql(db,s,"ROLLBACK");
        auto bad=db.execute(s,"INSERT INTO student VALUES(1,'重复',10)");require(!bad.value("ok",true),"constraint rejection");
        auto selected=sql(db,s,"SELECT id,name,score FROM student ORDER BY id");expected=selected.at("rows");require(expected.size()==4&&expected[0][2]==90&&expected[1][2].is_null(),"typed rows and rollback");
        auto joined=sql(db,s,"SELECT a.id,b.id FROM student a INNER JOIN student b ON a.id=b.id ORDER BY a.id");require(joined.at("rows").size()==4,"real SQL join on managed storage");
        auto status=sql(db,s,"SHOW MEDIA");require(status.at("diagnostic").at("configuration").at("layout")==layout,"SHOW MEDIA reflects persistent device");
        require(db.checkpoint().at("status")=="PASS","managed checkpoint");require(sql(db,s,"SCRUB MEDIA").at("diagnostic").at("status")=="PASS","SQL physical scrub");
        auto io=db.diagnostics().at("storage").at("io").at("files");require(io.at("data").at("write_bytes").get<std::uint64_t>()>0&&io.at("control").at("sync_calls").get<std::uint64_t>()>0,"actual member data and component journal role counters");
        rejects([&]{Storage::media_maintenance(dir.u8string(),{{"action","status"}});},"offline maintenance cannot bypass active directory lock");db.close_session(s);
    }
    {Engine db(dir.u8string(),4);auto s=db.open_session();require(sql(db,s,"SELECT id,name,score FROM student ORDER BY id").at("rows")==expected,"automatic manifest layout reopen");db.close_session(s);}
    rejects([&]{Engine db(dir.u8string(),4,32u*1024u*1024u,StorageOptions{"SINGLE",0});},"wrong requested layout rejected");
    const auto report=Storage::media_maintenance(dir.u8string(),{{"action","scrub"}});require(report.at("result").at("status")=="PASS","offline scrub");
    if(layout!="RAID0"){
        auto target=member(dir,0);auto aside=fs::path(target.u8string()+".missing-evidence");fs::rename(target,aside);
        {Engine db(dir.u8string(),4);auto s=db.open_session();require(sql(db,s,"SELECT id,name,score FROM student ORDER BY id").at("rows")==expected,"SQL degraded reads reconstruct real missing member");db.close_session(s);}
        require(Storage::media_maintenance(dir.u8string(),{{"action","rebuild"},{"member",std::uint64_t(0)}}).at("result").at("status")=="PASS","offline rebuild before database WAL recovery");
        {Engine db(dir.u8string(),4);auto s=db.open_session();sql(db,s,"UPDATE student SET score=96 WHERE id=3");require(sql(db,s,"SELECT score FROM student WHERE id=3").at("rows")[0][0]==96,"SQL writes resume after member rebuild");db.close_session(s);}
    }
    std::cout<<layout<<'/'<<members<<" SQL PASS\n";
}
}
int main(){try{auto root=fs::absolute(fs::path("managed-storage-test-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));require(!fs::exists(root),"fresh isolated directory");fs::create_directory(root);for(auto config:std::vector<std::pair<std::string,std::size_t>>{{"RAID0",4},{"RAID1",3},{"RAID2",7},{"RAID2",8},{"RAID3",4},{"RAID4",4},{"RAID5",5},{"RAID6",6}})run(root/(config.first+"-"+std::to_string(config.second)),config.first,config.second);std::cout<<json{{"status","PASS"},{"checks",checks},{"evidence_directory",root.u8string()}}.dump()<<'\n';return 0;}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
