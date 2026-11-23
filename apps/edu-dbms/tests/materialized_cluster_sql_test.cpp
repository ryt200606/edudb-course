#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <thread>
using namespace edudb;
namespace fs=std::filesystem;
namespace {
int checks=0;
void check(bool yes,const std::string& why){++checks;if(!yes)throw std::runtime_error(why);}
json sql(Engine& e,const std::string& s,const std::string& text){auto r=e.execute(s,text);check(r.value("ok",false),text+": "+r.dump());return r;}
void rows(Engine& e,const std::string& s,const std::string& text,const json& expected){auto r=sql(e,s,text);check(r["rows"]==expected,text+" rows="+r["rows"].dump());}
void wait_view(Engine& e){auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);while(std::chrono::steady_clock::now()<end){auto d=e.diagnostics();for(const auto& q:d["queues"])if(q["resource"]=="table:vsp"){check(!d["waits"].empty(),"view wait omitted from graph");return;}std::this_thread::sleep_for(std::chrono::milliseconds(2));}throw std::runtime_error("dependent MV reader did not reach a real lock queue");}
void cases(const fs::path& path,const std::string& mode){
    StorageOptions opts;opts.recovery_mode=mode;
    {Engine e((path/"source").u8string(),3,32u*1024u*1024u,opts);auto a=e.open_session(),b=e.open_session(),reader=e.open_session();
        sql(e,a,"CREATE CLUSTER c");sql(e,a,"CREATE TABLE a(id INTEGER PRIMARY KEY,k INTEGER,score INTEGER) RECORD FORMAT FIXED CLUSTER c(k)");
        sql(e,a,"CREATE TABLE b(id INTEGER PRIMARY KEY,k INTEGER,v VARCHAR(100)) RECORD FORMAT VARIABLE CLUSTER c(k)");sql(e,a,"CREATE TABLE other(id INTEGER PRIMARY KEY,v VARCHAR(100))");
        sql(e,a,"INSERT INTO a VALUES(1,7,10),(2,7,20),(3,NULL,NULL)");sql(e,a,"INSERT INTO b VALUES(1,7,'共享页成员')");
        sql(e,a,"CREATE MATERIALIZED VIEW vsp WITH(refresh='immediate',maintenance='incremental') AS SELECT id,score FROM a WHERE score>=0");
        sql(e,a,"CREATE MATERIALIZED VIEW g WITH(refresh='immediate',maintenance='incremental') AS SELECT k,COUNT(*) AS n,SUM(score) AS total,AVG(score) AS average FROM a GROUP BY k");
        sql(e,a,"BEGIN");auto changed=sql(e,a,"UPDATE a SET score=11 WHERE id=1");bool sp=false,group=false;for(const auto& r:changed["materialized_refreshes"]){sp=sp||r["maintenance"]=="INCREMENTAL_SP";group=group||r["maintenance"]=="INCREMENTAL_GROUP";check(r["base_rescan_rows"]==0,"joint path fell back to a full base scan");}check(sp&&group,"both real delta paths must execute");
        auto unrelated=std::async(std::launch::async,[&]{return e.execute(b,"INSERT INTO other VALUES(1,'无关写者')");});check(unrelated.wait_for(std::chrono::seconds(2))==std::future_status::ready,"unrelated table writer blocked on mutable MV metadata discovery");check(unrelated.get().value("ok",false),"unrelated writer failed while MV descriptors were dirty");
        auto names=sql(e,b,"SHOW TABLES");for(const auto& row:names["rows"])check(row[0].get<std::string>().rfind("__mv_state_",0)!=0,"auxiliary state leaked into normal table browser");
        auto pending=std::async(std::launch::async,[&]{return e.execute(reader,"SELECT COUNT(*) FROM vsp");});wait_view(e);sql(e,a,"COMMIT");check(pending.get().value("ok",false),"dependent view reader failed after actual wait");
        rows(e,a,"SELECT total,average FROM g WHERE k=7",json::array({json::array({31,15.5})}));
        sql(e,a,"BEGIN");sql(e,a,"SAVEPOINT keep");sql(e,a,"UPDATE a SET k=9,score=40 WHERE id=1");sql(e,a,"ROLLBACK TO keep");sql(e,a,"UPDATE a SET score=22 WHERE id=2");sql(e,a,"COMMIT");
        rows(e,a,"SELECT id,score FROM vsp ORDER BY id",json::array({json::array({1,11}),json::array({2,22})}));rows(e,a,"SELECT n,total,average FROM g WHERE k=7",json::array({json::array({2,33,16.5})}));
        auto bad=e.import_data(a,{{"format","csv"},{"table","a"},{"header",true},{"content","id,k,score\n4,7,99\n1,7,88\n"}});check(!bad.value("ok",true),"CSV constraint failure accepted");rows(e,a,"SELECT COUNT(*) FROM a WHERE id=4",json::array({json::array({0})}));rows(e,a,"SELECT total FROM g WHERE k=7",json::array({json::array({33})}));
        auto dump=e.export_data(a,{{"format","sql"}});check(dump.value("ok",false),"joint logical export failed: "+dump.dump());const auto content=dump["content"].get<std::string>();check(content.find("ALTER TABLE")<content.find("CREATE MATERIALIZED VIEW"),"dump must finish cluster attachment before deriving fresh views");
        {Engine restored((path/"restored").u8string(),3,32u*1024u*1024u,opts);auto s=restored.open_session();auto imported=restored.import_data(s,{{"format","sql"},{"content",content}});check(imported.value("ok",false),"joint SQL restore failed: "+imported.dump());rows(restored,s,"SELECT n,total,average FROM g WHERE k=7",json::array({json::array({2,33,16.5})}));auto views=sql(restored,s,"SHOW MATERIALIZED VIEWS");for(const auto& row:views["rows"])check(row[2].get<bool>(),"restored view is stale because attachment followed its definition");check(sql(restored,s,"VERIFY")["verification"]["ok"],"restored mixed kernel failed verification");}
        check(sql(e,a,"VERIFY")["verification"]["ok"],"original mixed kernel failed verification");
    }
    {Engine reopened((path/"source").u8string(),3);auto s=reopened.open_session();rows(reopened,s,"SELECT n,total,average FROM g WHERE k=7",json::array({json::array({2,33,16.5})}));check(reopened.diagnostics()["storage"]["transactions"]["page_version_graph"]["invalid_events"]==0,"combined mode produced invalid page-version events");}
}
}
int main(){try{auto root=fs::absolute(fs::path("mv-cluster-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));cases(root/"private","REDO_PRIVATE");cases(root/"aries","ARIES_PAGE");std::cout<<"PASS joint materialized/cluster/ARIES SQL "<<checks<<" checks\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
