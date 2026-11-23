#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <thread>
using namespace edudb;
namespace fs=std::filesystem;
namespace {
int checks=0;
void check(bool yes,const std::string& why){++checks;if(!yes)throw std::runtime_error(why);}
json sql(Engine& db,const std::string& s,const std::string& text){auto r=db.execute(s,text);check(r.value("ok",false),text+": "+r.dump());return r;}
StorageOptions options(){StorageOptions o;o.recovery_mode="ARIES_PAGE";return o;}
void seed(const fs::path& dir){
    Engine db(dir.u8string(),2,32u*1024u*1024u,options());auto s=db.open_session();
    sql(db,s,"CREATE CLUSTER c");
    sql(db,s,"CREATE TABLE a(id INTEGER PRIMARY KEY,k INTEGER,v VARCHAR(200)) RECORD FORMAT FIXED CLUSTER c(k)");
    sql(db,s,"CREATE TABLE b(id INTEGER PRIMARY KEY,k INTEGER,v VARCHAR(200)) RECORD FORMAT VARIABLE CLUSTER c(k)");
    std::string insert="INSERT INTO a VALUES";
    for(int n=1;n<=24;++n){if(n>1)insert+=",";insert+="("+std::to_string(n)+",7,'原值')";}sql(db,s,insert);
    sql(db,s,"INSERT INTO b VALUES(1,7,'成员乙'),(2,NULL,'空搜索码')");
    sql(db,s,"CREATE INDEX ak ON a(k) WITH(max_keys=5)");db.checkpoint();
}
json snapshot(const fs::path& dir){Engine db(dir.u8string(),2);auto s=db.open_session();auto a=sql(db,s,"SELECT * FROM a ORDER BY id");auto b=sql(db,s,"SELECT * FROM b ORDER BY id");auto verify=sql(db,s,"VERIFY");return {{"status","PASS"},{"a",a["rows"]},{"b",b["rows"]},{"verification",verify["verification"]},{"diagnostics",db.diagnostics()}};}
void waiting(Engine& db){
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
    while(std::chrono::steady_clock::now()<end){auto d=db.diagnostics();for(const auto& q:d["queues"])if(q["resource"].get<std::string>().rfind("cluster:",0)==0){check(!d["waits"].empty(),"cluster waiter omitted from real wait graph");return;}std::this_thread::sleep_for(std::chrono::milliseconds(2));}
    throw std::runtime_error("ARIES member access failed before reaching the actual cluster wait queue");
}
json units(const fs::path& dir){seed(dir);json observed;
    {Engine db(dir.u8string(),2);auto s=db.open_session(),r=db.open_session();
        auto inspection=sql(db,s,"INSPECT CLUSTER c LIMIT 256")["inspection"];bool shared=false;for(const auto& page:inspection["pages"])shared=shared||page["table_row_counts"].size()==2;check(shared,"ARIES fixture does not have true mixed-member HEAP pages");
        sql(db,s,"BEGIN");sql(db,s,"UPDATE a SET v='等待后提交' WHERE id<=24");
        auto read=std::async(std::launch::async,[&]{return db.execute(r,"SELECT COUNT(*) FROM b");});waiting(db);check(read.wait_for(std::chrono::milliseconds(0))!=std::future_status::ready,"member B read bypassed member A cluster X");sql(db,s,"COMMIT");auto result=read.get();check(result.value("ok",false)&&result["rows"][0][0]==2,"waiting ARIES reader could not read committed member B");
        sql(db,s,"BEGIN");sql(db,s,"UPDATE a SET v='事务回滚' WHERE id=1");
        auto write=std::async(std::launch::async,[&]{return db.execute(r,"UPDATE b SET v='排队写入' WHERE id=1");});waiting(db);sql(db,s,"ROLLBACK");result=write.get();check(result.value("ok",false)&&result["affected_rows"]==1,"cross-member ARIES writer did not resume after rollback");
        sql(db,s,"BEGIN");sql(db,s,"SAVEPOINT membership");sql(db,s,"ALTER TABLE a NO CLUSTER");sql(db,s,"ROLLBACK TO membership");sql(db,s,"COMMIT");check(sql(db,s,"DESCRIBE a").contains("cluster"),"CLR did not restore persistent catalog route/membership");
        sql(db,s,"ALTER TABLE a RENAME TO a_renamed");sql(db,s,"ALTER TABLE a_renamed RENAME TO a");
        check(sql(db,s,"SELECT COUNT(*) FROM a")["rows"][0][0]==24,"cluster table rename lost rows");
        auto bad=db.import_data(s,{{"format","csv"},{"table","b"},{"header",true},{"content","id,k,v\n3,7,待回滚\n1,7,主键冲突\n"}});check(!bad.value("ok",true),"ARIES member CSV violation did not reject whole import");check(sql(db,s,"SELECT COUNT(*) FROM b WHERE id=3")["rows"][0][0]==0,"failed ARIES CSV left row visible");
        auto verified=sql(db,s,"VERIFY");check(verified["verification"]["ok"],"ARIES shared-page/index verification failed");observed=db.diagnostics();check(observed["storage"]["steal_flushes"].get<std::uint64_t>()>0,"joint fixture never exercised actual uncommitted page steal");check(!observed["storage"]["recovery_required"].get<bool>(),"joint normal operations poisoned ARIES");
    }
    auto again=snapshot(dir);check(again["a"].size()==24&&again["b"].size()==2,"ARIES clustered data failed reopen");return {{"status","PASS"},{"checks",checks},{"observations",observed},{"reopened",again}};
}
void worker(const std::string& scenario,const fs::path& dir){Engine db(dir.u8string(),2);auto s=db.open_session();sql(db,s,"BEGIN");sql(db,s,"UPDATE a SET v='崩溃改动' WHERE id<=24");sql(db,s,"INSERT INTO b VALUES(99,7,'新增成员行')");if(scenario=="committed")sql(db,s,"COMMIT");else check(scenario=="loser","unknown worker scenario");auto d=db.diagnostics();check(d["storage"]["steal_flushes"].get<std::uint64_t>()>0,"worker did not actually steal any page");std::ofstream out(dir/"ready.json",std::ios::binary);out<<d.dump();out.flush();check(bool(out),"worker marker failed");out.close();std::cout<<"READY\n"<<std::flush;std::this_thread::sleep_for(std::chrono::seconds(20));throw std::runtime_error("parent must terminate owned child");}
}
int main(int argc,char** argv){try{
    if(argc==1){auto dir=fs::absolute(fs::path("cluster-aries-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));std::cout<<units(dir).dump()<<'\n';return 0;}
    if(argc==3&&std::string(argv[1])=="--seed"){seed(fs::u8path(argv[2]));std::cout<<"{\"status\":\"PASS\"}\n";return 0;}
    if(argc==3&&std::string(argv[1])=="--snapshot"){std::cout<<snapshot(fs::u8path(argv[2])).dump()<<'\n';return 0;}
    if(argc==4&&std::string(argv[1])=="--worker"){worker(argv[2],fs::u8path(argv[3]));return 2;}
    throw std::runtime_error("bad arguments");
}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
