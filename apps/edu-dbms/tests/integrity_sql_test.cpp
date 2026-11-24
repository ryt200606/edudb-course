#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <set>
#include <stdexcept>
#include <thread>

using edudb::Engine;
using edudb::json;
namespace fs=std::filesystem;
using namespace std::chrono_literals;

namespace {
void require(bool ok,const std::string& message) { if(!ok)throw std::runtime_error(message); }
json run(Engine& db,const std::string& id,const std::string& sql,const std::string& error={}) {
    auto r=db.execute(id,sql);
    require(error.empty()?r.value("ok",false):(!r.value("ok",true)&&r.at("error").at("code")==error),sql+" => "+r.dump());
    return r;
}
void count(Engine& db,const std::string& id,const std::string& relation,int n) {
    auto rows=run(db,id,"SELECT COUNT(*) FROM "+relation).at("rows");
    require(rows==json::array({json::array({n})}),"Unexpected cardinality of "+relation+": "+rows.dump());
}
void wait_for_block(Engine& db) {
    const auto limit=std::chrono::steady_clock::now()+3s;
    while(std::chrono::steady_clock::now()<limit){
        if(!db.diagnostics().at("waits").empty())return;
        std::this_thread::sleep_for(5ms);
    }
    throw std::runtime_error("No real lock waiter observed");
}
std::string schema_and_rows(const fs::path& path) {
    Engine db(path.u8string());
    auto a=db.open_session(),b=db.open_session();
    run(db,a,"CREATE TABLE students(id INTEGER PRIMARY KEY, name VARCHAR(32) NOT NULL)");
    run(db,a,"CREATE TABLE courses(term INTEGER,code VARCHAR(12),PRIMARY KEY(term,code))");
    run(db,a,"CREATE TABLE enrolled(sid INTEGER,term INTEGER,code VARCHAR(12),grade INTEGER, PRIMARY KEY(sid,term,code), CONSTRAINT score CHECK(grade>=0 AND grade<=100),CONSTRAINT student_fk FOREIGN KEY(sid) REFERENCES students(id) ON DELETE CASCADE, CONSTRAINT course_fk FOREIGN KEY(term,code) REFERENCES courses(term,code) ON DELETE RESTRICT)");
    run(db,a,"CREATE TABLE notes(id INTEGER PRIMARY KEY,sid INTEGER,term INTEGER,code VARCHAR(12),FOREIGN KEY(sid,term,code) REFERENCES enrolled(sid,term,code) ON DELETE CASCADE)");
    run(db,a,"INSERT INTO students VALUES(1,'陈同学'),(2,'Student B')");
    run(db,a,"INSERT INTO courses VALUES(1,'DB')");
    run(db,a,"INSERT INTO enrolled VALUES(1,1,'DB',NULL)");
    run(db,a,"INSERT INTO notes VALUES(1,1,1,'DB'),(2,777,NULL,'DB')");
    run(db,a,"INSERT INTO enrolled VALUES(2,1,'DB',90),(3,1,'DB',98)","FOREIGN_KEY_VIOLATION");
    count(db,a,"enrolled",1);
    run(db,a,"UPDATE enrolled SET grade=101","CHECK_VIOLATION");
    run(db,a,"DELETE FROM courses","FOREIGN_KEY_VIOLATION");
    run(db,a,"UPDATE courses SET term=2","FOREIGN_KEY_VIOLATION");
    run(db,a,"TRUNCATE TABLE courses","DEPENDENT_OBJECT");
    run(db,a,"DROP TABLE courses","DEPENDENT_OBJECT");
    run(db,a,"ALTER TABLE enrolled DROP COLUMN grade","DEPENDENT_OBJECT");
    run(db,a,"ALTER TABLE enrolled RENAME COLUMN grade TO mark");
    run(db,a,"UPDATE enrolled SET mark=-1","CHECK_VIOLATION");
    run(db,a,"ALTER TABLE courses RENAME COLUMN code TO unit");
    run(db,a,"ALTER TABLE courses RENAME TO units");
    run(db,a,"INSERT INTO enrolled VALUES(2,1,'BAD',50)","FOREIGN_KEY_VIOLATION");
    run(db,a,"ALTER TABLE enrolled ADD CONSTRAINT only_high CHECK(mark>90)");
    run(db,a,"UPDATE enrolled SET mark=90","CHECK_VIOLATION");
    run(db,a,"ALTER TABLE enrolled DROP CONSTRAINT only_high");
    run(db,a,"UPDATE enrolled SET mark=90");
    run(db,a,"ALTER TABLE enrolled ADD CONSTRAINT impossible CHECK(mark<10)","CHECK_VIOLATION");
    run(db,a,"ALTER TABLE enrolled ADD CONSTRAINT bad_fk FOREIGN KEY(mark) REFERENCES units(term)","INVALID_CONSTRAINT");
    auto csv=db.import_data(a,{{"format","csv"},{"table","enrolled"},{"content","sid,term,code,mark\n2,1,DB,99\n3,1,DB,88\n"}});
    require(!csv.at("ok").get<bool>()&&csv.at("error").at("code")=="FOREIGN_KEY_VIOLATION",csv.dump());
    count(db,a,"enrolled",1);
    run(db,a,"BEGIN");
    run(db,a,"DELETE FROM students WHERE id=1");
    count(db,a,"enrolled",0);count(db,a,"notes",1);
    run(db,a,"ROLLBACK");
    count(db,a,"notes",2);count(db,a,"enrolled",1);
    run(db,a,"BEGIN");run(db,a,"DELETE FROM students WHERE id=2");
    auto child=std::async(std::launch::async,[&]{return db.execute(b,"INSERT INTO enrolled VALUES(2,1,'DB',70)");});
    wait_for_block(db);run(db,a,"COMMIT");
    auto missing=child.get();require(!missing.at("ok").get<bool>()&&missing.at("error").at("code")=="FOREIGN_KEY_VIOLATION",missing.dump());
    run(db,a,"INSERT INTO students VALUES(2,'Student B')");
    run(db,b,"BEGIN");run(db,b,"INSERT INTO enrolled VALUES(2,1,'DB',70)");
    auto parent=std::async(std::launch::async,[&]{return db.execute(a,"DELETE FROM students WHERE id=2");});
    wait_for_block(db);run(db,b,"COMMIT");
    require(parent.get().at("ok").get<bool>(),"Cascading parent delete after child commit failed");
    count(db,a,"enrolled",1);
    run(db,a,"CREATE TABLE selfref(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES selfref(id) ON DELETE CASCADE)");
    run(db,a,"INSERT INTO selfref VALUES(11,12),(12,11)");
    run(db,a,"DELETE FROM selfref WHERE id=11");count(db,a,"selfref",0);
    run(db,a,"CREATE TABLE stale(id INTEGER PRIMARY KEY)");
    run(db,b,"BEGIN PROTOCOL SNAPSHOT");
    run(db,a,"ALTER TABLE stale ADD CONSTRAINT positive CHECK(id>0)","PROTOCOL_MODE_BUSY");
    run(db,b,"ROLLBACK");
    run(db,a,"ALTER TABLE stale ADD CONSTRAINT positive CHECK(id>0)");
    run(db,b,"BEGIN PROTOCOL SNAPSHOT");
    run(db,b,"INSERT INTO stale VALUES(-1)","INTEGRITY_PROTOCOL");run(db,b,"ROLLBACK");
    run(db,a,"CHECKPOINT");
    auto exported=db.export_data(a,{{"format","sql"}});
    require(exported.at("ok").get<bool>(),exported.dump());
    auto dump=exported.at("content").get<std::string>();
    require(dump.find("ADD CONSTRAINT \"student_fk\"")!=std::string::npos,"Missing FK in dump");
    return dump;
}
void verify_reopened(Engine& db) {
    auto a=db.open_session();count(db,a,"enrolled",1);
    run(db,a,"INSERT INTO enrolled VALUES(98,1,'DB',85)","FOREIGN_KEY_VIOLATION");
    run(db,a,"UPDATE enrolled SET mark=102","CHECK_VIOLATION");
    auto constraints=run(db,a,"DESCRIBE enrolled").at("constraints");
    require(constraints.size()==3,"Expected three named constraints");
    run(db,a,"DELETE FROM students");count(db,a,"enrolled",0);count(db,a,"notes",1);
    run(db,a,"VERIFY");
}
}
int main() {
    const auto root=fs::temp_directory_path()/("edudb-integrity-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(root);
    try{
        auto dump=schema_and_rows(root/"source");
        {Engine db((root/"source").u8string());verify_reopened(db);}
        {Engine copy((root/"restored").u8string());auto a=copy.open_session();auto r=copy.import_data(a,{{"format","sql"},{"content",dump}});require(r.at("ok").get<bool>(),r.dump());verify_reopened(copy);}
        fs::remove_all(root);std::cout<<"PASS check/FK DDL, composite MATCH SIMPLE, cascading rollback, both concurrent orders, snapshot guard, checkpoint/restart, CSV and SQL round trip\n";
        return 0;
    }catch(const std::exception& e){std::cerr<<e.what()<<"\nFixture: "<<root<<"\n";return 1;}
}
