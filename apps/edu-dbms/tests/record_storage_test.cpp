#include "edudb.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
using namespace edudb;
namespace fs=std::filesystem;
namespace {
std::size_t checks=0;
void check(bool b,const std::string& why){++checks;if(!b)throw std::runtime_error(why);}
json ok(Engine& db,const std::string& s,const std::string& sql){auto r=db.execute(s,sql);check(r.value("ok",false),sql+": "+r.dump());return r;}
void error(Engine& db,const std::string& s,const std::string& sql,const std::string& code){auto r=db.execute(s,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,sql+": "+r.dump());}
json col(const char* n,const char* t,unsigned length=0){return {{"name",n},{"type",t},{"length",length},{"not_null",false},{"primary",false},{"unique",false},{"default",nullptr}};}
void sql_cases(const fs::path& path,const std::string& format){
    json expected;
    {Engine db(path.u8string(),3);auto s=db.open_session();
        ok(db,s,"CREATE TABLE r(id INTEGER PRIMARY KEY,v REAL,flag BOOLEAN,note VARCHAR(20)) RECORD FORMAT "+format);
        ok(db,s,"INSERT INTO r VALUES(-9223372036854775808,1.25,TRUE,'中文😀'),(0,NULL,FALSE,'短'),(9223372036854775807,0,NULL,NULL)");
        expected=ok(db,s,"SELECT * FROM r USE SCAN ORDER BY id")["rows"];
        auto d=ok(db,s,"DESCRIBE r");check(d["record_layout"]["format"]==format,"DESCRIBE actual format");
        auto inspect=ok(db,s,"INSPECT TABLE r")["inspection"]["structure"];std::size_t fixed=0;
        for(auto& page:inspect["pages"])for(auto& row:page["rows"]){check(row["actual_record_format"]==format,"actual slot bytes match selected format");auto bytes=row["record_bytes"].get<std::size_t>();if(format=="FIXED"){check(bytes==d["record_layout"]["fixed_record_bytes"],"fixed width equals published layout");if(fixed)check(fixed==bytes,"NULL does not shorten a fixed record");fixed=bytes;}}
        ok(db,s,"CREATE INDEX note_idx ON r(note)");check(ok(db,s,"SELECT id,note FROM r USE INDEX(note_idx) WHERE note='中文😀'")["rows"][0][0]==(-9223372036854775807LL-1),"string index locates binary row");
        ok(db,s,"BEGIN");ok(db,s,"SAVEPOINT original");ok(db,s,"ALTER TABLE r RECORD FORMAT "+std::string(format=="FIXED"?"VARIABLE":"FIXED"));ok(db,s,"UPDATE r SET note='保存点之后' WHERE id=0");ok(db,s,"ROLLBACK TO original");check(ok(db,s,"SELECT * FROM r ORDER BY id")["rows"]==expected,"schema and binary page savepoint rollback");check(ok(db,s,"DESCRIBE r")["record_layout"]["format"]==format,"savepoint restores schema fingerprint and format");ok(db,s,"COMMIT");
        ok(db,s,"ALTER TABLE r RENAME COLUMN note TO renamed");check(ok(db,s,"SELECT * FROM r ORDER BY id")["rows"]==expected,"rename transactionally rewrites fingerprint");
        ok(db,s,"BEGIN");error(db,s,"INSERT INTO r VALUES(0,2,TRUE,'重复')","UNIQUE_VIOLATION");error(db,s,"SELECT * FROM r","TRANSACTION_ABORTED");ok(db,s,"ROLLBACK");check(ok(db,s,"SELECT * FROM r ORDER BY id")["rows"]==expected,"constraint abort preserves binary records");
        const auto before=db.diagnostics()["storage"]["next_page_id"];
        error(db,s,"CREATE TABLE too_wide(x VARCHAR(4000)) RECORD FORMAT FIXED","ROW_TOO_LARGE");check(db.diagnostics()["storage"]["next_page_id"]==before,"layout rejected before any page allocation");
        error(db,s,"CREATE TABLE wrong_org(x INTEGER) ORGANIZATION BPLUS(x) RECORD FORMAT FIXED","RECORD_LAYOUT");
        auto exported=db.export_data(s,{{"format","sql"},{"table","r"}});check(exported.value("ok",false),"logical export");check(exported["content"].get<std::string>().find("RECORD FORMAT "+format)!=std::string::npos,"SQL dump preserves record format");
        auto dest=path/"logical-copy";Engine copy(dest.u8string(),3);auto cs=copy.open_session();auto imported=copy.import_data(cs,{{"format","sql"},{"content",exported["content"]}});check(imported.value("ok",false),"SQL dump import uses native codec");check(ok(copy,cs,"SELECT * FROM r ORDER BY id")["rows"]==expected,"logical roundtrip exact rows");check(ok(copy,cs,"DESCRIBE r")["record_layout"]["format"]==format,"logical roundtrip physical format");copy.close_session(cs);
        ok(db,s,"VERIFY");db.close_session(s);
    }
    {Engine db(path.u8string(),2);auto s=db.open_session();check(ok(db,s,"SELECT * FROM r ORDER BY id")["rows"]==expected,"reopen redoes committed binary records");ok(db,s,"VERIFY");db.close_session(s);}
}
void storage_cases(const fs::path& path){
    Storage db(path.u8string(),2);auto tx=db.begin();json table={{"columns",json::array({col("id","INTEGER"),col("payload","VARCHAR",7800)})},{"rows",json::array()},{"indexes",json::array()},{"record_format","VARIABLE"}};db.write_table(tx,"m",table);
    auto a=db.insert_row(tx,"m",json::array({1,std::string(2300,'a')})),b=db.insert_row(tx,"m",json::array({2,std::string(2300,'b')})),c=db.insert_row(tx,"m",json::array({3,std::string(2300,'c')}));check(a.page_id==b.page_id&&b.page_id==c.page_id,"variable fixture shares physical page");auto moved=db.update_row(tx,"m",a,json::array({1,std::string(7000,'x')}));check(!(moved==a)&&moved.logical_id==a.logical_id,"variable growth moves RID and retains stable LID");check(db.verify(tx)["ok"]==true,"migration row map is consistent");db.commit(tx);
    tx=db.begin();auto original=db.read_table(tx,"m");const auto ids=original["row_ids"];db.savepoint(tx,"before_conversion");original["record_format"]="JSON";db.write_table(tx,"m",original);check(db.read_table(tx,"m")["row_ids"]==ids,"transactional rewrite retains logical identities");db.rollback_to(tx,"before_conversion");check(db.read_table(tx,"m",false)["record_layout"]["format"]=="VARIABLE","native savepoint restores variable metadata and slots");check(db.verify(tx)["ok"]==true,"native rewrite rollback graph");db.commit(tx);
}
void blob_cases(const fs::path& path){Engine db(path.u8string(),3);auto s=db.open_session();ok(db,s,"CREATE TABLE b(id INTEGER PRIMARY KEY,data BLOB) RECORD FORMAT FIXED WITH(blob_ref_bytes=1024)");std::string hex;for(int i=0;i<12000;++i)hex+="AB";ok(db,s,"INSERT INTO b VALUES(1,X'"+hex+"')");auto result=ok(db,s,"SELECT data FROM b WHERE id=1");check(result["rows"][0][0]["$blob"]==hex,"fixed record retains complete chunked BLOB reference");ok(db,s,"ALTER TABLE b RECORD FORMAT VARIABLE WITH(blob_ref_bytes=1024)");check(ok(db,s,"SELECT data FROM b WHERE id=1")["rows"]==result["rows"],"BLOB survives record rewrite");ok(db,s,"VERIFY");db.close_session(s);}
void snapshot_cases(const fs::path& path){Engine db(path.u8string(),3);auto a=db.open_session(),b=db.open_session();ok(db,a,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER) RECORD FORMAT FIXED");ok(db,a,"INSERT INTO t VALUES(1,10)");ok(db,a,"BEGIN PROTOCOL SNAPSHOT");check(ok(db,a,"SELECT v FROM t")["rows"][0][0]==10,"snapshot first binary image");ok(db,b,"BEGIN PROTOCOL SNAPSHOT");ok(db,b,"UPDATE t SET v=11 WHERE id=1");ok(db,b,"COMMIT");check(ok(db,a,"SELECT v FROM t")["rows"][0][0]==10,"page history decodes old binary snapshot");ok(db,a,"COMMIT");check(ok(db,b,"SELECT v FROM t")["rows"][0][0]==11,"new committed binary image");db.close_session(a);db.close_session(b);}
}
int main(){try{auto root=fs::absolute(fs::path("record-storage-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));check(!fs::exists(root),"fresh root");fs::create_directory(root);sql_cases(root/"fixed","FIXED");sql_cases(root/"variable","VARIABLE");storage_cases(root/"migration");blob_cases(root/"blob");snapshot_cases(root/"snapshot");std::cout<<"PASS record Storage/Engine "<<checks<<" checks; "<<root.u8string()<<'\n';return 0;}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
