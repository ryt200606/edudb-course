#include "edudb.hpp"
#include "query_spill.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#ifdef _WIN32
#include <windows.h>
#endif
using edudb::json;static int checks=0;
static void check(bool b,const std::string& s){++checks;if(!b)throw std::runtime_error(s);}
static json ok(edudb::Engine& db,const std::string& sid,const std::string& sql){auto r=db.execute(sid,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static std::size_t temp_files(const std::filesystem::path& path){std::size_t n=0;for(const auto& e:std::filesystem::directory_iterator(path))if(e.path().filename().u8string().rfind("edudb-query-",0)==0)++n;return n;}
struct Environment {std::string key,old;bool present;Environment(const char* name,const char* value):key(name),present(std::getenv(name)!=nullptr){if(present)old=std::getenv(name);
#ifdef _WIN32
 _putenv_s(key.c_str(),value);
#else
 setenv(key.c_str(),value,1);
#endif
}~Environment(){
#ifdef _WIN32
 _putenv_s(key.c_str(),present?old.c_str():"");
#else
 if(present)setenv(key.c_str(),old.c_str(),1);else unsetenv(key.c_str());
#endif
}};
int main(){
#ifdef _WIN32
 SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
#endif
 auto path=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB共享扫描_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
 try{edudb::Engine db(path.u8string(),8);auto sid=db.open_session();
 ok(db,sid,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER,s VARCHAR(40),b BLOB(32))");
 std::string insert="INSERT INTO t VALUES";for(int n=0;n<75;++n){if(n)insert+=',';insert+='('+std::to_string(n)+','+(n%7?std::to_string(n%9):"NULL")+",'中文"+std::to_string(n%5)+"',X'00FF')";}ok(db,sid,insert);
 ok(db,sid,"INSERT INTO t VALUES(9223372036854775807,1,'大整数',NULL)");ok(db,sid,"CREATE INDEX ix_v ON t(v) USING BITMAP");
 auto twin=[&](const std::vector<std::string>& sql){auto r=db.execute_shared_batch(sid,sql);check(r.value("ok",false),"batch failed "+r.dump());check(r["results"].size()==sql.size(),"result count");for(std::size_t i=0;i<sql.size();++i){auto expected=ok(db,sid,sql[i]);check(r["results"][i]["rows"]==expected["rows"]&&r["results"][i]["columns"]==expected["columns"],"ordinary SELECT differs "+sql[i]+" => "+r.dump());check(r["results"][i]["plan"]["materialized_view_rewrite"]["chosen"]==false,"MV path disabled");}check(temp_files(path)==0,"success removes spool");return r;};
 auto r=twin({"SELECT id,s FROM t WHERE id>70 ORDER BY id","SELECT v,COUNT(*),SUM(v),AVG(id) FROM t GROUP BY v HAVING COUNT(*)>1 ORDER BY v","SELECT DISTINCT s FROM t ORDER BY s LIMIT 3 OFFSET 1","SELECT COUNT(*) FROM t","SELECT id,b,OCTET_LENGTH(b) FROM t WHERE id=1","SELECT id FROM t USE SCAN WHERE NOT(v=1) OR v IS NULL ORDER BY id LIMIT 8"});
 auto m=r["metrics"]["shared_scan"];check(m["base_scan_opens"]==1&&m["base_rows"]==76,"one base cursor and actual row count");check(m["spool_reader_opens"]==6,"six real spool readers");check(m["temporary_io"]["frame_crc"]==true&&m["temporary_io"]["live_runs"]==0&&m["temporary_io"]["bytes_read"].get<unsigned>()>0,"CRC spool actual IO and cleanup");std::size_t qi=0;for(const auto& x:r["results"]){check(x["plan"]["sources"][0]["scan"]=="SharedSpoolScan","actual source named spool");if(qi!=4)check(x["plan"]["sources"][0].value("heap_page_reads",0)==0,"non-BLOB spool source does not rescan base heap");++qi;}
 twin({"SELECT id FROM t WHERE 1/(id-2)>0 AND id=1","SELECT COUNT(v),COUNT(*) FROM t WHERE v IS NULL","SELECT id FROM t WHERE id=NULL OR id=2 ORDER BY id"});
 auto empty=twin({"SELECT id FROM t LIMIT 0","SELECT id FROM t WHERE id>9 AND id<3","SELECT COUNT(*) FROM t LIMIT 0"});check(empty["metrics"]["shared_scan"]["base_scan_opens"]==0&&empty["metrics"]["shared_scan"]["spool_reader_opens"]==0,"all proven empty queries avoid base/spool IO");
 for(const auto& queries:std::vector<std::vector<std::string>>{{},{"SELECT 1"},{"UPDATE t SET v=1"},{"SELECT a.id FROM t a JOIN t b ON a.id=b.id"},{"SELECT id FROM t UNION SELECT id FROM t"},{"SELECT id FROM t WHERE EXISTS(SELECT 1 FROM t)"},{"SELECT id FROM t USE INDEX(__pk_t) WHERE id=1"},{"EXPLAIN SELECT id FROM t"},{"SELECT id FROM t","SELECT missing FROM t"}}){auto e=db.execute_shared_batch(sid,queries);check(!e.value("ok",true),"invalid profile/schema accepted");check(e["transaction_state"]=="idle"&&temp_files(path)==0,"validation failure cleans owned state");ok(db,sid,"SELECT 42");}
 auto too_many=db.execute_shared_batch(sid,std::vector<std::string>(17,"SELECT id FROM t"));check(too_many["error"]["code"]=="BATCH_LIMIT","query count bound");auto too_big=db.execute_shared_batch(sid,{"SELECT id FROM t /*"+std::string(262144,'x')+"*/"});check(too_big["error"]["code"]=="BATCH_LIMIT","total SQL byte bound");
 auto error=db.execute_shared_batch(sid,{"SELECT id FROM t LIMIT 1","SELECT 1/(id-2) FROM t"});check(!error["ok"].get<bool>()&&error["error"]["code"]=="DIVISION_BY_ZERO"&&error["error"]["query_index"]==1&&error["transaction_state"]=="idle","later query runtime error rolls owned batch back");check(!error.contains("results")&&temp_files(path)==0,"no partial results or temporary files on query error");ok(db,sid,"SELECT 42");
 ok(db,sid,"BEGIN");ok(db,sid,"INSERT INTO t VALUES(999,0,'私有',NULL)");auto active=db.execute_shared_batch(sid,{"SELECT id FROM t"});check(active["error"]["code"]=="TRANSACTION_ACTIVE"&&active["transaction_state"]=="active","existing user transaction survives rejection");check(ok(db,sid,"SELECT COUNT(*) FROM t WHERE id=999")["rows"][0][0]==1,"private data intact");ok(db,sid,"ROLLBACK");check(ok(db,sid,"SELECT COUNT(*) FROM t WHERE id=999")["rows"][0][0]==0,"caller rollback remains possible");
 ok(db,sid,"SET TRANSACTION READ ONLY");auto readonly_import=db.import_data(sid,{{"format","csv"},{"table","t"},{"content","id,v,s,b\n999,1,blocked,\\N\n"}});check(readonly_import["error"]["code"]=="READ_ONLY_TRANSACTION"&&readonly_import["transaction_state"]=="idle","readonly import failure cleans its owned transaction");check(db.execute_shared_batch(sid,{"SELECT id FROM t WHERE id=999"})["results"][0]["rows"].empty(),"shared read works after readonly import failure without inserted row");
 ok(db,sid,"SET TRANSACTION PROTOCOL SNAPSHOT");auto protocol=db.execute_shared_batch(sid,{"SELECT id FROM t"});check(protocol["error"]["code"]=="BATCH_TRANSACTION_UNSUPPORTED","unsupported protocol rejected");auto snapshot=ok(db,sid,"BEGIN");check(snapshot["transaction_options"]["protocol"]=="SNAPSHOT","rejection preserves pending config");ok(db,sid,"ROLLBACK");
 ok(db,sid,"SET LOCK GRANULARITY RECORD");check(db.execute_shared_batch(sid,{"SELECT id FROM t"})["error"]["code"]=="BATCH_TRANSACTION_UNSUPPORTED","record mode rejected");ok(db,sid,"SET LOCK GRANULARITY TABLE");
 ok(db,sid,"CREATE TABLE empty(id INTEGER)");twin({"SELECT COUNT(*) FROM empty","SELECT id FROM empty WHERE id IS NULL"});
 ok(db,sid,"CREATE TABLE other(id INTEGER)");check(db.execute_shared_batch(sid,{"SELECT id FROM t","SELECT id FROM other"})["error"]["code"]=="BATCH_QUERY_UNSUPPORTED","different tables rejected");
 ok(db,sid,"CREATE MATERIALIZED VIEW mv AS SELECT id,s FROM t");auto base=twin({"SELECT id,s FROM t","SELECT id,s FROM t ORDER BY id LIMIT 2"});check(base["metrics"]["shared_scan"]["base_scan_opens"]==1,"MV cannot replace common base scan");check(db.execute_shared_batch(sid,{"SELECT id FROM mv"})["error"]["code"]=="BATCH_QUERY_UNSUPPORTED","MV source excluded");
 check(ok(db,sid,"VERIFY")["verification"]["ok"]==true,"database invariants");db.close_session(sid);
 {Environment limit("EDUDB_QUERY_MATERIALIZED_BYTE_LIMIT","600");auto smallpath=path/"预算独立库";edudb::Engine small(smallpath.u8string(),8);auto ss=small.open_session();ok(small,ss,"CREATE TABLE q(id INTEGER,v VARCHAR(200))");ok(small,ss,"INSERT INTO q VALUES(1,'中文'),(2,'中文'),(3,'中文'),(4,'中文'),(5,'中文')");auto failure=small.execute_shared_batch(ss,{"SELECT id FROM q","SELECT COUNT(*) FROM q"});check(failure["error"]["code"]=="QUERY_LIMIT_EXCEEDED"&&failure["error"]["phase"]=="base_scan"&&failure["transaction_state"]=="idle","cumulative row allocation estimate rejects shared spool safely");check(temp_files(smallpath)==0,"quota failure removes partially written spool");ok(small,ss,"SELECT 42");small.close_session(ss);}
 for(const auto& kind:{"header","frame"}){auto crcpath=path/std::filesystem::u8path(std::string("校验损坏_")+kind);std::filesystem::create_directory(crcpath);{edudb::QuerySpillStore store(crcpath.u8string(),std::string(32,'0'));auto writer=store.create();writer->append(json::array({1,"中文",nullptr}));auto run=writer->finish();writer.reset();std::filesystem::path owned;for(const auto& e:std::filesystem::directory_iterator(crcpath))if(e.path().filename().u8string().rfind("edudb-query-",0)==0)owned=e.path();check(!owned.empty(),"actual owned CRC stream exists");std::fstream file(owned,std::ios::binary|std::ios::in|std::ios::out);const auto offset=std::string(kind)=="header"?0:104;file.seekg(offset);char byte=0;file.read(&byte,1);byte^=1;file.seekp(offset);file.write(&byte,1);file.close();bool rejected=false;try{auto reader=store.open(run);json value;reader->next(value);}catch(const edudb::DbError& error){rejected=error.code=="QUERY_SPILL_CORRUPT";}check(rejected,"shared reader primitive rejects actual "+std::string(kind)+" corruption");}check(temp_files(crcpath)==0,"CRC error RAII removes only owned run");}
 std::cout<<"PASS "<<checks<<" actual same-transaction one-base-scan/CRC-spool/native bounds/error cleanup checks\n";return 0;
 }catch(const std::exception& e){std::cerr<<"FAIL after "<<checks<<": "<<e.what()<<"\nDATA "<<path.u8string()<<"\n";return 1;}
}
