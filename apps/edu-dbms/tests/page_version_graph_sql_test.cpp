#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
using namespace edudb;
namespace fs=std::filesystem;
int checks=0;
void check(bool yes,const char* why){++checks;if(!yes)throw std::runtime_error(why);}
json sql(Engine& engine,const std::string& session,const std::string& text){auto result=engine.execute(session,text);if(!result.value("ok",false))throw std::runtime_error(text+": "+result.dump());return result;}
std::uint64_t id(Engine& engine,const std::string& session){auto d=engine.diagnostics();for(const auto& item:d["sessions"])if(item["id"]==session)return item["tx_id"].get<std::uint64_t>();throw std::runtime_error("no active session");}
json graph(Engine& engine){return engine.diagnostics()["storage"]["transactions"]["page_version_graph"];}
bool edge(const json& g,std::uint64_t from,std::uint64_t to,const char* kind){for(const auto& e:g.at("edges"))if(e["from"]==from&&e["to"]==to&&e["kind"]==kind)return true;return false;}
void sql_cases(const fs::path& path){
 Engine db(path.u8string(),3);auto a=db.open_session(),b=db.open_session();
 sql(db,a,"CREATE TABLE x(id INTEGER PRIMARY KEY,v INTEGER)");sql(db,a,"CREATE TABLE y(id INTEGER PRIMARY KEY,v INTEGER)");sql(db,a,"INSERT INTO x VALUES(1,1)");sql(db,a,"INSERT INTO y VALUES(1,1)");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT");sql(db,b,"BEGIN PROTOCOL SNAPSHOT");auto ta=id(db,a),tb=id(db,b);
 for(const auto& s:{a,b}){sql(db,s,"SELECT v FROM x WHERE id=1");sql(db,s,"SELECT v FROM y WHERE id=1");}
 sql(db,a,"UPDATE x SET v=0 WHERE id=1");sql(db,b,"UPDATE y SET v=0 WHERE id=1");sql(db,a,"COMMIT");sql(db,b,"COMMIT");
 auto g=graph(db);check(edge(g,ta,tb,"RW")&&edge(g,tb,ta,"RW"),"actual SQL write-skew lacks reciprocal version anti-dependencies");check(!g["committed_cycle"].empty(),"actual SQL committed cycle not detected");check(!g["truncated"]&&!g["disabled"],"small SQL graph unexpectedly incomplete");
 check(sql(db,a,"SELECT v FROM x")["rows"][0][0]==0&&sql(db,a,"SELECT v FROM y")["rows"][0][0]==0,"test did not actually commit SI write skew");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT");sql(db,b,"BEGIN PROTOCOL SNAPSHOT");ta=id(db,a);tb=id(db,b);sql(db,b,"UPDATE x SET v=5 WHERE id=1");sql(db,b,"COMMIT");
 check(sql(db,a,"SELECT v FROM x WHERE id=1")["rows"][0][0]==0,"late read did not select an old physical version");sql(db,a,"COMMIT");g=graph(db);check(edge(g,ta,tb,"RW"),"late old-page read omitted committed successor");
 sql(db,a,"BEGIN");auto reader=id(db,a);sql(db,a,"SELECT v FROM x WHERE id=1");sql(db,a,"COMMIT");check(edge(graph(db),tb,reader,"WR"),"actual SQL read-from creator WR missing");
 sql(db,a,"BEGIN PROTOCOL SNAPSHOT");sql(db,b,"BEGIN PROTOCOL SNAPSHOT");ta=id(db,a);tb=id(db,b);sql(db,a,"SELECT v FROM y");sql(db,b,"UPDATE y SET v=8 WHERE id=1");sql(db,b,"COMMIT");sql(db,a,"ROLLBACK");
 g=graph(db);for(const auto& e:g["edges"])check(e["from"]!=ta&&e["to"]!=ta,"rolled-back actual SQL reader remains in graph");
 for(const auto& node:g["nodes"])check(node["tx"]!=ta,"aborted graph node retained as definite committed node");
 check(g["invalid_events"]==0,"integrated graph received invalid event order");
 std::cout<<json{{"case","actual_SQL_page_versions_and_SI_write_skew"},{"a",ta},{"b",tb},{"graph",g}}.dump()<<'\n';
}
void aries_case(const fs::path& path){StorageOptions opts;opts.recovery_mode="ARIES_PAGE";Engine db(path.u8string(),3,32u*1024u*1024u,opts);auto s=db.open_session();sql(db,s,"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)");sql(db,s,"INSERT INTO t VALUES(1,1)");sql(db,s,"BEGIN");auto writer=id(db,s);sql(db,s,"UPDATE t SET v=2 WHERE id=1");sql(db,s,"SELECT v FROM t");sql(db,s,"COMMIT");sql(db,s,"BEGIN");auto reader=id(db,s);sql(db,s,"SELECT v FROM t");sql(db,s,"COMMIT");auto g=graph(db);check(edge(g,writer,reader,"WR"),"ARIES actual committed creator not tracked");check(g["invalid_events"]==0&&!g["disabled"],"ARIES own dirty page incorrectly treated as foreign version");sql(db,s,"BEGIN");auto aborted=id(db,s);sql(db,s,"UPDATE t SET v=9 WHERE id=1");sql(db,s,"ROLLBACK");g=graph(db);for(const auto& node:g["nodes"])check(node["tx"]!=aborted,"ARIES full rollback falsely committed in graph");
 sql(db,s,"BEGIN");auto after_rollback=id(db,s);sql(db,s,"SELECT v FROM t");sql(db,s,"COMMIT");g=graph(db);check(edge(g,writer,after_rollback,"WR"),"CLR page lost original committed writer");check(!edge(g,after_rollback,writer,"RW"),"CLR page invented a false anti-dependency to its restored writer");
 sql(db,s,"BEGIN");auto savepoint_reader=id(db,s);sql(db,s,"SAVEPOINT restore");sql(db,s,"UPDATE t SET v=17 WHERE id=1");sql(db,s,"ROLLBACK TO restore");check(sql(db,s,"SELECT v FROM t")["rows"][0][0]==2,"savepoint did not restore original committed value");sql(db,s,"COMMIT");g=graph(db);check(edge(g,writer,savepoint_reader,"WR")&&!edge(g,savepoint_reader,writer,"RW"),"savepoint CLR version alias has wrong dependency direction");check(!g["disabled"]&&g["invalid_events"]==0,"rollback alias sequence disabled physical graph");
}
int main(){try{const auto root=fs::absolute(fs::path("page-graph-sql-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));sql_cases(root/"private");aries_case(root/"aries");std::cout<<"PASS real SQL page version graph "<<checks<<" checks\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
