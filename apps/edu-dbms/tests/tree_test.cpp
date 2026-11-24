#include "test_platform.hpp"
#include "edudb.hpp"
#include "tree_control.hpp"
#include <chrono>
#include <deque>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
using namespace edudb;namespace fs=std::filesystem;
static unsigned checks=0;static json events=json::array();static std::mutex event_mutex;
static void need(bool b,const std::string& m){++checks;if(!b)throw std::runtime_error(m);}
static json sql(Engine& e,const std::string& s,const std::string& q,bool ok=true){auto r=e.execute(s,q);{std::lock_guard<std::mutex> g(event_mutex);events.push_back({{"session",s},{"sql",q},{"response",r}});}if(ok)need(r.value("ok",false),q+" => "+r.value("error",json()).dump());return r;}
static void error(Engine& e,const std::string& s,const std::string& q,const std::string& code){auto r=sql(e,s,q,false);need(!r.value("ok",true)&&r.at("error").at("code")==code,q+" requires "+code);}
static std::uint64_t open(Engine& e,const std::string& s,const std::string& table="t"){auto id=sql(e,s,"BEGIN PROTOCOL TREE").at("tx_id").get<std::uint64_t>();sql(e,s,"TREE OPEN "+table+" PARENT parent");return id;}
static void value(Engine& e,const std::string& s,int id,int expected){auto r=sql(e,s,"SELECT v FROM t WHERE id="+std::to_string(id));need(r.at("rows")==json::array({json::array({expected})}),"actual guarded node value");need(r.at("locking").at("effective")=="TREE_NODE_X","actual TREE plan");}
static void no_table(const json& j,std::uint64_t id){bool node=false;for(auto& l:j.at("locks"))if(l.at("tx_id")==id){auto n=l.at("resource").get<std::string>();need(n.rfind("table:",0)!=0&&n.rfind("cluster:",0)!=0,"no hidden table/cluster S");if(n.rfind("tree:",0)==0){need(l.at("mode")=="X","actual node X");node=true;}}need(node,"actual node grant exists");}
static void waiting(Engine& e,std::uint64_t id,const std::string& node){auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);while(std::chrono::steady_clock::now()<deadline){auto d=e.diagnostics();for(auto& q:d.at("queues"))if(q.at("tx_id")==id&&q.at("resource")==node){std::lock_guard<std::mutex> g(event_mutex);events.push_back({{"actual_wait",d}});return;}std::this_thread::sleep_for(std::chrono::milliseconds(3));}throw std::runtime_error("missing node wait "+node);}
static void finish(std::future<json>& f){need(f.wait_for(std::chrono::seconds(3))==std::future_status::ready,"actual release wakes waiter");need(f.get().value("ok",false),"waiter granted after release");}
static void write(const fs::path& p,const json& j){std::ofstream f(p,std::ios::binary|std::ios::trunc);f<<j.dump(2);f.flush();if(!f)throw std::runtime_error("receipt write");}
static void exhaustive(json& proof){
 auto topo=std::make_shared<LockTreeTopology>("small","id","parent",std::map<std::int64_t,std::optional<std::int64_t>>{{1,{}},{2,1},{3,1}});
 struct S{unsigned held[2]{},ever[2]{},wait[2]{},ended=0;};std::deque<S> todo(1);std::set<std::uint64_t> seen;unsigned transitions=0;
 auto code=[](const S& s){std::uint64_t k=s.ended;for(int t=0;t<2;++t){k=k*8+s.held[t];k=k*8+s.ever[t];k=k*4+s.wait[t];}return k;};
 while(!todo.empty()){auto s=todo.front();todo.pop_front();if(!seen.insert(code(s)).second)continue;need(seen.size()<200000,"bounded complete small-tree schedule search");need(!(s.held[0]&s.held[1]),"X ownership unique");
  auto owner=[&](unsigned n){unsigned bit=1u<<(n-1);return s.held[0]&bit?0:s.held[1]&bit?1:-1;};
  need(!(s.wait[0]&&s.wait[1]&&owner(s.wait[0])==1&&owner(s.wait[1])==0),"all legal small-tree interleavings have no wait cycle");
  for(int t=0;t<2;++t){if(s.ended&(1u<<t))continue;LockTreeState state;state.topology=topo;for(unsigned n=1;n<=3;++n){auto bit=1u<<(n-1);if(s.held[t]&bit)state.held.insert(n);else if(s.ever[t]&bit)state.released.insert(n);}
   if(s.wait[t]){if(owner(s.wait[t])==-1){state.before_lock(s.wait[t]);auto z=s;z.held[t]|=1u<<(s.wait[t]-1);z.ever[t]|=1u<<(s.wait[t]-1);z.wait[t]=0;todo.push_back(z);++transitions;}continue;}
   for(unsigned n=1;n<=3;++n){auto bit=1u<<(n-1);bool legal=!(s.ever[t]&bit)&&(!s.ever[t]||(n!=1&&(s.held[t]&1)));bool accepted=true;try{state.before_lock(n);}catch(const DbError&){accepted=false;}need(accepted==legal,"independent four-rule model matches node controller");if(legal){auto z=s;if(owner(n)==-1){z.held[t]|=bit;z.ever[t]|=bit;}else z.wait[t]=n;todo.push_back(z);++transitions;}if(s.held[t]&bit){state.before_unlock(n);auto z=s;z.held[t]&=~bit;todo.push_back(z);++transitions;}}
   auto z=s;z.held[t]=0;z.ended|=1u<<t;todo.push_back(z);++transitions;
  }
 }
 proof["exhaustive_small_tree"]={{"states",seen.size()},{"legal_transitions",transitions},{"transactions",2},{"nodes",3},{"wait_cycles",0},{"scope","complete finite acquisition/early-release/finish model with blocked requests; actual SQL/LockManager checked separately"}};
 LockTreeState written;written.topology=topo;written.before_lock(2);written.granted(2);written.access(2,true);bool denied=false;try{written.before_unlock(2);}catch(const DbError& e){denied=e.code=="TREE_WRITTEN_HELD";}need(denied,"written node cannot release early");
 for(auto edges:std::vector<std::map<std::int64_t,std::optional<std::int64_t>>>{{{1,{}},{2,9}},{{1,{}},{2,3},{3,2}},{{1,{}},{2,{}}}}){denied=false;try{LockTreeTopology invalid("bad","id","parent",edges);}catch(const DbError& e){denied=e.code=="TREE_TOPOLOGY";}need(denied,"invalid orphan/cycle/multiple-root topology rejected");}
}
static void lifecycle(const fs::path& dir,json& proof){
 // Destructor must discard private writes and all node grants.
 {Engine e((dir/"core").u8string(),8);auto s=e.open_session();open(e,s);sql(e,s,"TREE LOCK 2");sql(e,s,"UPDATE t SET v=999 WHERE id=2");}
 {Engine e((dir/"core").u8string(),8);auto s=e.open_session();open(e,s);sql(e,s,"TREE LOCK 2");value(e,s,2,22);sql(e,s,"COMMIT");e.close_session(s);}
 edudb_test::set_env("EDUDB_LOCK_TIMEOUT_MS","60");{Engine e((dir/"timeout").u8string(),8);auto s=e.open_session();sql(e,s,"CREATE TABLE t(id INTEGER PRIMARY KEY,parent INTEGER,v INTEGER)");sql(e,s,"INSERT INTO t VALUES(1,NULL,10),(2,1,20)");e.close_session(s);auto a=e.open_session(),b=e.open_session();open(e,a);sql(e,a,"TREE LOCK 2");open(e,b);error(e,b,"TREE LOCK 2","LOCK_TIMEOUT");proof["actual_node_timeout"]=e.diagnostics();sql(e,b,"ROLLBACK");sql(e,a,"COMMIT");e.close_session(a);e.close_session(b);}edudb_test::set_env("EDUDB_LOCK_TIMEOUT_MS","");
}
static void normal(const fs::path& path,json& proof){
 exhaustive(proof);Engine e(path.u8string(),8);auto setup=e.open_session();sql(e,setup,"CREATE TABLE t(id INTEGER PRIMARY KEY,parent INTEGER,v INTEGER)");sql(e,setup,"INSERT INTO t VALUES(1,NULL,10),(2,1,20),(3,1,30),(4,2,40),(5,2,50)");sql(e,setup,"CREATE TABLE bad(id INTEGER PRIMARY KEY,parent INTEGER,v INTEGER)");sql(e,setup,"INSERT INTO bad VALUES(1,NULL,10),(2,NULL,20)");e.close_session(setup);
 auto a=e.open_session(),b=e.open_session(),other=e.open_session();open(e,a);sql(e,a,"TREE LOCK 4");value(e,a,4,40);error(e,a,"TREE LOCK 5","TREE_PARENT_NOT_HELD");sql(e,a,"ROLLBACK");
 auto aid=open(e,a);sql(e,a,"TREE LOCK 1");sql(e,a,"TREE LOCK 2");sql(e,a,"TREE UNLOCK 1");auto bid=open(e,b);sql(e,b,"TREE LOCK 1");value(e,b,1,10);auto alias=sql(e,b,"SELECT b.v FROM t b WHERE b.id=1");need(alias.at("rows")==json::array({json::array({10})}),"guarded alias primary-key equality");sql(e,a,"TREE LOCK 4");auto blocked=std::async(std::launch::async,[&]{return sql(e,b,"TREE LOCK 2",false);});waiting(e,bid,"tree:t:2");no_table(e.diagnostics(),aid);sql(e,a,"TREE UNLOCK 2");finish(blocked);value(e,b,2,20);value(e,a,4,40);proof["real_early_release"]=e.diagnostics();sql(e,b,"COMMIT");sql(e,a,"COMMIT");
 open(e,a);sql(e,a,"TREE LOCK 1");sql(e,a,"TREE UNLOCK 1");error(e,a,"TREE LOCK 1","TREE_REACQUIRE");sql(e,a,"ROLLBACK");open(e,a);error(e,a,"SELECT v FROM t WHERE id=1","TREE_NOT_LOCKED");sql(e,a,"ROLLBACK");
 open(e,a);sql(e,a,"TREE LOCK 2");sql(e,a,"UPDATE t SET v=99 WHERE id=2");error(e,a,"TREE UNLOCK 2","TREE_WRITTEN_HELD");sql(e,a,"ROLLBACK");
 aid=open(e,a);sql(e,a,"TREE LOCK 2");sql(e,a,"UPDATE t SET v=21 WHERE id=2");bid=open(e,b);sql(e,b,"TREE LOCK 3");value(e,b,3,30);no_table(e.diagnostics(),aid);no_table(e.diagnostics(),bid);sql(e,a,"COMMIT");value(e,b,3,30);sql(e,b,"UPDATE t SET v=31 WHERE id=3");sql(e,b,"UPDATE t SET v=30 WHERE id=3");sql(e,b,"COMMIT");
 open(e,a);sql(e,a,"TREE LOCK 2");sql(e,a,"UPDATE t SET v=22 WHERE id=2");open(e,b);sql(e,b,"TREE LOCK 3");sql(e,b,"UPDATE t SET v=31 WHERE id=3");sql(e,a,"COMMIT");error(e,b,"COMMIT","SERIALIZATION_FAILURE");sql(e,b,"ROLLBACK");
 open(e,a);sql(e,a,"TREE LOCK 2");sql(e,a,"UPDATE t SET v=100 WHERE id=2");bid=open(e,b);auto closewait=std::async(std::launch::async,[&]{return sql(e,b,"TREE LOCK 2",false);});waiting(e,bid,"tree:t:2");e.close_session(a);finish(closewait);value(e,b,2,22);sql(e,b,"COMMIT");a=e.open_session();
 for(const auto& q:std::vector<std::string>{"INSERT INTO t VALUES(6,1,60)","DELETE FROM t WHERE id=1","UPDATE t SET parent=2 WHERE id=1","UPDATE t SET id=8 WHERE id=1","SELECT v FROM t WHERE id>=1","SELECT a.v FROM t a JOIN t b ON a.id=b.id WHERE a.id=1","SAVEPOINT forbidden"}){open(e,a);sql(e,a,"TREE LOCK 1");error(e,a,q,"TREE_UNSUPPORTED");sql(e,a,"ROLLBACK");}
 open(e,a);error(e,other,"BEGIN PROTOCOL STRICT_2PL","PROTOCOL_MODE_BUSY");sql(e,a,"ROLLBACK");sql(e,a,"BEGIN PROTOCOL TREE");error(e,a,"TREE OPEN bad PARENT parent","TREE_TOPOLOGY");sql(e,a,"ROLLBACK");
 sql(e,a,"SET TRANSACTION PROTOCOL TREE");auto imported=e.import_data(a,{{"format","csv"},{"table","t"},{"content","id,parent,v\n6,1,60\n"}});need(!imported["ok"]&&imported["error"]["code"]=="TREE_UNSUPPORTED","CSV cannot bypass TREE guard");sql(e,a,"SET TRANSACTION PROTOCOL TREE");auto exported=e.export_data(a,{{"format","csv"},{"table","t"}});need(!exported["ok"]&&exported["error"]["code"]=="TREE_UNSUPPORTED","unrestricted export cannot bypass node access");
 open(e,a);sql(e,a,"TREE LOCK 2");value(e,a,2,22);sql(e,a,"COMMIT");for(auto& s:std::vector<std::string>{a,b,other})e.close_session(s);auto verify=e.open_session();sql(e,verify,"VERIFY");e.close_session(verify);
}
int main(int argc,char** argv){fs::path dir=argc>1?fs::u8path(argv[1]):fs::temp_directory_path()/fs::path("edudb-tree-"+std::to_string(edudb_test::process_id()));std::string mode=argc>2?argv[2]:"normal";auto receipt=argc>3?fs::u8path(argv[3]):dir/"result.json";json proof={{"mode",mode},{"scope","isolated TREE static logical row topology; early release only unwritten nodes, written nodes held to END"}};try{
 fs::create_directories(dir);if(mode=="normal"){need(!fs::exists(dir/"core"),"fresh directory");normal(dir/"core",proof);lifecycle(dir,proof);{StorageOptions o;o.recovery_mode="ARIES_PAGE";Engine e((dir/"aries-reject").u8string(),8,32u*1024u*1024u,o);auto s=e.open_session();error(e,s,"BEGIN PROTOCOL TREE","ARIES_PROTOCOL");e.close_session(s);}}
 else if(mode=="hold-commit"||mode=="hold-loser"){Engine e((dir/"core").u8string(),8);auto s=e.open_session();open(e,s);sql(e,s,"TREE LOCK 1");sql(e,s,"UPDATE t SET v=77 WHERE id=1");if(mode=="hold-commit")sql(e,s,"COMMIT");proof["pid"]=edudb_test::process_id();proof["status"]="READY_FOR_EXTERNAL_TERMINATE";proof["events"]=events;write(receipt,proof);for(unsigned i=0;i<120;++i)std::this_thread::sleep_for(std::chrono::seconds(1));throw std::runtime_error("external kill missing");}
 else if(mode=="recover-commit"||mode=="recover-loser"){Engine e((dir/"core").u8string(),8);proof["startup"]=e.diagnostics();auto s=e.open_session();open(e,s);sql(e,s,"TREE LOCK 1");value(e,s,1,mode=="recover-commit"?77:10);sql(e,s,"COMMIT");open(e,s);sql(e,s,"TREE LOCK 2");value(e,s,2,22);sql(e,s,"COMMIT");e.close_session(s);s=e.open_session();sql(e,s,"VERIFY");e.close_session(s);}else throw std::runtime_error("unknown mode");
 proof["status"]="PASS";proof["checks"]=checks;proof["events"]=events;write(receipt,proof);std::cout<<"TREE_TEST_PASS checks="<<checks<<"\n";return 0;
 }catch(const std::exception& ex){proof["status"]="FAIL";proof["error"]=ex.what();proof["checks"]=checks;proof["events"]=events;try{write(receipt,proof);}catch(...){}std::cerr<<ex.what()<<"\n";return 1;}}
