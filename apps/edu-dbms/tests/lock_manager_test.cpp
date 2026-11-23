#include "lock_manager.hpp"
#include "edudb.hpp"
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>
using namespace edudb;
using namespace std::chrono_literals;
namespace {
void require(bool condition,const std::string& message){if(!condition)throw std::runtime_error(message);}
template<class F>void expect(const std::string& code,F f){try{f();}catch(const DbError& e){require(e.code==code,"expected "+code+", actual "+e.code+": "+e.what());return;}throw std::runtime_error("missing "+code);}
struct Gate {std::promise<void> promise;std::shared_future<void> future=promise.get_future().share();bool opened=false;void open(){if(!opened){opened=true;promise.set_value();}}bool wait(){return future.wait_for(2s)==std::future_status::ready;}};
struct Worker {std::future<std::string> result;std::future<void> acquired;};
Worker worker(LockManager& lm,std::uint64_t tx,std::string resource,LockMode mode,std::shared_ptr<Gate> hold={}){
 auto signal=std::make_shared<std::promise<void>>();auto acquired=signal->get_future();
 auto result=std::async(std::launch::async,[&,tx,resource,mode,signal,hold]{try{lm.acquire(tx,resource,mode);signal->set_value();if(hold&&!hold->wait())throw std::runtime_error("bounded test gate timed out");lm.release(tx);return std::string("GRANTED");}catch(const DbError& e){lm.release(tx);return e.code;}catch(const std::exception& e){lm.release(tx);return std::string("ERROR: ")+e.what();}});
 return {std::move(result),std::move(acquired)};
}
bool waiting(LockManager& lm,std::uint64_t tx,std::uint64_t blocker=0){auto snapshot=lm.snapshot();for(const auto& w:snapshot["waits"])if(w["tx_id"]==tx){if(!blocker)return true;for(const auto& b:w["blocked_by"])if(b==blocker)return true;}return false;}
template<class F>void until(F f,const std::string& message){auto end=std::chrono::steady_clock::now()+1500ms;while(std::chrono::steady_clock::now()<end){if(f())return;std::this_thread::sleep_for(1ms);}throw std::runtime_error(message);}
void granted(Worker& w,const std::string& context){require(w.acquired.wait_for(1500ms)==std::future_status::ready,context+" acquisition missing");}
void done(Worker& w,const std::string& expected){require(w.result.wait_for(4s)==std::future_status::ready,"bounded worker did not finish");auto result=w.result.get();require(result==expected,"worker expected "+expected+", actual "+result);}
void empty(LockManager& lm){auto s=lm.snapshot();require(s["locks"].empty()&&s["queues"].empty()&&s["waits"].empty()&&s["lock_transactions"].empty(),"lock lifecycle leaked state: "+s.dump());}
void compatibility(){
 const bool expected[5][5]={{1,1,1,1,0},{1,1,0,0,0},{1,0,1,0,0},{1,0,0,0,0},{0,0,0,0,0}};
 for(unsigned a=0;a<5;++a)for(unsigned b=0;b<5;++b)require(LockManager::compatible(static_cast<LockMode>(a),static_cast<LockMode>(b))==expected[a][b],"IS/IX/S/SIX/X compatibility mismatch");
 std::cout<<"PASS all25 multigranularity compatibility pairs\n";
}
void fifo_writer(){
 LockManager lm(3s);lm.acquire(1,"t",false);auto wh=std::make_shared<Gate>(),rh=std::make_shared<Gate>();auto writer=worker(lm,2,"t",LockMode::X,wh);until([&]{return waiting(lm,2,1);},"writer did not wait for reader");auto reader=worker(lm,3,"t",LockMode::S,rh);until([&]{return waiting(lm,3,2);},"later reader lacks queued-writer dependency");require(reader.acquired.wait_for(0ms)!=std::future_status::ready,"reader barged past writer");lm.release(1);granted(writer,"FIFO writer");require(reader.acquired.wait_for(0ms)!=std::future_status::ready,"reader ran during writer");wh->open();done(writer,"GRANTED");granted(reader,"following reader");rh->open();done(reader,"GRANTED");empty(lm);std::cout<<"PASS FIFO writer barrier includes queue-predecessor wait edge\n";
}
void shared_group(){
 LockManager lm(3s);lm.acquire(1,"t",true);auto hold=std::make_shared<Gate>();auto a=worker(lm,2,"t",LockMode::S,hold);until([&]{return waiting(lm,2);},"first group reader missing");auto b=worker(lm,3,"t",LockMode::S,hold);until([&]{return waiting(lm,3);},"second group reader missing");auto w=worker(lm,4,"t",LockMode::X);until([&]{return waiting(lm,4,2)&&waiting(lm,4,3);},"writer lacks shared-group predecessor edges");lm.release(1);granted(a,"reader group A");granted(b,"reader group B");require(w.acquired.wait_for(0ms)!=std::future_status::ready,"writer overlapped shared group");hold->open();done(a,"GRANTED");done(b,"GRANTED");done(w,"GRANTED");empty(lm);std::cout<<"PASS contiguous compatible reader group grants concurrently\n";
}
void upgrade(){
 LockManager lm(3s);lm.acquire(1,"t",false);lm.acquire(2,"t",false);auto hold=std::make_shared<Gate>();auto u=worker(lm,1,"t",LockMode::X,hold);until([&]{return waiting(lm,1,2);},"upgrade missing holder dependency");auto r=worker(lm,3,"t",LockMode::S);until([&]{return waiting(lm,3,1);},"upgrade failed to block later reader");lm.release(2);granted(u,"S to X upgrade");require(r.acquired.wait_for(0ms)!=std::future_status::ready,"reader overlapped upgrade");hold->open();done(u,"GRANTED");done(r,"GRANTED");empty(lm);std::cout<<"PASS upgrade retains S and blocks later readers without bypass\n";
}
void maximum_victim(){
 LockManager lm(3s);lm.acquire(1,"a",true);lm.acquire(2,"b",true);lm.acquire(3,"c",true);auto a=worker(lm,1,"b",LockMode::X);until([&]{return waiting(lm,1,2);},"cycle edge1");auto b=worker(lm,2,"c",LockMode::X);until([&]{return waiting(lm,2,3);},"cycle edge2");auto c=worker(lm,3,"a",LockMode::X);done(c,"DEADLOCK");done(b,"GRANTED");done(a,"GRANTED");bool exact=false;auto snapshot=lm.snapshot();for(const auto& e:snapshot["events"])if(e["kind"]=="deadlock_cycle"&&e["details"]["victim"]==3)exact=true;require(exact,"maximum participating transaction not recorded");empty(lm);std::cout<<"PASS three-way cycle chooses greatest participating transaction ID\n";
}
void queue_cycle(){
 LockManager lm(3s);lm.acquire(1,"a",false);lm.acquire(2,"b",true);auto c=worker(lm,3,"a",LockMode::X);until([&]{return waiting(lm,3,1);},"queued writer edge missing");auto a=worker(lm,1,"b",LockMode::S);until([&]{return waiting(lm,1,2);},"second queue-cycle edge missing");auto b=worker(lm,2,"a",LockMode::S);done(c,"DEADLOCK");done(b,"GRANTED");done(a,"GRANTED");empty(lm);std::cout<<"PASS deadlock closes through incompatible FIFO predecessor, not just holders\n";
}
void wait_die(){
 {LockManager lm(3s,DeadlockPolicy::WaitDie);lm.acquire(10,"a",true);expect("DEADLOCK",[&]{lm.acquire(20,"a",false);});lm.release(20);lm.release(10);empty(lm);}
 {LockManager lm(3s,DeadlockPolicy::WaitDie);lm.acquire(20,"a",true);auto old=worker(lm,10,"a",LockMode::X);until([&]{return waiting(lm,10,20);},"old requester should wait for young holder");lm.release(20);done(old,"GRANTED");empty(lm);}
 {LockManager lm(3s,DeadlockPolicy::WaitDie);lm.register_transaction(10,100);lm.register_transaction(20,100);lm.acquire(10,"a",true);expect("DEADLOCK",[&]{lm.acquire(20,"a",false);});lm.release(20);lm.release(10);empty(lm);}
 std::cout<<"PASS wait-die older waits/younger aborts with deterministic timestamp tie-break\n";
}
void wound_wait(){
 {LockManager lm(3s,DeadlockPolicy::WoundWait);lm.acquire(20,"a",true);auto old=worker(lm,10,"a",LockMode::X);until([&]{auto snapshot=lm.snapshot();for(const auto& t:snapshot["lock_transactions"])if(t["tx_id"]==20&&t["cancelled"].get<bool>())return true;return false;},"older requester did not wound young owner");expect("DEADLOCK",[&]{auto p=lm.prepare_commit(20);(void)p;});lm.release(20);done(old,"GRANTED");empty(lm);}
 {LockManager lm(3s,DeadlockPolicy::WoundWait);lm.acquire(10,"a",true);auto young=worker(lm,20,"a",LockMode::S);until([&]{return waiting(lm,20,10);},"young requester must wait");lm.check_cancelled(10);lm.release(10);done(young,"GRANTED");empty(lm);}
 std::cout<<"PASS wound-wait cancellation stops commit and does not release live owner's locks\n";
}
void commit_gate(){
 LockManager lm(3s,DeadlockPolicy::WoundWait);lm.acquire(20,"a",true);auto gate=std::make_shared<Gate>(),release=std::make_shared<Gate>();std::promise<void> entered,committed;auto e=entered.get_future(),c=committed.get_future();std::atomic<bool> durable{false};
 auto young=std::async(std::launch::async,[&]{try{{auto permit=lm.prepare_commit(20);entered.set_value();require(gate->wait(),"commit gate signal timed out");durable=true;lm.commit_succeeded(permit);}committed.set_value();require(release->wait(),"commit release signal timed out");lm.release(20);return std::string("COMMITTED");}catch(const std::exception& x){lm.release(20);return std::string("ERROR: ")+x.what();}});
 require(e.wait_for(1500ms)==std::future_status::ready,"commit permit not entered");auto older=worker(lm,10,"a",LockMode::X);gate->open();require(c.wait_for(1500ms)==std::future_status::ready,"durable confirmation missing");until([&]{return waiting(lm,10,20);},"older requester should wait for terminal holder");lm.check_cancelled(20);require(durable,"simulated durable action missing");release->open();require(young.get()=="COMMITTED","commit callback failed");done(older,"GRANTED");empty(lm);
 {lm.acquire(30,"b",true);{auto unknown=lm.prepare_commit(30);}bool labelled=false;auto unknown_snapshot=lm.snapshot();for(const auto& t:unknown_snapshot["lock_transactions"])if(t["tx_id"]==30&&t["phase"]=="OUTCOME_UNKNOWN")labelled=true;require(labelled,"abandoned permit must say UNKNOWN, not COMMITTED/ROLLBACK");lm.release(30);lm.acquire(31,"b",true);auto known=lm.prepare_commit(31);lm.commit_failed(known,false);bool failed=false;auto failed_snapshot=lm.snapshot();for(const auto& t:failed_snapshot["lock_transactions"])if(t["tx_id"]==31&&t["phase"]=="FAILED_BEFORE_DURABILITY")failed=true;require(failed,"pre-WAL failure classification lost");lm.release(31);lm.acquire(32,"b",true);lm.release(32);empty(lm);}
 std::cout<<"PASS commit permit serializes wounding with durable confirmation and classifies UNKNOWN separately\n";
}
void hierarchy(){
 LockManager lm(3s);lm.register_resource("db");lm.register_resource("table:t","db");lm.register_resource("page:1","table:t");lm.register_resource("row:1","page:1");lm.register_resource("row:2","page:1");
 expect("LOCK_HIERARCHY",[&]{lm.acquire(1,"row:1",LockMode::S);});lm.acquire_path(1,"row:1",LockMode::S);lm.acquire_path(2,"row:2",LockMode::X);bool is=false,ix=false;auto initial=lm.snapshot();for(const auto& h:initial["locks"]){if(h["resource"]=="table:t"&&h["tx_id"]==1&&h["mode"]=="IS")is=true;if(h["resource"]=="table:t"&&h["tx_id"]==2&&h["mode"]=="IX")ix=true;}require(is&&ix,"ancestor intention locks missing");require(!lm.acquire_path(3,"table:t",LockMode::S,true),"table S bypassed IX");lm.release(3);lm.release(2);lm.release(1);
 lm.acquire_path(4,"table:t",LockMode::S);lm.acquire_path(4,"row:1",LockMode::X);bool six=false;auto upgraded=lm.snapshot();for(const auto& h:upgraded["locks"])if(h["resource"]=="table:t"&&h["tx_id"]==4&&h["mode"]=="SIX")six=true;require(six,"S+IX did not upgrade to SIX");expect("LOCK_HIERARCHY",[&]{lm.register_resource("table:t","page:1");});lm.release(4);empty(lm);std::cout<<"PASS ancestor intention enforcement and real S plus IX to SIX upgrade\n";
}
void timeout_cleanup(){
 LockManager lm(40ms);lm.acquire(1,"a",true);require(!lm.acquire(2,"a",false,true),"nonblocking request should fail");require(lm.snapshot()["queues"].empty(),"nonblocking failure queued work");expect("LOCK_TIMEOUT",[&]{lm.acquire(2,"a",false);});require(lm.snapshot()["queues"].empty()&&lm.snapshot()["waits"].empty(),"timed-out request leaked graph edges");expect("DEADLOCK",[&]{auto p=lm.prepare_commit(2);(void)p;});lm.release(2);lm.release(1);lm.acquire(3,"a",false);lm.release(3);empty(lm);std::cout<<"PASS bounded timeout and nonblocking cleanup; timed-out transaction cannot commit\n";
}
void stress(){
 LockManager lm(3s);std::atomic<int> readers{0},writers{0},increments{0};std::atomic<bool> overlap{false};std::vector<std::future<void>> tasks;
 for(int thread=0;thread<8;++thread)tasks.push_back(std::async(std::launch::async,[&,thread]{for(int n=0;n<80;++n){auto tx=std::uint64_t(1000+thread*80+n);bool write=n%4==0;lm.acquire(tx,"shared",write);if(write){if(writers.fetch_add(1)!=0||readers.load()!=0)overlap=true;std::this_thread::yield();++increments;--writers;}else{++readers;if(writers.load()!=0)overlap=true;std::this_thread::yield();--readers;}lm.release(tx);}}));
 for(auto& task:tasks)task.get();
 require(!overlap&&increments==160,"stress S/X exclusion or updates failed");empty(lm);std::cout<<"PASS 640 real concurrent shared/exclusive lifecycles, no exclusion violation\n";
}
}
int main(){try{compatibility();fifo_writer();shared_group();upgrade();maximum_victim();queue_cycle();wait_die();wound_wait();commit_gate();hierarchy();timeout_cleanup();stress();std::cout<<"PASS independent stage2 lock_manager completed\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL stage2 lock_manager: "<<e.what()<<"\n";return 1;}}
