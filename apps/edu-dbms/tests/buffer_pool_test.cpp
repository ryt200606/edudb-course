#include "buffer_pool.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <thread>

using namespace edudb::buffer;
namespace fs=std::filesystem;
namespace {
std::size_t checks=0;
void require(bool b,const char* m){++checks;if(!b)throw std::runtime_error(m);}
template<class F>void rejects(F fn,const char* code){bool caught=false;try{fn();}catch(const Error& e){caught=e.code==code;}require(caught,"Expected precise buffer error");}
Page page(PageId id,unsigned char value=0,std::uint64_t lsn=0){Page p{};for(unsigned i=0;i<8;++i){p[i]=static_cast<unsigned char>(id>>(8*i));p[8+i]=static_cast<unsigned char>(lsn>>(8*i));}std::fill(p.begin()+16,p.end(),value);return p;}
std::uint64_t lsn(const Page& p){std::uint64_t n=0;for(int i=7;i>=0;--i)n=(n<<8)|p[8+i];return n;}
struct Store {
    std::map<PageId,Page> disk;std::uint64_t loads=0,flushes=0,durable=0;std::vector<PageId> flushed;
    Store(){for(PageId i=0;i<100;++i)disk[i]=page(i,static_cast<unsigned char>(i));}
    Page load(PageId id){++loads;if(!disk.count(id))throw Error("MISSING_PAGE","Missing fixture page");return disk.at(id);}
    void flush(PageId id,const Page& p){if(lsn(p)>durable)throw Error("WAL_ORDER","Page LSN exceeds durable WAL fixture");disk[id]=p;++flushes;flushed.push_back(id);}
    BufferPool make(std::size_t n,Config c={}){return BufferPool(n,[this](PageId id){return load(id);},[this](PageId id,const Page& p){flush(id,p);},c);}
};
std::set<PageId> resident(BufferPool& p){std::set<PageId> r;for(const auto& f:p.snapshot())r.insert(f.id);return r;}
void policies(){
    for(auto policy:{Policy::Lru,Policy::Mru,Policy::Clock}){
        Store store;auto pool=store.make(3,{policy,0,0});for(auto id:{1,2,3,1,4})pool.fetch(id).reset();
        require(resident(pool)==(policy==Policy::Lru?std::set<PageId>{1,3,4}:policy==Policy::Mru?std::set<PageId>{2,3,4}:std::set<PageId>{2,3,4}),"Independent first victim for LRU/MRU/CLOCK");
        pool.fetch(5).reset();auto pin=pool.fetch(policy==Policy::Clock?3:policy==Policy::Lru?1:5);pool.fetch(6).reset();
        require(resident(pool)==(policy==Policy::Lru?std::set<PageId>{1,5,6}:policy==Policy::Mru?std::set<PageId>{2,5,6}:std::set<PageId>{3,5,6}),"Replacement must skip pinned frame");
        auto s=pool.statistics();require(s.pinned_frames==1&&s.live_guards==1&&s.resident_frames==3,"Pin/resident accounting");
        if(policy==Policy::Clock)require(s.clock_second_chances==5,"Hand-calculated CLOCK second chances");
        pin.reset();require(pool.statistics().guard_acquires==pool.statistics().guard_releases,"Balanced guard lifetime");
    }
    for(auto policy:{Policy::Lru,Policy::Mru}){
        Store store;auto pool=store.make(7,{policy,0,0});std::vector<PageId> model;std::mt19937 random(10103);
        for(int i=0;i<3000;++i){PageId id=random()%29;auto found=std::find(model.begin(),model.end(),id);
            if(found!=model.end())model.erase(found);else if(model.size()==7)model.erase(policy==Policy::Lru?model.begin():std::prev(model.end()));
            model.push_back(id);pool.fetch(id).reset();require(resident(pool)==std::set<PageId>(model.begin(),model.end()),"Seeded independent ordered replacement model");
        }
    }
}
void pins_and_wait(){
    Store store;auto pool=store.make(2);auto a=pool.fetch(1),b=pool.fetch(2);
    rejects([&]{pool.fetch(3);},"BUFFER_BUSY");rejects([&]{pool.reserve_publication();},"BUFFER_BUSY");
    require(store.loads==2&&store.flushes==0,"All-pinned rejection happens before loader/flusher or WAL work");
    pool.configure({Policy::Lru,0,1000});std::promise<void> start;auto ready=start.get_future();
    auto waiting=std::async(std::launch::async,[&]{start.set_value();return pool.fetch(3);});ready.wait();
    require(waiting.wait_for(std::chrono::milliseconds(20))==std::future_status::timeout,"All-pinned demand cannot complete while pins remain");
    a.reset();auto c=waiting.get();require(c.id()==3&&b.image()[16]==2,"Release wakes bounded waiter and preserves other pin");
    BufferPool::Guard moved=std::move(c);require(!c&&bool(moved),"Move transfers the one RAII pin");moved.reset();b.reset();
    require(pool.statistics().live_guards==0&&pool.statistics().guard_acquires==pool.statistics().guard_releases,"Moved/reset guards unpin exactly once");
    std::vector<BufferPool::Guard> many;for(int i=0;i<8;++i)many.push_back(pool.fetch(3));rejects([&]{pool.fetch(3);},"BUFFER_PIN_LIMIT");
    require(pool.statistics().pin_high_water==8,"Bounded guard budget counts actual guards");many.clear();
    try{auto scoped=pool.fetch(3);throw std::runtime_error("scope unwind");}catch(const std::runtime_error&){}
    require(pool.statistics().live_guards==0,"Exception scope releases its pin");
    a=pool.fetch(1);b=pool.fetch(2);
    std::promise<void> started1,started2;auto signal1=started1.get_future(),signal2=started2.get_future();
    auto same1=std::async(std::launch::async,[&]{started1.set_value();return pool.fetch(7);});auto same2=std::async(std::launch::async,[&]{started2.set_value();return pool.fetch(7);});signal1.wait();signal2.wait();
    require(same1.wait_for(std::chrono::milliseconds(20))==std::future_status::timeout&&same2.wait_for(std::chrono::milliseconds(20))==std::future_status::timeout,"Same-page waiters both respect existing real pins");auto loads=store.loads;
    a.reset();auto one=same1.get(),two=same2.get();require(one.id()==7&&two.id()==7&&store.loads==loads+1,"Concurrent same-page waiters share the one loaded image");one.reset();two.reset();b.reset();
}
void publication(){
    Store store;auto pool=store.make(2);auto old=pool.fetch(1);pool.fetch(2).reset();auto p=pool.reserve_publication();
    require(pool.statistics().publication_active&&pool.statistics().resident_frames==1,"Pre-WAL reservation really frees a frame");
    rejects([&]{pool.fetch(9);},"BUFFER_BUSY");store.durable=10;
    p.publish(3,page(3,103,10));rejects([&]{pool.fetch(3);},"BUFFER_BUSY");
    p.publish(4,page(4,104,10));p.publish(1,page(1,101,10));p.publish(5,page(5,105,10));
    require(old.image()[16]==1&&lsn(old.image())==0,"Pinned immutable image is not overwritten by commit publication");
    auto latest=pool.fetch(1);require(latest.image()[16]==101&&lsn(latest.image())==10,"A later guard sees the newly published image");
    require(pool.statistics().resident_frames==2&&pool.statistics().publication_flushes==2,"Many publications reuse one real reserved frame");p.finish();latest.reset();old.reset();pool.flush_all();
    for(auto id:{1,3,4,5})require(store.disk.at(id)[16]==100+id,"Every published page reaches the real flusher");
    auto stats=pool.statistics();require(stats.publication_pages==4&&stats.publication_reservations==1&&stats.dirty_flushes==4&&!stats.publication_active,"Publication accounting");
    // A flusher exception must propagate and leave dirty state intact. It is
    // the Storage adapter's responsibility to enter recovery-required mode.
    auto q=pool.reserve_publication();q.publish(8,page(8,88,20));rejects([&]{q.publish(9,page(9,99,20));},"WAL_ORDER");
    require(pool.statistics().io_failures==1&&pool.statistics().dirty_frames==1,"Failed flush does not clear dirty state or pretend rollback");store.durable=20;q.publish(9,page(9,99,20));q.finish();pool.flush_all();
}
void prefetch_and_toss(){
    Store store;auto pool=store.make(3,{Policy::Lru,2,0});pool.fetch(1).reset();pool.prefetch({2,3,4});
    require(store.loads==3&&pool.statistics().prefetch_loaded==2,"Read-ahead invokes real loader and honors configured window");pool.fetch(2).reset();pool.fetch(4).reset();
    require(pool.statistics().prefetch_useful==1&&pool.statistics().prefetch_wasted==1,"Useful and discarded unused prefetch counted independently");pool.prefetch({2});
    auto a=pool.fetch(1),b=pool.fetch(2),c=pool.fetch(4);pool.prefetch({5});
    auto stats=pool.statistics();require(stats.prefetch_requested==4&&stats.prefetch_loaded==2&&stats.prefetch_already_resident==1&&stats.prefetch_skipped_busy==1,"Prefetch does not wait or invent hits under pin pressure");a.reset();b.reset();c.reset();
    {auto toss=pool.fetch(7,Hint::TossImmediate);require(resident(pool).count(7)==1,"Toss page stays resident during actual pin");}
    require(!resident(pool).count(7)&&pool.statistics().toss_immediate==1,"Clean last-use frame is immediately discarded on unpin");
    auto dirty=pool.fetch(8,Hint::TossImmediate);auto pub=pool.reserve_publication();store.durable=1;pub.publish(8,page(8,88,1));pub.finish();dirty.reset();
    require(resident(pool).count(8)&&pool.statistics().toss_dirty_deferred==1,"Dirty last-use frame is not silently discarded in destructor");auto flushed=store.flushes;pool.flush_all();
    require(!resident(pool).count(8)&&store.flushes==flushed+1&&store.disk[8][16]==88,"Explicit checked flush completes dirty last-use toss");
}
void failures_and_close(){
    Store store;auto pool=store.make(2);rejects([&]{pool.fetch(999);},"MISSING_PAGE");require(pool.statistics().resident_frames==0&&pool.statistics().io_failures==1,"Loader failure cannot install an invalid frame");
    rejects([&]{pool.configure({Policy::Lru,17,0});},"INVALID_CONFIG");rejects([&]{pool.configure({Policy::Lru,0,5001});},"INVALID_CONFIG");rejects([&]{pool.snapshot(2049);},"INVALID_CONFIG");
    auto pin=pool.fetch(1);pool.configure({Policy::Clock,0,0});require(pool.statistics().policy_changes==1&&pin.image()[16]==1,"Policy switch preserves resident pinned content");
    pool.close();require(pin.image()[16]==1,"Guard image remains valid after pool closes");rejects([&]{pool.fetch(1);},"BUFFER_CLOSED");pin.reset();require(pool.statistics().live_guards==0,"Release after close is metadata-only and safe");
}
void file_io(){
    auto root=fs::temp_directory_path()/("edudb-buffer-stageA-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));fs::create_directory(root);std::ofstream(root/"owned-test.marker")<<"stageA";auto path=root/"pages.bin";
    {std::ofstream file(path,std::ios::binary);for(PageId id=0;id<6;++id){auto p=page(id,static_cast<unsigned char>(id));file.write(reinterpret_cast<const char*>(p.data()),p.size());}}
    std::uint64_t reads=0,writes=0;
    {BufferPool pool(2,[&](PageId id){std::ifstream file(path,std::ios::binary);file.seekg(static_cast<std::streamoff>(id*8192));Page p{};file.read(reinterpret_cast<char*>(p.data()),p.size());if(!file)throw std::runtime_error("native fixture read");++reads;return p;},[&](PageId id,const Page& p){std::fstream file(path,std::ios::binary|std::ios::in|std::ios::out);file.seekp(static_cast<std::streamoff>(id*8192));file.write(reinterpret_cast<const char*>(p.data()),p.size());file.flush();if(!file)throw std::runtime_error("native fixture write");++writes;});
        pool.fetch(1).reset();auto pub=pool.reserve_publication();pub.publish(2,page(2,102,1));pub.publish(3,page(3,103,1));pub.finish();pool.flush_all();pool.fetch(4).reset();
        require(reads==2&&writes==2,"Native file callback reads/writes are actual, not a cost model");}
    {std::ifstream file(path,std::ios::binary);Page p{};file.seekg(2*8192);file.read(reinterpret_cast<char*>(p.data()),p.size());require(p==page(2,102,1),"File bytes contain exact evicted publication image");file.read(reinterpret_cast<char*>(p.data()),p.size());require(p==page(3,103,1),"File bytes contain exact final publication image");}
    require(fs::equivalent(root.parent_path(),fs::temp_directory_path())&&root.filename().string().rfind("edudb-buffer-stageA-",0)==0&&fs::exists(root/"owned-test.marker"),"Bounded owned file fixture cleanup");fs::remove_all(root);
}
}
int main(){try{policies();pins_and_wait();publication();prefetch_and_toss();failures_and_close();file_io();std::cout<<"PASS buffer_pool StageA "<<checks<<" checks: pins, immutable publication, LRU/MRU/CLOCK/toss, bounded read-ahead, real file bytes\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL buffer_pool StageA: "<<e.what()<<"\n";return 1;}}
