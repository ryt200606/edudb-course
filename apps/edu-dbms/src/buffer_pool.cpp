#include "buffer_pool.hpp"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace edudb::buffer {
const char* policy_name(Policy p) noexcept {
    switch(p){case Policy::Lru:return "LRU";case Policy::Mru:return "MRU";case Policy::Clock:return "CLOCK";case Policy::TossImmediate:return "TOSS_IMMEDIATE";}
    return "INVALID";
}
namespace {
void validate(Config c){
    if(c.read_ahead_pages>16||c.wait_ms>5000||std::string(policy_name(c.policy))=="INVALID")
        throw Error("INVALID_CONFIG","Buffer read-ahead must be 0..16, wait 0..5000ms, and policy LRU/MRU/CLOCK/TOSS_IMMEDIATE");
}
}
struct BufferPool::State {
    struct Frame {
        PageId id=0;std::shared_ptr<const Page> image;std::size_t pins=0;
        bool dirty=false,referenced=false,prefetched=false,toss=false,toss_deferred=false,reserved=false;
        std::list<PageId>::iterator recency,ring;
    };
    mutable std::mutex mutex;std::condition_variable cv;
    std::size_t capacity;Loader loader;Flusher flusher;Config config;Statistics stats;
    std::unordered_map<PageId,std::shared_ptr<Frame>> frames;
    std::list<PageId> recency,ring;std::list<PageId>::iterator hand;
    bool closed=false;std::uint64_t reservation=0,next_reservation=1;PageId workspace=0;bool has_workspace=false;
    State(std::size_t c,Loader l,Flusher f,Config cfg):capacity(c),loader(std::move(l)),flusher(std::move(f)),config(cfg),hand(ring.end()){
        if(c<2||c>1000000||!loader||!flusher)throw Error("INVALID_CONFIG","A buffer pool needs 2..1000000 frames and loader/flusher callbacks");
        validate(config);stats.max_live_guards=c*4;
    }
    void require_open()const{if(closed)throw Error("BUFFER_CLOSED","Buffer pool is closed");}
    std::size_t occupied()const{return frames.size()+((reservation&&!has_workspace)?1:0);}
    bool eligible(const std::shared_ptr<Frame>& frame)const{return !frame->pins&&!frame->reserved;}
    std::shared_ptr<Frame> victim(){
        // A proven last-use hint overrides the default replacement policy.
        for(auto i=recency.rbegin();i!=recency.rend();++i){auto f=frames.at(*i);if(eligible(f)&&f->toss)return f;}
        if(config.policy==Policy::Lru||config.policy==Policy::TossImmediate){for(auto i=recency.rbegin();i!=recency.rend();++i)if(eligible(frames.at(*i)))return frames.at(*i);}
        else if(config.policy==Policy::Mru){for(auto id:recency)if(eligible(frames.at(id)))return frames.at(id);}
        else if(!ring.empty()){
            if(hand==ring.end())hand=ring.begin();
            for(std::size_t n=0;n<2*ring.size();++n){auto i=hand;if(++hand==ring.end())hand=ring.begin();auto f=frames.at(*i);++stats.clock_scans;
                if(!eligible(f))continue;
                if(f->referenced){f->referenced=false;++stats.clock_second_chances;continue;}
                return f;
            }
        }
        return {};
    }
    void flush(const std::shared_ptr<Frame>& f,bool publication=false){
        if(!f->dirty)return;
        try{flusher(f->id,*f->image);}catch(...){++stats.io_failures;throw;}
        f->dirty=false;++stats.dirty_flushes;if(publication)++stats.publication_flushes;
    }
    void erase(const std::shared_ptr<Frame>& f,bool count_eviction=true){
        if(f->pins)throw Error("BUFFER_INTERNAL","Attempted to evict a pinned frame");
        if(f->dirty)throw Error("BUFFER_INTERNAL","Attempted to erase an unflushed dirty frame");
        if(f->prefetched)++stats.prefetch_wasted;
        recency.erase(f->recency);
        if(hand==f->ring){++hand;if(hand==ring.end())hand=ring.begin();}
        ring.erase(f->ring);if(ring.empty())hand=ring.end();
        frames.erase(f->id);if(count_eviction)++stats.evictions;
    }
    bool make_slot(){
        if(occupied()<capacity)return true;
        auto f=victim();if(!f)return false;flush(f);erase(f);return true;
    }
    bool await_slot(std::unique_lock<std::mutex>& lock,unsigned ms){
        const auto end=std::chrono::steady_clock::now()+std::chrono::milliseconds(ms);
        while(!make_slot()){
            if(!ms||cv.wait_until(lock,end)==std::cv_status::timeout)return false;
            require_open();
        }
        return true;
    }
    std::shared_ptr<Frame> insert(PageId id,std::shared_ptr<const Page> image,bool dirty,bool prefetched,bool reserved){
        if(frames.count(id))throw Error("BUFFER_INTERNAL","Duplicate resident page insertion");
        auto frame=std::make_shared<Frame>();frame->id=id;frame->image=std::move(image);frame->dirty=dirty;frame->prefetched=prefetched;frame->reserved=reserved;
        recency.push_front(id);frame->recency=recency.begin();
        try{ring.push_back(id);}catch(...){recency.erase(frame->recency);throw;}
        frame->ring=std::prev(ring.end());
        try{frames.emplace(id,frame);}catch(...){recency.erase(frame->recency);ring.erase(frame->ring);throw;}
        if(hand==ring.end())hand=ring.begin();
        return frame;
    }
    void touch(const std::shared_ptr<Frame>& f,Hint hint){
        recency.splice(recency.begin(),recency,f->recency);f->referenced=true;
        if(f->prefetched){f->prefetched=false;++stats.prefetch_useful;}
        if(hint==Hint::TossImmediate||config.policy==Policy::TossImmediate)f->toss=true;
    }
    void release(const std::shared_ptr<Frame>& f)noexcept{
        try{std::lock_guard<std::mutex> lock(mutex);
            if(f->pins){--f->pins;--stats.live_guards;++stats.guard_releases;}
            if(!closed&&!f->pins&&f->toss&&!f->reserved){
                if(!f->dirty){erase(f);++stats.toss_immediate;}
                else if(!f->toss_deferred){f->toss_deferred=true;++stats.toss_dirty_deferred;}
            }
            cv.notify_all();
        }catch(...){/* A guard destructor performs no I/O and never throws. */}
    }
    void release_publication(std::uint64_t token)noexcept{
        try{std::lock_guard<std::mutex> lock(mutex);if(reservation!=token)return;
            if(has_workspace){auto found=frames.find(workspace);if(found!=frames.end())found->second->reserved=false;}
            reservation=0;has_workspace=false;workspace=0;cv.notify_all();
        }catch(...){/* Metadata-only noexcept release, never a data flush. */}
    }
};
struct BufferPool::Guard::Lease {
    std::shared_ptr<State> state;std::shared_ptr<State::Frame> frame;std::shared_ptr<const Page> image;
    ~Lease(){image.reset();state->release(frame);}
};
BufferPool::Guard::Guard()noexcept=default;
BufferPool::Guard::Guard(std::unique_ptr<Lease> lease)noexcept:lease_(std::move(lease)){}
BufferPool::Guard::~Guard()=default;
BufferPool::Guard::Guard(Guard&&)noexcept=default;
BufferPool::Guard& BufferPool::Guard::operator=(Guard&&)noexcept=default;
BufferPool::Guard::operator bool()const noexcept{return bool(lease_);}
const Page& BufferPool::Guard::image()const{if(!lease_)throw Error("BUFFER_GUARD_EMPTY","Guard is empty");return *lease_->image;}
PageId BufferPool::Guard::id()const{if(!lease_)throw Error("BUFFER_GUARD_EMPTY","Guard is empty");return lease_->frame->id;}
void BufferPool::Guard::reset()noexcept{lease_.reset();}
struct BufferPool::Publication::Lease {
    std::shared_ptr<State> state;std::uint64_t token=0;
    ~Lease(){state->release_publication(token);}
};
BufferPool::Publication::Publication()noexcept=default;
BufferPool::Publication::Publication(std::unique_ptr<Lease> lease)noexcept:lease_(std::move(lease)){}
BufferPool::Publication::~Publication()=default;
BufferPool::Publication::Publication(Publication&&)noexcept=default;
BufferPool::Publication& BufferPool::Publication::operator=(Publication&&)noexcept=default;
BufferPool::Publication::operator bool()const noexcept{return bool(lease_);}
void BufferPool::Publication::finish()noexcept{lease_.reset();}
void BufferPool::Publication::publish(PageId id,Page image,bool dirty){
    if(!lease_)throw Error("BUFFER_PUBLICATION_EMPTY","Publication is empty");
    auto page=std::make_shared<const Page>(std::move(image));auto& s=*lease_->state;std::lock_guard<std::mutex> lock(s.mutex);s.require_open();
    if(s.reservation!=lease_->token)throw Error("BUFFER_INTERNAL","Publication lease is no longer current");
    auto found=s.frames.find(id);
    if(found!=s.frames.end()){
        auto f=found->second;f->image=std::move(page);f->dirty=dirty;f->referenced=true;
        if(f->prefetched){f->prefetched=false;++s.stats.prefetch_wasted;}
        s.recency.splice(s.recency.begin(),s.recency,f->recency);
    }else{
        // No demand guard can acquire the reserved workspace. Reusing it
        // cannot fail due to pin pressure after the caller has forced WAL.
        if(s.has_workspace){auto f=s.frames.at(s.workspace);s.flush(f,true);s.erase(f);}
        s.insert(id,std::move(page),dirty,false,true);s.workspace=id;s.has_workspace=true;
    }
    ++s.stats.publication_pages;
}
BufferPool::BufferPool(std::size_t capacity,Loader loader,Flusher flusher,Config config):state_(std::make_shared<State>(capacity,std::move(loader),std::move(flusher),config)){}
BufferPool::~BufferPool(){close();}
BufferPool::Guard BufferPool::fetch(PageId id,Hint hint){
    auto s=state_;std::unique_lock<std::mutex> lock(s->mutex);s->require_open();
    if(s->stats.live_guards>=s->stats.max_live_guards)throw Error("BUFFER_PIN_LIMIT","Live buffer guards exceed the bounded pin budget");
    auto found=s->frames.find(id);bool missed=found==s->frames.end();
    // A transaction publication owns its staging frame exclusively. Do not
    // turn it into an ordinary demand pin between two page publications.
    if(!missed&&found->second->reserved){++s->stats.busy_reads;throw Error("BUFFER_BUSY","Requested page is being published");}
    if(missed){++s->stats.demand_misses;const auto wait=s->config.wait_ms;const auto end=std::chrono::steady_clock::now()+std::chrono::milliseconds(wait);
        for(;;){
            // On wakeup another reader may already have loaded this page.
            // Such a hit needs no extra frame, even if every frame is pinned.
            found=s->frames.find(id);if(found!=s->frames.end())break;
            if(s->make_slot()){
                Page image{};try{image=s->loader(id);}catch(...){++s->stats.io_failures;throw;}
                auto frame=s->insert(id,std::make_shared<const Page>(std::move(image)),false,false,false);found=s->frames.find(frame->id);break;
            }
            if(!wait||s->cv.wait_until(lock,end)==std::cv_status::timeout){
                found=s->frames.find(id);if(found!=s->frames.end())break;
                ++s->stats.busy_reads;throw Error("BUFFER_BUSY","Every replaceable frame is pinned or reserved");
            }
            s->require_open();
        }
    }else ++s->stats.demand_hits;
    s->require_open();
    if(found->second->reserved){++s->stats.busy_reads;throw Error("BUFFER_BUSY","Requested page is being published");}
    if(s->stats.live_guards>=s->stats.max_live_guards)throw Error("BUFFER_PIN_LIMIT","Live buffer guards exceed the bounded pin budget");
    auto frame=found->second;auto lease=std::make_unique<Guard::Lease>();lease->state=s;lease->frame=frame;lease->image=frame->image;
    ++frame->pins;++s->stats.guard_acquires;++s->stats.live_guards;s->stats.pin_high_water=std::max(s->stats.pin_high_water,s->stats.live_guards);s->touch(frame,hint);
    s->cv.notify_all();
    return Guard(std::move(lease));
}
BufferPool::Publication BufferPool::reserve_publication(unsigned wait_ms){
    if(wait_ms>5000)throw Error("INVALID_CONFIG","Publication wait must be 0..5000ms");
    auto s=state_;std::unique_lock<std::mutex> lock(s->mutex);s->require_open();
    if(s->reservation){++s->stats.publication_busy;throw Error("BUFFER_BUSY","Another publication already owns the reserved frame");}
    if(!s->await_slot(lock,wait_ms)){++s->stats.publication_busy;throw Error("BUFFER_BUSY","Cannot reserve a publication frame before WAL");}
    if(s->reservation){++s->stats.publication_busy;throw Error("BUFFER_BUSY","Another publication acquired the slot while waiting");}
    auto lease=std::make_unique<Publication::Lease>();lease->state=s;lease->token=s->next_reservation++;
    s->reservation=lease->token;s->has_workspace=false;++s->stats.publication_reservations;return Publication(std::move(lease));
}
void BufferPool::prefetch(const std::vector<PageId>& ids){
    auto s=state_;std::lock_guard<std::mutex> lock(s->mutex);s->require_open();
    for(std::size_t n=0;n<ids.size()&&n<s->config.read_ahead_pages;++n){++s->stats.prefetch_requested;
        if(s->frames.count(ids[n])){++s->stats.prefetch_already_resident;continue;}
        if(!s->make_slot()){++s->stats.prefetch_skipped_busy;continue;}
        Page image{};try{image=s->loader(ids[n]);}catch(...){++s->stats.io_failures;throw;}
        auto frame=s->insert(ids[n],std::make_shared<const Page>(std::move(image)),false,true,false);
        // An unused prefetched page is not a demand hit and starts cold.
        s->recency.splice(s->recency.end(),s->recency,frame->recency);++s->stats.prefetch_loaded;
    }
}
void BufferPool::flush_all(){
    auto s=state_;std::lock_guard<std::mutex> lock(s->mutex);s->require_open();std::vector<std::shared_ptr<State::Frame>> toss;
    for(auto& pair:s->frames){s->flush(pair.second);if(!pair.second->pins&&!pair.second->reserved&&pair.second->toss)toss.push_back(pair.second);}
    for(auto& frame:toss){s->erase(frame);++s->stats.toss_immediate;}s->cv.notify_all();
}
bool BufferPool::flush_one(){
    auto s=state_;std::lock_guard<std::mutex> lock(s->mutex);s->require_open();
    for(auto& pair:s->frames)if(pair.second->dirty){s->flush(pair.second);if(!pair.second->pins&&!pair.second->reserved&&pair.second->toss){s->erase(pair.second);++s->stats.toss_immediate;}return true;}
    return false;
}
void BufferPool::configure(Config config){validate(config);auto s=state_;std::lock_guard<std::mutex> lock(s->mutex);s->require_open();if(s->config.policy!=config.policy)++s->stats.policy_changes;s->config=config;}
Statistics BufferPool::statistics()const{
    auto s=state_;std::lock_guard<std::mutex> lock(s->mutex);auto out=s->stats;out.capacity=s->capacity;out.resident_frames=s->frames.size();out.config=s->config;out.publication_active=s->reservation!=0;
    for(const auto& pair:s->frames){out.dirty_frames+=pair.second->dirty;out.pinned_frames+=pair.second->pins!=0;}return out;
}
std::vector<FrameSnapshot> BufferPool::snapshot(std::size_t maximum)const{
    if(maximum>2048)throw Error("INVALID_CONFIG","At most 2048 diagnostic frame copies are allowed");
    auto s=state_;std::lock_guard<std::mutex> lock(s->mutex);std::vector<FrameSnapshot> out;out.reserve(std::min(maximum,s->frames.size()));
    for(auto id:s->ring){if(out.size()==maximum)break;auto f=s->frames.at(id);out.push_back({id,*f->image,f->pins,f->dirty,f->prefetched,f->toss,f->reserved});}return out;
}
void BufferPool::close()noexcept{
    auto s=state_;if(!s)return;try{std::lock_guard<std::mutex> lock(s->mutex);s->closed=true;s->frames.clear();s->recency.clear();s->ring.clear();s->hand=s->ring.end();s->reservation=0;s->has_workspace=false;s->cv.notify_all();}catch(...){}
}
} // namespace edudb::buffer
