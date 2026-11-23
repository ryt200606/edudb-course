#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace edudb::buffer {
using PageId = std::uint64_t;
using Page = std::array<unsigned char,8192>;
enum class Policy { Lru, Mru, Clock, TossImmediate };
enum class Hint { Normal, Sequential, TossImmediate };
struct Config { Policy policy=Policy::Lru; unsigned read_ahead_pages=0,wait_ms=0; };
struct Error : std::runtime_error {
    std::string code;
    Error(std::string value,std::string message):std::runtime_error(std::move(message)),code(std::move(value)){}
};
struct Statistics {
    std::uint64_t demand_hits=0,demand_misses=0,evictions=0,dirty_flushes=0;
    std::uint64_t guard_acquires=0,guard_releases=0,live_guards=0,pin_high_water=0,busy_reads=0;
    std::uint64_t clock_scans=0,clock_second_chances=0,toss_immediate=0,toss_dirty_deferred=0;
    std::uint64_t prefetch_requested=0,prefetch_loaded=0,prefetch_already_resident=0;
    std::uint64_t prefetch_useful=0,prefetch_wasted=0,prefetch_skipped_busy=0;
    std::uint64_t publication_reservations=0,publication_busy=0,publication_pages=0,publication_flushes=0;
    std::uint64_t io_failures=0,policy_changes=0;
    std::size_t capacity=0,resident_frames=0,dirty_frames=0,pinned_frames=0,max_live_guards=0;
    bool publication_active=false;
    Config config;
};
struct FrameSnapshot {
    PageId id=0;Page image{};std::size_t pins=0;bool dirty=false,prefetched=false,toss=false,reserved=false;
};
class BufferPool {
    struct State;
public:
    using Loader=std::function<Page(PageId)>;
    using Flusher=std::function<void(PageId,const Page&)>;
    class Guard {
    public:
        Guard() noexcept;
        ~Guard();
        Guard(Guard&&) noexcept;
        Guard& operator=(Guard&&) noexcept;
        Guard(const Guard&)=delete;Guard& operator=(const Guard&)=delete;
        explicit operator bool() const noexcept;
        const Page& image() const;
        PageId id() const;
        void reset() noexcept;
    private:
        struct Lease;std::unique_ptr<Lease> lease_;
        explicit Guard(std::unique_ptr<Lease> lease) noexcept;
        friend class BufferPool;
    };
    // Reserve before appending/forcing the transaction WAL. One actual frame
    // slot belongs exclusively to this publication until finish/destruction.
    // Publishing absent pages reuses that slot and never returns BUFFER_BUSY.
    class Publication {
    public:
        Publication() noexcept;
        ~Publication();
        Publication(Publication&&) noexcept;
        Publication& operator=(Publication&&) noexcept;
        Publication(const Publication&)=delete;Publication& operator=(const Publication&)=delete;
        explicit operator bool() const noexcept;
        void publish(PageId id,Page image,bool dirty=true);
        void finish() noexcept;
    private:
        struct Lease;std::unique_ptr<Lease> lease_;
        explicit Publication(std::unique_ptr<Lease> lease) noexcept;
        friend class BufferPool;
    };
    BufferPool(std::size_t capacity,Loader loader,Flusher flusher,Config config={});
    ~BufferPool();
    BufferPool(const BufferPool&)=delete;BufferPool& operator=(const BufferPool&)=delete;
    Guard fetch(PageId id,Hint hint=Hint::Normal);
    Publication reserve_publication(unsigned wait_ms=0);
    void prefetch(const std::vector<PageId>& ids);
    void flush_all();
    bool flush_one();
    void configure(Config config);
    Statistics statistics() const;
    std::vector<FrameSnapshot> snapshot(std::size_t maximum=64) const;
    void close() noexcept;
private:
    std::shared_ptr<State> state_;
};
const char* policy_name(Policy policy) noexcept;
} // namespace edudb::buffer
