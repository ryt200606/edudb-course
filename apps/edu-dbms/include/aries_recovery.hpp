#pragma once
#include "json.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace edudb {class DataDevice;
namespace aries {
constexpr std::size_t page_size=8192;
using Page=std::array<unsigned char,page_size>;
using TxId=std::uint64_t;
using PageId=std::uint64_t;
using Lsn=std::uint64_t;
using json=nlohmann::json;
struct PageCodec {
    std::function<Page(PageId)> empty;
    std::function<bool(const Page&,PageId)> valid;
    std::function<Lsn(const Page&)> lsn;
    std::function<void(Page&,Lsn)> stamp;
};
PageCodec default_page_codec();
std::unique_ptr<DataDevice> open_native_data_device(const std::string& path);
struct Options {
    std::size_t buffer_frames=8;
    bool steal=true;
    bool force_at_commit=false;
    // Absolute WAL limit. Online updates/checkpoint metadata use at most half;
    // the rest is conservatively reserved for COMMIT/ABORT/CLR/END recovery.
    std::uint64_t wal_limit_bytes=512ull*1024*1024;
    // A controlled interleave hook is called OUTSIDE the manager mutex between
    // BEGIN_CHECKPOINT and the TT/DPT snapshot. It must be finite; production
    // integrations leave it empty. This permits deterministic fuzzy-CKPT tests.
    std::function<void()> checkpoint_interleave;
    // Optional deterministic error injection at named native-I/O, post-append
    // bookkeeping and post-COMMIT-force boundaries.
    // Throw DbError("IO_ERROR",...) from this callback; production leaves empty.
    std::function<void(const char*)> io_fault;
};
class Manager {
public:
    // New ARIES_PAGE mode only. data must outlive this manager. Supply existing
    // Storage page codec callbacks to keep its page identity/type/CRC contract.
    // All SQL transactions must use Strict2PL and catalog X for DDL; this module
    // also rejects dirty reads and conflicting full-page writers by page owner.
    Manager(const std::string& directory,DataDevice& data,const std::string& database_uuid,
            PageCodec codec=default_page_codec(),Options options={});
    ~Manager();
    void begin(TxId tx,bool read_only=false);
    // Trusted ARIES_LOGICAL adapter only: normal online writes cannot consume
    // this outstanding logical-compensation reservation. Changing it requires
    // no active physical writer. Recovery reconstructs it from both journals.
    void set_external_reservation(std::uint64_t bytes);
    void begin_compensation(TxId tx,std::uint64_t credit_bytes,std::uint64_t update_bytes);
    struct LogSpace {std::uint64_t used=0,limit=0,reserved=0;};
    LogSpace log_space() const;
    std::uint64_t transaction_record_bytes(TxId tx) const;
    bool transaction_committed(TxId tx) const;
    // Logical startup may append compensating mini-transactions after the
    // physical constructor's recovery. End that phase with all data and END
    // records synchronized; never changes ordinary no-force commit policy.
    void recovery_barrier();
    Page read(TxId tx,PageId page);
    // Called for EVERY physical SQL page modification, not batched at COMMIT.
    // Captures before/after image, links PrevLSN, updates the authority buffer.
    void update(TxId tx,PageId page,const Page& image);
    json commit(TxId tx);
    void abort(TxId tx);
    void savepoint(TxId tx,const std::string& name);
    void rollback_to(TxId tx,const std::string& name);
    void release_savepoint(TxId tx,const std::string& name);
    json checkpoint_fuzzy();
    void flush_page(PageId page);
    void flush_all();
    std::uint64_t transaction_wal_bytes(TxId tx) const;
    json transaction_metrics(TxId tx) const;
    json diagnostics() const;
    json wal_records(std::size_t limit=4096) const;
    Manager(const Manager&)=delete;
    Manager& operator=(const Manager&)=delete;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} } // namespace edudb::aries
