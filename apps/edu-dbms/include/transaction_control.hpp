#pragma once
#include "json.hpp"
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace edudb {
enum class TransactionProtocol { Strict2PL, Snapshot, Occ, Timestamp, Thomas, Mv2pl, Mvto, Tree };
std::string transaction_protocol_name(TransactionProtocol protocol);
TransactionProtocol parse_transaction_protocol(const std::string& name);
struct TransactionOptions {
    TransactionProtocol protocol = TransactionProtocol::Strict2PL;
    bool read_only = false;
    // Snapshot + true = a new committed view for each statement. Writers still
    // hold target-table X to transaction end; false uses a fixed BEGIN snapshot.
    bool read_committed = false;
    // Strict range/record locks still require one physical committed-page view
    // per statement: disjoint keys can share a split/retired index page.
    bool record_locks = false;
};
// TREE owns an explicit statement-view policy; it never enables Strict RECORD.
bool tree_statement_snapshot(const TransactionOptions& options);
struct TransactionAccess {
    std::string table;
    bool read = false;
    bool write = false;
    // Only a genuinely blind assignment may use the Thomas obsolete-write rule.
    bool blind_write = false;
};
class TransactionControl {
public:
    using json=nlohmann::json;
    struct Context {
        std::uint64_t id=0,start_ts=0,start_csn=0,read_csn=0;
        TransactionOptions options;
        bool active=true,committed=false;
        std::set<std::string> read_tables,write_tables,ignored_write_tables;
        std::map<std::string,std::uint64_t> table_epochs_at_begin;
        std::map<std::uint64_t,std::uint64_t> read_pages;
    };
    using Handle=std::shared_ptr<Context>;
    struct StatementView {std::uint64_t read_csn=0;bool ignore_write=false;};
    struct PageCheck {std::uint64_t page=0,base_lsn=0,current_lsn=0;bool newly_allocated=false,write=true;};
    struct CommitPermit {
        std::unique_lock<std::recursive_mutex> guard;
        Handle context;
        std::uint64_t csn=0;
        bool writes=false,completed=false;
        CommitPermit(std::unique_lock<std::recursive_mutex> lock,Handle ctx,std::uint64_t number,bool writing)
            :guard(std::move(lock)),context(std::move(ctx)),csn(number),writes(writing){}
        CommitPermit(CommitPermit&&)=default;
        CommitPermit& operator=(CommitPermit&&)=default;
        CommitPermit(const CommitPermit&)=delete;
        CommitPermit& operator=(const CommitPermit&)=delete;
    };
    explicit TransactionControl(std::size_t event_limit=4096);
    Handle begin(std::uint64_t id,const TransactionOptions& options={});
    StatementView before_statement(const Handle& tx,const std::vector<TransactionAccess>& accesses);
    void note_page_read(const Handle& tx,std::uint64_t page,std::uint64_t lsn);
    // Caller holds WAL, then this mutex, then accesses buffer/pages. The permit
    // remains held across WAL force and page publication. Never buffer -> mutex.
    CommitPermit prepare_commit(const Handle& tx,const std::vector<PageCheck>& pages);
    void complete_commit(CommitPermit& permit,std::uint64_t durable_lsn);
    void finish(const Handle& tx,const std::string& reason="rollback");
    void event(const Handle& tx,const std::string& kind,const std::string& resource="",const json& details=json::object());
    json diagnostics() const;
    std::unique_lock<std::recursive_mutex> lock() {return std::unique_lock<std::recursive_mutex>(mutex_);}
    std::uint64_t committed_csn() const;
    std::uint64_t oldest_snapshot_csn() const;
    bool has_snapshot_readers(std::uint64_t excluding_transaction=0) const;
private:
    mutable std::recursive_mutex mutex_;
    std::uint64_t next_ts_=1,csn_=0,event_sequence_=0;
    std::size_t event_limit_;
    std::size_t trace_reader_count_=0;
    bool events_truncated_=false;
    std::map<std::uint64_t,Handle> active_;
    std::map<std::string,std::uint64_t> epochs_,read_ts_,write_ts_,last_writer_;
    std::map<std::string,std::set<std::uint64_t>> readers_;
    std::map<std::pair<std::uint64_t,std::uint64_t>,std::set<std::string>> edges_;
    std::deque<json> events_;
    void require(const Handle& tx) const;
    void trace_access(const Handle& tx,const TransactionAccess& access);
};
} // namespace edudb
