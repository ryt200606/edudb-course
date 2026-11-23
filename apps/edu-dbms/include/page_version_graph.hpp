#pragma once
#include "json.hpp"
#include <cstddef>
#include <cstdint>
#include <atomic>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace edudb {
// An observation-only, bounded graph of actual committed PAGE versions in one
// process epoch. It neither authorizes a commit nor proves predicate/row-level
// serializability. Unknown pre-start creators and all pruning remain explicit.
class PageVersionGraph {
public:
    using Id=std::uint64_t;
    using json=nlohmann::json;
    struct Limits {std::size_t transactions=4096,versions=65536,readers=131072,edges=131072;};
    struct Write {Id page=0,previous_lsn=0,new_lsn=0;std::uint16_t page_type=0;};
    explicit PageVersionGraph(Limits limits);
    PageVersionGraph():PageVersionGraph(Limits{}){}
    void begin(Id transaction,Id start_csn) noexcept;
    void read(Id transaction,Id page,Id selected_page_lsn,std::uint16_t page_type) noexcept;
    void publish(Id transaction,Id csn,const std::vector<Write>& pages) noexcept;
    void restore_alias(Id page,Id committed_base_lsn,Id restored_page_lsn,std::uint16_t page_type) noexcept;
    void finish(Id transaction,const std::string& outcome) noexcept;
    // Caller has excluded active transactions before replacing page history.
    void reset_epoch(const std::string& reason) noexcept;
    json diagnostics() const;
private:
    struct Node {Id start=0,commit=0;std::string outcome="ACTIVE";};
    struct Version {Id csn=0,creator=0;std::uint16_t type=0;bool creator_known=false;std::set<Id> readers;Id restored_from=0;bool system_restored=false;};
    struct Edge {std::string kind;Id page=0,source_lsn=0,target_lsn=0;bool operator<(const Edge& b)const;};
    using VersionKey=std::pair<Id,Id>; // page + concrete pageLSN
    Limits limits_;
    mutable std::mutex mutex_;
    std::map<Id,Node> nodes_;
    std::map<VersionKey,Version> versions_;
    std::map<std::pair<Id,Id>,std::set<Edge>> edges_;
    std::size_t readers_=0,edge_count_=0;
    std::uint64_t read_events_=0,published_pages_=0,pruned_transactions_=0,invalid_events_=0;
    std::uint64_t epoch_resets_=0;std::string epoch_reset_reason_;
    std::atomic<bool> truncated_{false},disabled_{false};
    Version& version(Id page,Id lsn,std::uint16_t type);
    void edge(Id from,Id to,const char* kind,Id page,Id from_lsn,Id to_lsn);
    void erase_node(Id id);
    void failed() noexcept;
};
}
