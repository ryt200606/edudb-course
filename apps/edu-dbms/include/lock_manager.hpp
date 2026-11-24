#pragma once
#include "json.hpp"
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace edudb {
enum class LockMode { IS, IX, S, SIX, X };
enum class DeadlockPolicy { Detect, WaitDie, WoundWait };
class LockManager {
public:
    using json=nlohmann::json;
    using Mode=LockMode;
    using Policy=DeadlockPolicy;
    struct IntegerRange {
        std::optional<std::int64_t> lower,upper;
        bool lower_inclusive=true,upper_inclusive=true;
    };
    // Integer-domain intervals. Absent endpoints are infinities; endpoint
    // normalization never increments/decrements INT64_MAX/MIN.
    static bool range_empty(const IntegerRange& range);
    static bool ranges_overlap(const IntegerRange& a,const IntegerRange& b);
    static json range_json(const IntegerRange& range);
    bool acquire_range(std::uint64_t tx,const std::string& resource,const IntegerRange& range,Mode mode,bool nonblocking=false);
    class CommitPermit {
        friend class LockManager;
        LockManager* owner_=nullptr;
        std::uint64_t tx_=0;
        std::unique_lock<std::mutex> guard_;
        bool completed_=false;
        CommitPermit(LockManager* owner,std::uint64_t tx,std::unique_lock<std::mutex> guard);
    public:
        CommitPermit(CommitPermit&& other) noexcept;
        CommitPermit& operator=(CommitPermit&&)=delete;
        CommitPermit(const CommitPermit&)=delete;
        CommitPermit& operator=(const CommitPermit&)=delete;
        ~CommitPermit();
    };
    explicit LockManager(std::chrono::milliseconds timeout,Policy policy=Policy::Detect,std::size_t event_limit=4096);
    static bool compatible(Mode held,Mode requested);
    static std::string mode_name(Mode mode);
    static std::string policy_name(Policy policy);
    // Legacy table/catalog API: unchanged S/X and nonblocking contract.
    bool acquire(std::uint64_t tx,const std::string& resource,bool exclusive,bool nonblocking=false);
    bool acquire(std::uint64_t tx,const std::string& resource,Mode mode,bool nonblocking=false);
    void release(std::uint64_t tx);
    // Restricted TREE helper; default strict2PL never calls early release.
    void release_one_tree_node(std::uint64_t tx,const std::string& resource);
    json snapshot();
    // Optional priorities are immutable. Lower timestamp means older; cycle
    // recovery still chooses the greatest transaction ID, not greatest priority.
    void register_transaction(std::uint64_t tx,std::uint64_t start_timestamp);
    void check_cancelled(std::uint64_t tx);
    std::shared_ptr<const std::atomic<bool>> cancellation_token(std::uint64_t tx);
    void set_policy(Policy policy);
    // Hold this permit across Storage.commit. Storage must NEVER call back into
    // this manager. Mark the outcome before release(tx). No check-then-wound gap.
    CommitPermit prepare_commit(std::uint64_t tx,bool parallel_admission=false);
    void commit_succeeded(CommitPermit& permit);
    void commit_failed(CommitPermit& permit,bool outcome_unknown);
    // Independent schema/catalog locks remain flat. Data hierarchy registration
    // is explicit, so legacy catalog S does not accidentally mean database S.
    void register_resource(const std::string& resource,const std::string& parent="");
    bool acquire_path(std::uint64_t tx,const std::string& leaf,Mode mode,bool nonblocking=false);
    // Nonblocking path failure retains ancestors already acquired, consistent
    // with strict 2PL; release(tx) is the rollback/cleanup operation.
private:
    enum class Phase { Active, Committing, Committed, Failed, Unknown };
    struct State {std::uint64_t priority=0;bool cancelled=false;std::string reason;Phase phase=Phase::Active;std::shared_ptr<std::atomic<bool>> token=std::make_shared<std::atomic<bool>>(false);};
    struct Request {std::uint64_t sequence=0,tx=0;std::string resource;Mode mode=Mode::S;bool upgrade=false,cancelled=false;std::string cancellation;std::optional<IntegerRange> range;};
    struct RangeGrant {std::uint64_t tx;IntegerRange range;Mode mode;};
    struct Entry {std::map<std::uint64_t,Mode> holders;std::deque<std::shared_ptr<Request>> queue;};
    std::mutex mutex_;std::condition_variable cv_;
    std::map<std::string,Entry> entries_;
    std::map<std::string,std::vector<RangeGrant>> range_holders_;
    std::map<std::uint64_t,std::shared_ptr<Request>> pending_;
    std::map<std::uint64_t,State> states_;
    std::map<std::string,std::string> parents_;
    std::deque<json> events_;
    std::chrono::milliseconds timeout_;Policy policy_;
    std::size_t event_limit_;bool events_truncated_=false;
    std::uint64_t sequence_=0,event_sequence_=0;
    static Mode join(Mode a,Mode b);
    static bool covers(Mode held,Mode requested);
    State& state(std::uint64_t tx);
    std::uint64_t priority(std::uint64_t tx) const;
    bool protected_outcome(std::uint64_t tx) const;
    void event(const std::string& kind,std::uint64_t tx,const std::string& resource="",const json& details=json::object());
    void cancelled(std::uint64_t tx,const std::shared_ptr<Request>& request={});
    std::set<std::uint64_t> blockers(const std::shared_ptr<Request>& request) const;
    std::map<std::uint64_t,std::set<std::uint64_t>> waiting_graph() const;
    std::vector<std::uint64_t> cycle(const std::map<std::uint64_t,std::set<std::uint64_t>>& graph) const;
    void remove_request(const std::shared_ptr<Request>& request);
    void cancel(std::uint64_t tx,const std::string& reason,std::uint64_t by=0);
    void validate_ancestors(std::uint64_t tx,const std::string& resource,Mode requested) const;
    void grant(std::uint64_t tx,const std::string& resource,Mode mode,bool upgrade);
    void grant_range(const std::shared_ptr<Request>& request);
    bool wait_for_request(std::unique_lock<std::mutex>& guard,const std::shared_ptr<Request>& request);
    void finish_permit(CommitPermit& permit,Phase phase);
    void abandon_permit(CommitPermit& permit) noexcept;
};
} // namespace edudb
