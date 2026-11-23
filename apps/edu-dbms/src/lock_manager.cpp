#include "lock_manager.hpp"
#include "edudb.hpp"
#include <algorithm>
#include <limits>
#include <utility>

namespace edudb {
namespace {using Clock=std::chrono::steady_clock;}
namespace {
bool inclusive_bounds(const LockManager::IntegerRange& range,std::int64_t& lower,std::int64_t& upper){
    lower=range.lower.value_or(std::numeric_limits<std::int64_t>::min());upper=range.upper.value_or(std::numeric_limits<std::int64_t>::max());
    if(range.lower&&!range.lower_inclusive){if(lower==std::numeric_limits<std::int64_t>::max())return false;++lower;}
    if(range.upper&&!range.upper_inclusive){if(upper==std::numeric_limits<std::int64_t>::min())return false;--upper;}
    return lower<=upper;
}
}
bool LockManager::range_empty(const IntegerRange& range){std::int64_t lower,upper;return !inclusive_bounds(range,lower,upper);}
bool LockManager::ranges_overlap(const IntegerRange& a,const IntegerRange& b){std::int64_t al,au,bl,bu;return inclusive_bounds(a,al,au)&&inclusive_bounds(b,bl,bu)&&al<=bu&&bl<=au;}
LockManager::json LockManager::range_json(const IntegerRange& range){return {{"lower",range.lower?json(std::to_string(*range.lower)):json()},{"upper",range.upper?json(std::to_string(*range.upper)):json()},{"lower_inclusive",range.lower_inclusive},{"upper_inclusive",range.upper_inclusive},{"empty",range_empty(range)},{"domain","INT64"},{"encoding","signed64 decimal strings; null endpoint is infinity"}};}
bool LockManager::compatible(Mode a,Mode b) {
    static const bool matrix[5][5]={{true,true,true,true,false},{true,true,false,false,false},{true,false,true,false,false},{true,false,false,false,false},{false,false,false,false,false}};
    auto x=static_cast<unsigned>(a),y=static_cast<unsigned>(b);
    if(x>=5||y>=5)throw DbError("INVALID_LOCK_MODE","Unknown lock mode");
    return matrix[x][y];
}
std::string LockManager::mode_name(Mode mode) {
    switch(mode){case Mode::IS:return "IS";case Mode::IX:return "IX";case Mode::S:return "S";case Mode::SIX:return "SIX";case Mode::X:return "X";}
    throw DbError("INVALID_LOCK_MODE","Unknown lock mode");
}
std::string LockManager::policy_name(Policy p) {
    switch(p){case Policy::Detect:return "DETECT";case Policy::WaitDie:return "WAIT_DIE";case Policy::WoundWait:return "WOUND_WAIT";}
    throw DbError("INVALID_DEADLOCK_POLICY","Unknown deadlock policy");
}
LockManager::LockManager(std::chrono::milliseconds timeout,Policy policy,std::size_t event_limit)
    :timeout_(timeout),policy_(policy),event_limit_(std::max<std::size_t>(16,event_limit)) {
    if(timeout_.count()<0)throw DbError("ARGUMENT","Lock timeout cannot be negative");
    (void)policy_name(policy);
}
LockManager::Mode LockManager::join(Mode a,Mode b) {
    if(a==b)return a;
    if(a==Mode::X||b==Mode::X)return Mode::X;
    if(a==Mode::SIX||b==Mode::SIX)return Mode::SIX;
    if((a==Mode::S&&b==Mode::IX)||(a==Mode::IX&&b==Mode::S))return Mode::SIX;
    if(a==Mode::S||b==Mode::S)return Mode::S;
    if(a==Mode::IX||b==Mode::IX)return Mode::IX;
    return Mode::IS;
}
bool LockManager::covers(Mode held,Mode requested){return join(held,requested)==held;}
LockManager::State& LockManager::state(std::uint64_t tx) {
    if(!tx)throw DbError("ARGUMENT","Transaction ID must be nonzero");
    auto found=states_.find(tx);if(found==states_.end())found=states_.emplace(tx,State{tx,false,"",Phase::Active}).first;
    return found->second;
}
std::uint64_t LockManager::priority(std::uint64_t tx) const {auto s=states_.find(tx);return s==states_.end()?tx:s->second.priority;}
bool LockManager::protected_outcome(std::uint64_t tx) const {auto s=states_.find(tx);return s!=states_.end()&&(s->second.phase==Phase::Committing||s->second.phase==Phase::Committed||s->second.phase==Phase::Unknown);}
void LockManager::event(const std::string& kind,std::uint64_t tx,const std::string& resource,const json& details) {
    events_.push_back({{"sequence",++event_sequence_},{"kind",kind},{"tx_id",tx},{"resource",resource},{"details",details}});
    if(events_.size()>event_limit_){events_.pop_front();events_truncated_=true;}
}
void LockManager::register_transaction(std::uint64_t tx,std::uint64_t timestamp) {
    std::lock_guard<std::mutex> guard(mutex_);if(!timestamp)throw DbError("ARGUMENT","Transaction timestamp must be nonzero");
    auto found=states_.find(tx);if(found!=states_.end()&&found->second.priority!=timestamp)throw DbError("TRANSACTION_ACTIVE","Lock priority is immutable");state(tx).priority=timestamp;
}
void LockManager::cancelled(std::uint64_t tx,const std::shared_ptr<Request>& request) {
    auto s=states_.find(tx);
    if((request&&request->cancelled)||(s!=states_.end()&&s->second.cancelled))throw DbError("DEADLOCK",request&&request->cancelled?request->cancellation:s->second.reason);
}
void LockManager::check_cancelled(std::uint64_t tx){std::lock_guard<std::mutex> guard(mutex_);cancelled(tx);}
std::shared_ptr<const std::atomic<bool>> LockManager::cancellation_token(std::uint64_t tx){std::lock_guard<std::mutex> guard(mutex_);return state(tx).token;}
void LockManager::set_policy(Policy policy) {
    std::lock_guard<std::mutex> guard(mutex_);(void)policy_name(policy);
    if(!entries_.empty()||!pending_.empty())throw DbError("TRANSACTION_ACTIVE","Cannot change deadlock policy while locks or requests exist");
    policy_=policy;
}
std::set<std::uint64_t> LockManager::blockers(const std::shared_ptr<Request>& r) const {
    std::set<std::uint64_t> result;auto found=entries_.find(r->resource);
    if(r->range){
        auto grants=range_holders_.find(r->resource);if(grants!=range_holders_.end())for(const auto& h:grants->second)if(h.tx!=r->tx&&!compatible(h.mode,r->mode)&&ranges_overlap(h.range,*r->range))result.insert(h.tx);
        if(found!=entries_.end())for(const auto& previous:found->second.queue){if(previous.get()==r.get())break;if(previous->tx!=r->tx&&!previous->cancelled&&!compatible(previous->mode,r->mode)&&previous->range&&ranges_overlap(*previous->range,*r->range))result.insert(previous->tx);}
        return result;
    }
    if(found==entries_.end())return result;
    for(const auto& h:found->second.holders)if(h.first!=r->tx&&!compatible(h.second,r->mode))result.insert(h.first);
    for(const auto& previous:found->second.queue) {
        if(previous.get()==r.get())break;
        if(previous->tx!=r->tx&&!previous->cancelled&&!compatible(previous->mode,r->mode))result.insert(previous->tx);
    }
    return result;
}
std::map<std::uint64_t,std::set<std::uint64_t>> LockManager::waiting_graph() const {
    std::map<std::uint64_t,std::set<std::uint64_t>> graph;
    for(const auto& p:pending_){auto& edges=graph[p.second->tx];auto blocked=blockers(p.second);edges.insert(blocked.begin(),blocked.end());}
    return graph;
}
std::vector<std::uint64_t> LockManager::cycle(const std::map<std::uint64_t,std::set<std::uint64_t>>& graph) const {
    struct Frame{std::uint64_t node;std::vector<std::uint64_t> edges;std::size_t next=0;};
    std::map<std::uint64_t,int> color;std::map<std::uint64_t,std::size_t> position;
    for(const auto& start:graph) {
        if(color[start.first])continue;
        std::vector<Frame> stack;auto push=[&](std::uint64_t n){auto it=graph.find(n);std::vector<std::uint64_t> edges;if(it!=graph.end())edges.assign(it->second.begin(),it->second.end());position[n]=stack.size();color[n]=1;stack.push_back({n,std::move(edges),0});};push(start.first);
        while(!stack.empty()) {
            auto& top=stack.back();if(top.next==top.edges.size()){color[top.node]=2;position.erase(top.node);stack.pop_back();continue;}
            auto n=top.edges[top.next++];if(color[n]==1){std::vector<std::uint64_t> answer;for(std::size_t i=position.at(n);i<stack.size();++i)answer.push_back(stack[i].node);return answer;}
            if(!color[n])push(n);
        }
    }
    return {};
}
void LockManager::remove_request(const std::shared_ptr<Request>& r) {
    auto e=entries_.find(r->resource);
    if(e!=entries_.end()){auto& q=e->second.queue;q.erase(std::remove(q.begin(),q.end(),r),q.end());if(q.empty()&&e->second.holders.empty()&&!range_holders_.count(r->resource))entries_.erase(e);}
    pending_.erase(r->sequence);
}
void LockManager::cancel(std::uint64_t tx,const std::string& reason,std::uint64_t by) {
    if(protected_outcome(tx))return;
    auto& s=state(tx);s.cancelled=true;s.token->store(true);s.reason=reason;
    std::vector<std::shared_ptr<Request>> remove;for(const auto& p:pending_)if(p.second->tx==tx)remove.push_back(p.second);
    for(auto& r:remove){r->cancelled=true;r->cancellation=reason;remove_request(r);}
    event("victim",tx,"",{{"reason",reason},{"by_tx",by},{"policy",policy_name(policy_)}});cv_.notify_all();
}
void LockManager::validate_ancestors(std::uint64_t tx,const std::string& resource,Mode requested) const {
    auto found=parents_.find(resource);if(found==parents_.end())return;
    auto parent=found->second;const bool write=requested==Mode::IX||requested==Mode::SIX||requested==Mode::X;
    while(!parent.empty()) {
        auto e=entries_.find(parent);if(e==entries_.end()||!e->second.holders.count(tx))throw DbError("LOCK_HIERARCHY","Missing ancestor intention lock: "+parent);
        auto held=e->second.holders.at(tx);
        if(write&&held!=Mode::IX&&held!=Mode::SIX&&held!=Mode::X)throw DbError("LOCK_HIERARCHY","Ancestor requires IX, SIX or X: "+parent);
        auto p=parents_.find(parent);parent=p==parents_.end()?"":p->second;
    }
}
void LockManager::grant(std::uint64_t tx,const std::string& resource,Mode mode,bool upgrade) {
    auto& s=state(tx);if(s.phase!=Phase::Active)throw DbError("NO_TRANSACTION","Cannot acquire locks after commit completion");
    entries_[resource].holders[tx]=mode;event(upgrade?"upgrade_granted":"granted",tx,resource,{{"mode",mode_name(mode)}});
}
void LockManager::grant_range(const std::shared_ptr<Request>& r){
    auto& s=state(r->tx);if(s.phase!=Phase::Active)throw DbError("NO_TRANSACTION","Cannot acquire ranges after commit completion");
    range_holders_[r->resource].push_back({r->tx,*r->range,r->mode});entries_[r->resource];event(r->upgrade?"range_upgrade_granted":"range_granted",r->tx,r->resource,{{"mode",mode_name(r->mode)},{"interval",range_json(*r->range)}});
}
bool LockManager::acquire_range(std::uint64_t tx,const std::string& resource,const IntegerRange& range,Mode mode,bool nonblocking){
    std::unique_lock<std::mutex> guard(mutex_);if(!tx||resource.rfind("range:",0)!=0||(mode!=Mode::S&&mode!=Mode::X))throw DbError("ARGUMENT","Range locks require nonzero transaction, range: domain and S/X mode");cancelled(tx);
    auto existing=states_.find(tx);if(existing!=states_.end()&&existing->second.phase!=Phase::Active)throw DbError("NO_TRANSACTION","Cannot acquire ranges after commit completion");
    if(!parents_.count(resource)||parents_.at(resource).empty())throw DbError("LOCK_HIERARCHY","Range domain needs a registered table parent");
    validate_ancestors(tx,resource,mode);
    if(range_empty(range))return true;
    auto r=std::make_shared<Request>();r->tx=tx;r->resource=resource;r->mode=mode;r->range=range;
    auto grants=range_holders_.find(resource);if(grants!=range_holders_.end())for(const auto& h:grants->second)if(h.tx==tx){
        std::int64_t hl,hu,rl,ru;inclusive_bounds(h.range,hl,hu);inclusive_bounds(range,rl,ru);
        if(covers(h.mode,mode)&&hl<=rl&&hu>=ru)return true;
        if(ranges_overlap(h.range,range)&&h.mode!=mode)r->upgrade=true;
    }
    std::size_t grant_count=0;for(const auto& item:range_holders_)grant_count+=item.second.size();if(grant_count>=131072)throw DbError("LOCK_RESOURCE_LIMIT","Range grant limit exceeded");
    if(blockers(r).empty()){grant_range(r);return true;}if(nonblocking)return false;
    return wait_for_request(guard,r);
}
bool LockManager::acquire(std::uint64_t tx,const std::string& resource,bool exclusive,bool nonblocking){return acquire(tx,resource,exclusive?Mode::X:Mode::S,nonblocking);}
bool LockManager::acquire(std::uint64_t tx,const std::string& resource,Mode requested,bool nonblocking) {
    std::unique_lock<std::mutex> guard(mutex_);if(!tx||resource.empty())throw DbError("ARGUMENT","Nonzero transaction and nonempty resource required");(void)mode_name(requested);cancelled(tx);
    if(resource.rfind("range:",0)==0)throw DbError("ARGUMENT","Use acquire_range for interval domains");
    auto existing_state=states_.find(tx);if(existing_state!=states_.end()&&existing_state->second.phase!=Phase::Active)throw DbError("NO_TRANSACTION","Cannot acquire locks after commit completion");
    auto e=entries_.find(resource);bool upgrade=false;Mode desired=requested;
    if(e!=entries_.end()){auto h=e->second.holders.find(tx);if(h!=e->second.holders.end()){if(covers(h->second,requested))return true;desired=join(h->second,requested);upgrade=true;}}
    validate_ancestors(tx,resource,desired);
    auto r=std::make_shared<Request>();r->tx=tx;r->resource=resource;r->mode=desired;r->upgrade=upgrade;
    if(blockers(r).empty()){grant(tx,resource,desired,upgrade);return true;}
    if(nonblocking)return false;
    return wait_for_request(guard,r);
}
bool LockManager::wait_for_request(std::unique_lock<std::mutex>& guard,const std::shared_ptr<Request>& r){
    const auto tx=r->tx;const auto& resource=r->resource;
    if(pending_.size()>=8192)throw DbError("LOCK_RESOURCE_LIMIT","Pending lock request limit exceeded");
    for(const auto& p:pending_)if(p.second->tx==tx&&p.second->resource==resource)throw DbError("LOCK_REQUEST_ACTIVE","Transaction already waits for this resource");
    state(tx);r->sequence=++sequence_;entries_[resource].queue.push_back(r);pending_[r->sequence]=r;json details={{"mode",mode_name(r->mode)},{"upgrade",r->upgrade},{"ticket",r->sequence}};if(r->range)details["interval"]=range_json(*r->range);event("queued",tx,resource,details);
    const auto deadline=Clock::now()+timeout_;
    for(;;) {
        cancelled(tx,r);auto blocked=blockers(r);
        if(blocked.empty()){remove_request(r);if(r->range)grant_range(r);else grant(tx,resource,r->mode,r->upgrade);cv_.notify_all();return true;}
        if(policy_==Policy::WaitDie) {
            bool die=false;for(auto b:blocked)if(!protected_outcome(b)&&std::make_pair(priority(tx),tx)>std::make_pair(priority(b),b))die=true;
            if(die){cancel(tx,"wait-die: younger requester must abort");cancelled(tx,r);}
        }else if(policy_==Policy::WoundWait) {
            bool wounded=false;for(auto b:blocked)if(!protected_outcome(b)&&std::make_pair(priority(tx),tx)<std::make_pair(priority(b),b)&&!state(b).cancelled){cancel(b,"wound-wait: cancelled by older transaction",tx);wounded=true;}
            if(wounded)continue;
        }else {
            auto found=cycle(waiting_graph());if(!found.empty()){auto victim=*std::max_element(found.begin(),found.end());event("deadlock_cycle",tx,resource,{{"cycle",found},{"victim",victim}});cancel(victim,"deadlock: greatest transaction ID in the cycle");continue;}
        }
        if(cv_.wait_until(guard,deadline)==std::cv_status::timeout){cancel(tx,"Lock wait timed out; rollback required");event("timeout",tx,resource);cv_.notify_all();throw DbError("LOCK_TIMEOUT","Lock wait exceeded "+std::to_string(timeout_.count())+" ms");}
    }
}
void LockManager::release_one_tree_node(std::uint64_t tx,const std::string& resource){
    std::lock_guard<std::mutex> guard(mutex_);cancelled(tx);auto& status=state(tx);
    if(status.phase!=Phase::Active||resource.rfind("tree:",0)!=0)throw DbError("TREE_RESOURCE","Only active TREE node grants can release early");
    auto entry=entries_.find(resource);if(entry==entries_.end()||!entry->second.holders.count(tx)||entry->second.holders.at(tx)!=Mode::X)throw DbError("TREE_NOT_LOCKED","Actual node X grant missing");
    entry->second.holders.erase(tx);if(entry->second.holders.empty()&&entry->second.queue.empty())entries_.erase(entry);
    event("tree_node_released_early",tx,resource);cv_.notify_all();
}
void LockManager::release(std::uint64_t tx) {
    std::lock_guard<std::mutex> guard(mutex_);std::vector<std::shared_ptr<Request>> remove;
    for(const auto& p:pending_)if(p.second->tx==tx)remove.push_back(p.second);
    for(auto& r:remove){r->cancelled=true;r->cancellation="Transaction released while request was pending";remove_request(r);}
    for(auto it=range_holders_.begin();it!=range_holders_.end();){auto& grants=it->second;grants.erase(std::remove_if(grants.begin(),grants.end(),[&](const RangeGrant& g){return g.tx==tx;}),grants.end());if(grants.empty())it=range_holders_.erase(it);else ++it;}
    for(auto it=entries_.begin();it!=entries_.end();) {it->second.holders.erase(tx);if(it->second.holders.empty()&&it->second.queue.empty()&&!range_holders_.count(it->first))it=entries_.erase(it);else ++it;}
    auto finished=states_.find(tx);if(finished!=states_.end())finished->second.token->store(true);states_.erase(tx);event("released",tx);cv_.notify_all();
}
LockManager::json LockManager::snapshot() {
    std::lock_guard<std::mutex> guard(mutex_);json locks=json::array(),waits=json::array(),queues=json::array(),transactions=json::array();auto graph=waiting_graph();
    for(const auto& e:entries_){for(const auto& h:e.second.holders)locks.push_back({{"resource",e.first},{"tx_id",h.first},{"mode",mode_name(h.second)}});for(const auto& r:e.second.queue)queues.push_back({{"resource",r->resource},{"tx_id",r->tx},{"mode",mode_name(r->mode)},{"ticket",r->sequence},{"upgrade",r->upgrade}});}
    for(const auto& domain:range_holders_)for(const auto& h:domain.second)locks.push_back({{"resource",domain.first},{"tx_id",h.tx},{"mode",mode_name(h.mode)},{"kind","INTEGER_RANGE"},{"interval",range_json(h.range)}});
    for(auto& q:queues){auto pending=pending_.find(q.at("ticket").get<std::uint64_t>());if(pending!=pending_.end()&&pending->second->range){q["kind"]="INTEGER_RANGE";q["interval"]=range_json(*pending->second->range);}}
    for(const auto& w:graph)waits.push_back({{"tx_id",w.first},{"blocked_by",w.second}});
    for(const auto& s:states_){std::string phase;switch(s.second.phase){case Phase::Active:phase="ACTIVE";break;case Phase::Committing:phase="COMMITTING";break;case Phase::Committed:phase="COMMITTED";break;case Phase::Failed:phase="FAILED_BEFORE_DURABILITY";break;case Phase::Unknown:phase="OUTCOME_UNKNOWN";break;}transactions.push_back({{"tx_id",s.first},{"timestamp",s.second.priority},{"cancelled",s.second.cancelled},{"reason",s.second.reason},{"phase",phase}});}
    return {{"locks",locks},{"waits",waits},{"queues",queues},{"lock_transactions",transactions},{"deadlock_policy",policy_name(policy_)},{"events",events_},{"events_truncated",events_truncated_},{"wait_graph_scope","incompatible holders plus incompatible preceding FIFO requests; actual lock dependencies, not serialization dependencies"}};
}
LockManager::CommitPermit::CommitPermit(LockManager* owner,std::uint64_t tx,std::unique_lock<std::mutex> guard):owner_(owner),tx_(tx),guard_(std::move(guard)) {}
LockManager::CommitPermit::CommitPermit(CommitPermit&& other) noexcept:owner_(std::exchange(other.owner_,nullptr)),tx_(other.tx_),guard_(std::move(other.guard_)),completed_(other.completed_) {}
LockManager::CommitPermit::~CommitPermit(){if(owner_&&!completed_)owner_->abandon_permit(*this);}
LockManager::CommitPermit LockManager::prepare_commit(std::uint64_t tx) {
    std::unique_lock<std::mutex> guard(mutex_);cancelled(tx);
    for(const auto& p:pending_)if(p.second->tx==tx)throw DbError("LOCK_REQUEST_ACTIVE","Cannot commit while a lock request remains pending");
    auto& s=state(tx);if(s.phase!=Phase::Active)throw DbError("NO_TRANSACTION","Transaction commit already completed");s.phase=Phase::Committing;event("commit_gate_entered",tx);return CommitPermit(this,tx,std::move(guard));
}
void LockManager::finish_permit(CommitPermit& permit,Phase phase) {
    if(permit.owner_!=this||permit.completed_||!permit.guard_.owns_lock())throw DbError("INTERNAL_ERROR","Invalid lock-manager commit permit");
    state(permit.tx_).phase=phase;event(phase==Phase::Committed?"commit_confirmed":phase==Phase::Unknown?"commit_outcome_unknown":"commit_failed_before_durability",permit.tx_);permit.completed_=true;permit.guard_.unlock();cv_.notify_all();
}
void LockManager::commit_succeeded(CommitPermit& permit){finish_permit(permit,Phase::Committed);}
void LockManager::commit_failed(CommitPermit& permit,bool unknown){finish_permit(permit,unknown?Phase::Unknown:Phase::Failed);}
void LockManager::abandon_permit(CommitPermit& permit) noexcept {
    try{if(permit.guard_.owns_lock()){auto i=states_.find(permit.tx_);if(i!=states_.end())i->second.phase=Phase::Unknown;event("commit_outcome_unknown",permit.tx_);permit.completed_=true;permit.guard_.unlock();cv_.notify_all();}}catch(...){/* The unique_lock still releases its owned mutex on destruction. */}
}
void LockManager::register_resource(const std::string& resource,const std::string& parent) {
    std::lock_guard<std::mutex> guard(mutex_);if(resource.empty()||resource==parent)throw DbError("LOCK_HIERARCHY","Invalid resource parent");
    if(!parent.empty()&&!parents_.count(parent))throw DbError("LOCK_HIERARCHY","Register parent before child");
    auto found=parents_.find(resource);if(found!=parents_.end()){if(found->second!=parent)throw DbError("LOCK_HIERARCHY","Resource parent is immutable");return;}
    if(parents_.size()>=131072)throw DbError("LOCK_RESOURCE_LIMIT","Registered hierarchy resource limit exceeded");
    parents_[resource]=parent;
}
bool LockManager::acquire_path(std::uint64_t tx,const std::string& leaf,Mode mode,bool nonblocking) {
    std::vector<std::string> path;
    {std::lock_guard<std::mutex> guard(mutex_);auto it=parents_.find(leaf);if(it==parents_.end())throw DbError("LOCK_HIERARCHY","Unregistered hierarchy resource");for(auto name=leaf;!name.empty();name=parents_.at(name))path.push_back(name);}
    std::reverse(path.begin(),path.end());const auto intent=(mode==Mode::IS||mode==Mode::S)?Mode::IS:Mode::IX;
    for(std::size_t n=0;n<path.size();++n)if(!acquire(tx,path[n],n+1==path.size()?mode:intent,nonblocking))return false;
    return true;
}
} // namespace edudb
