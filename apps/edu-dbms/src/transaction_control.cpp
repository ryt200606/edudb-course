#include "transaction_control.hpp"
#include "edudb.hpp"
#include <algorithm>
#include <cctype>
#include <functional>

namespace edudb {
namespace {
bool retains_committed_history(const TransactionOptions& options) {
    return options.protocol==TransactionProtocol::Mv2pl ? options.read_only : (options.protocol!=TransactionProtocol::Strict2PL||options.record_locks);
}
}

bool tree_statement_snapshot(const TransactionOptions& options){return options.protocol==TransactionProtocol::Tree;}
std::string transaction_protocol_name(TransactionProtocol p) {
    switch(p) {
    case TransactionProtocol::Strict2PL:return "STRICT_2PL";
    case TransactionProtocol::Snapshot:return "SNAPSHOT";
    case TransactionProtocol::Occ:return "OCC_SERIALIZABLE";
    case TransactionProtocol::Timestamp:return "TIMESTAMP";
    case TransactionProtocol::Thomas:return "TIMESTAMP_THOMAS";
    case TransactionProtocol::Mv2pl:return "MV2PL";
    case TransactionProtocol::Mvto:return "MVTO";
    case TransactionProtocol::Tree:return "TREE";
    }
    throw DbError("INVALID_PROTOCOL","Unknown transaction protocol");
}
TransactionProtocol parse_transaction_protocol(const std::string& name) {
    auto n=name;for(auto& c:n)c=static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if(n=="TREE")return TransactionProtocol::Tree;
    if(n=="MVTO")return TransactionProtocol::Mvto;
    if(n=="MV2PL")return TransactionProtocol::Mv2pl;
    if(n=="STRICT_2PL"||n=="2PL")return TransactionProtocol::Strict2PL;
    if(n=="SNAPSHOT"||n=="PAGE_MVCC"||n=="MVCC")return TransactionProtocol::Snapshot;
    if(n=="OCC"||n=="OCC_SERIALIZABLE")return TransactionProtocol::Occ;
    if(n=="TIMESTAMP"||n=="TS")return TransactionProtocol::Timestamp;
    if(n=="THOMAS"||n=="TIMESTAMP_THOMAS")return TransactionProtocol::Thomas;
    throw DbError("INVALID_PROTOCOL","Supported protocols: STRICT_2PL, SNAPSHOT, OCC_SERIALIZABLE, TIMESTAMP, TIMESTAMP_THOMAS, MV2PL, MVTO, TREE");
}
TransactionControl::TransactionControl(std::size_t limit):event_limit_(std::max<std::size_t>(16,limit)) {}
void TransactionControl::require(const Handle& tx) const {
    if(!tx||!tx->active||!active_.count(tx->id)||active_.at(tx->id)!=tx)
        throw DbError("NO_TRANSACTION","Inactive or foreign protocol transaction");
}
void TransactionControl::event(const Handle& tx,const std::string& kind,const std::string& resource,const json& details) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    events_.push_back({{"sequence",++event_sequence_},{"tx_id",tx?tx->id:0},{"kind",kind},{"resource",resource},{"csn",csn_},{"details",details}});
    if(events_.size()>event_limit_){events_.pop_front();events_truncated_=true;}
    if(edges_.size()>event_limit_*2) {
        edges_.clear();readers_.clear();last_writer_.clear();trace_reader_count_=0;events_truncated_=true;
    }
}
TransactionControl::Handle TransactionControl::begin(std::uint64_t id,const TransactionOptions& options) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    (void)transaction_protocol_name(options.protocol);
    if(options.record_locks&&options.protocol!=TransactionProtocol::Strict2PL)
        throw DbError("INVALID_LOCK_GRANULARITY","RECORD locking requires STRICT_2PL");
    if(options.read_committed&&options.protocol!=TransactionProtocol::Snapshot)
        throw DbError("INVALID_ISOLATION","Per-statement read views require SNAPSHOT protocol");
    if(active_.count(id))throw DbError("TRANSACTION_ACTIVE","Duplicate transaction identity");
    if(!active_.empty()&&active_.begin()->second->options.protocol!=options.protocol)
        throw DbError("PROTOCOL_MODE_BUSY","Concurrent transactions must use the same protocol epoch; finish other transactions first");
    auto tx=std::make_shared<Context>();tx->id=id;tx->options=options;
    tx->start_ts=next_ts_++;tx->start_csn=tx->read_csn=csn_;tx->table_epochs_at_begin=epochs_;
    active_[id]=tx;event(tx,"begin","",{{"protocol",transaction_protocol_name(options.protocol)},{"read_only",options.read_only},{"start_timestamp",tx->start_ts}});return tx;
}
void TransactionControl::trace_access(const Handle& tx,const TransactionAccess& a) {
    const auto key="table:"+a.table;
    auto edge=[&](std::uint64_t before,const char* type) {if(before&&before!=tx->id)edges_[{before,tx->id}].insert(key+":"+type);};
    if(a.read){edge(last_writer_[a.table],"WR");if(readers_[a.table].insert(tx->id).second)++trace_reader_count_;event(tx,"read",key);}
    if(a.write){edge(last_writer_[a.table],"WW");for(auto r:readers_[a.table])edge(r,"RW");last_writer_[a.table]=tx->id;trace_reader_count_-=readers_[a.table].size();readers_.erase(a.table);event(tx,"write_intent",key);}
    if(trace_reader_count_>event_limit_||readers_.size()>event_limit_||last_writer_.size()>event_limit_) {
        readers_.clear();last_writer_.clear();edges_.clear();trace_reader_count_=0;events_truncated_=true;
    }
}
TransactionControl::StatementView TransactionControl::before_statement(const Handle& tx,const std::vector<TransactionAccess>& accesses) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);require(tx);
    const bool ts=tx->options.protocol==TransactionProtocol::Timestamp||tx->options.protocol==TransactionProtocol::Thomas;
    bool writes=false,all_ignored=true;
    for(const auto& a:accesses) {
        if(a.table.empty())throw DbError("INVALID_TRANSACTION_ACCESS","Empty relation identity");
        if(a.write&&tx->options.read_only)throw DbError("READ_ONLY_TRANSACTION","Write attempted in a read-only transaction");
        if(ts&&a.read&&tx->start_ts<write_ts_[a.table]) {
            event(tx,"timestamp_read_rejected",a.table,{{"write_timestamp",write_ts_[a.table]}});
            throw DbError("TIMESTAMP_CONFLICT","Read timestamp precedes a committed table write");
        }
        bool ignore=false;
        if(ts&&a.write) {
            if(tx->start_ts<read_ts_[a.table]) {
                event(tx,"timestamp_write_rejected",a.table,{{"read_timestamp",read_ts_[a.table]}});
                throw DbError("TIMESTAMP_CONFLICT","Write timestamp precedes an observed table read");
            }
            if(tx->start_ts<write_ts_[a.table]) {
                if(tx->options.protocol==TransactionProtocol::Thomas&&a.blind_write&&!a.read)ignore=true;
                else throw DbError("TIMESTAMP_CONFLICT","Write timestamp precedes a committed table write");
            }
        }
        if(a.read) {
            tx->read_tables.insert(a.table);
            if(ts)read_ts_[a.table]=std::max(read_ts_[a.table],tx->start_ts);
        }
        if(a.write) {
            writes=true;
            if(ignore){tx->ignored_write_tables.insert(a.table);event(tx,"thomas_ignored_write",a.table);}
            else {all_ignored=false;tx->write_tables.insert(a.table);}
        }
        auto actual=a;actual.write=a.write&&!ignore;trace_access(tx,actual);
    }
    if(tx->options.protocol==TransactionProtocol::Strict2PL||tree_statement_snapshot(tx->options)||ts||tx->options.read_committed||(tx->options.protocol==TransactionProtocol::Mv2pl&&!tx->options.read_only))tx->read_csn=csn_;
    return {tx->read_csn,writes&&all_ignored};
}
void TransactionControl::note_page_read(const Handle& tx,std::uint64_t page,std::uint64_t lsn) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);require(tx);tx->read_pages[page]=lsn;
}
TransactionControl::CommitPermit TransactionControl::prepare_commit(const Handle& tx,const std::vector<PageCheck>& pages) {
    std::unique_lock<std::recursive_mutex> lock(mutex_);require(tx);
    for(const auto& p:pages)if(!p.newly_allocated&&p.base_lsn!=p.current_lsn) {
        event(tx,"page_validation_rejected","page:"+std::to_string(p.page),{{"base_lsn",p.base_lsn},{"current_lsn",p.current_lsn}});
        throw DbError("SERIALIZATION_FAILURE","Committed page changed since the private write base: "+std::to_string(p.page));
    }
    if(tx->options.protocol==TransactionProtocol::Occ)for(const auto& table:tx->read_tables) {
        const auto i=tx->table_epochs_at_begin.find(table);auto before=i==tx->table_epochs_at_begin.end()?0:i->second;
        if(before!=epochs_[table]) {
            event(tx,"occ_validation_rejected",table,{{"snapshot_epoch",before},{"current_epoch",epochs_[table]}});
            throw DbError("SERIALIZATION_FAILURE","OCC relation changed after the snapshot (including predicate/phantom protection): "+table);
        }
    }
    const bool writes=std::any_of(pages.begin(),pages.end(),[](const PageCheck& p){return p.write;});
    if(writes&&tx->options.read_only)throw DbError("READ_ONLY_TRANSACTION","Read-only transaction contains private pages");
    if(writes&&(tx->options.protocol==TransactionProtocol::Timestamp||tx->options.protocol==TransactionProtocol::Thomas))
        for(const auto& table:tx->write_tables)if(tx->start_ts<read_ts_[table]||tx->start_ts<write_ts_[table]) {
            event(tx,"timestamp_commit_rejected",table,{{"read_timestamp",read_ts_[table]},{"write_timestamp",write_ts_[table]}});
            throw DbError("TIMESTAMP_CONFLICT","Timestamp order changed while the write remained private: "+table);
        }
    event(tx,"commit_validated","",{{"private_pages",pages.size()}});
    return CommitPermit(std::move(lock),tx,writes?csn_+1:csn_,writes);
}
void TransactionControl::complete_commit(CommitPermit& permit,std::uint64_t durable_lsn) {
    if(!permit.guard.owns_lock()||permit.completed)throw DbError("INTERNAL_ERROR","Invalid protocol commit permit");
    require(permit.context);auto tx=permit.context;
    if(permit.writes) {
        csn_=permit.csn;
        for(const auto& table:tx->write_tables){++epochs_[table];write_ts_[table]=std::max(write_ts_[table],tx->start_ts);}
    }
    event(tx,"commit","",{{"commit_csn",csn_},{"durable_lsn",durable_lsn},{"writes",permit.writes}});
    tx->committed=true;tx->active=false;active_.erase(tx->id);permit.completed=true;
}
void TransactionControl::finish(const Handle& tx,const std::string& reason) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if(!tx||!tx->active)return;
    require(tx);event(tx,reason);tx->active=false;active_.erase(tx->id);
}
std::uint64_t TransactionControl::committed_csn() const {std::lock_guard<std::recursive_mutex> lock(mutex_);return csn_;}
std::uint64_t TransactionControl::oldest_snapshot_csn() const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);auto oldest=csn_;
    for(const auto& item:active_)if(retains_committed_history(item.second->options))oldest=std::min(oldest,item.second->read_csn);
    return oldest;
}
bool TransactionControl::has_snapshot_readers(std::uint64_t excluding) const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for(const auto& item:active_)if(item.first!=excluding&&(retains_committed_history(item.second->options)))return true;
    return false;
}
TransactionControl::json TransactionControl::diagnostics() const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);json txs=json::array(),tables=json::array(),edges=json::array();std::set<std::uint64_t> nodes;
    for(const auto& x:active_) {auto t=x.second;nodes.insert(t->id);txs.push_back({{"tx_id",t->id},{"protocol",transaction_protocol_name(t->options.protocol)},{"read_only",t->options.read_only},{"read_committed",t->options.read_committed},{"record_locks",t->options.record_locks},{"effective_isolation",t->options.protocol==TransactionProtocol::Snapshot?(t->options.read_committed?"READ_COMMITTED":"SNAPSHOT_REPEATABLE_READ"):"SERIALIZABLE"},{"start_timestamp",t->start_ts},{"start_csn",t->start_csn},{"read_csn",t->read_csn},{"read_tables",t->read_tables},{"write_tables",t->write_tables},{"ignored_write_tables",t->ignored_write_tables},{"read_page_count",t->read_pages.size()}});}
    for(const auto& x:edges_){nodes.insert(x.first.first);nodes.insert(x.first.second);edges.push_back({{"from",x.first.first},{"to",x.first.second},{"resources",x.second}});}
    std::map<std::uint64_t,std::size_t> degree;std::map<std::uint64_t,std::vector<std::uint64_t>> adjacent;
    for(auto n:nodes)degree[n]=0;
    for(const auto& e:edges_){adjacent[e.first.first].push_back(e.first.second);++degree[e.first.second];}
    std::deque<std::uint64_t> ready;for(const auto& n:degree)if(!n.second)ready.push_back(n.first);
    std::size_t visited=0;while(!ready.empty()){auto n=ready.front();ready.pop_front();++visited;for(auto next:adjacent[n])if(--degree[next]==0)ready.push_back(next);}
    const bool cyclic=visited!=nodes.size();
    std::set<std::string> names;for(const auto& x:epochs_)names.insert(x.first);for(const auto& x:read_ts_)names.insert(x.first);for(const auto& x:write_ts_)names.insert(x.first);
    for(const auto& n:names)tables.push_back({{"table",n},{"mutation_epoch",epochs_.count(n)?epochs_.at(n):0},{"read_timestamp",read_ts_.count(n)?read_ts_.at(n):0},{"write_timestamp",write_ts_.count(n)?write_ts_.at(n):0}});
    return {{"committed_csn",csn_},{"active_transactions",txs},{"tables",tables},{"events",events_},{"events_truncated",events_truncated_},{"protocol_epoch",active_.empty()?"IDLE":transaction_protocol_name(active_.begin()->second->options.protocol)},
        {"conflict_graph",{{"edges",edges},{"cycle",cyclic},{"scope","coarse table access/intention order, including aborted transactions; conservative overapproximation, not the wait-for graph or a serializability proof"},{"trace_complete",!events_truncated_},{"is_serializability_proof",false}}},
        {"snapshot_conflicts","whole physical page first-committer-wins; sharing an index/descriptor page may conservatively abort disjoint logical rows"}};
}
} // namespace edudb
