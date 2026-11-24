#include "edudb.hpp"
#include "tree_control.hpp"
#include "access.hpp"
#include "io.hpp"
#include "input_limits.hpp"
#include "interchange.hpp"
#include "input_limits.hpp"
#include "query_operators.hpp"
#include "query_relational.hpp"
#include "query_semijoin.hpp"
#include "query_topk.hpp"
#include "query_reuse.hpp"
#include "materialized_delta.hpp"
#include "query_spill.hpp"
#include "lock_manager.hpp"
#include "execution_progress.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace edudb {
namespace {
using Clock = std::chrono::steady_clock;
std::string make_instance_id() {
    static std::atomic<std::uint64_t> instances{0};
    std::random_device entropy;
    std::ostringstream value;
    value<<'i'<<std::hex<<std::setfill('0');
    for(int i=0;i<4;++i)value<<std::setw(8)<<static_cast<std::uint32_t>(entropy());
    value<<'-'<<std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count()
         <<'-'<<++instances;
    return value.str();
}
#include "sql_frontend.inc"
#include "sql_planner.inc"
#include "sql_shared_scan.inc"

} // anonymous namespace

struct Engine::Impl {
    Storage storage;LockManager locks;const bool concurrent_commits;
    std::mutex tree_topology_mutex;std::map<std::string,std::weak_ptr<const LockTreeTopology>> tree_topologies;
    const std::string instance=make_instance_id();
    const QueryLimits query_limits=query_limits_from_environment();
    const std::chrono::milliseconds idle_timeout;
    struct Session {std::mutex mutex;std::string id;std::shared_ptr<Transaction> tx;bool explicit_tx=false,aborted=false,closed=false,record_locks=false,range_candidate=false;Clock::time_point touched=Clock::now();json next_transaction=json::object(),current_locking=json::object();TransactionOptions active_options;LockTreeState tree;std::vector<TransactionAccess> current_accesses;};
    std::mutex sessions_mutex,cache_mutex;std::map<std::string,std::shared_ptr<Session>> sessions;std::atomic<std::uint64_t> sequence{0};std::atomic<bool> stopping{false};std::thread reaper;json table_cache=json::array();bool table_cache_stale=true,table_cache_truncated=false;std::string table_cache_note="not refreshed";
    Impl(const std::string& dir,std::size_t frames,std::size_t limit,const StorageOptions& options)
        :storage(dir,frames,limit,options),
         locks(timeout_from_environment("EDUDB_LOCK_TIMEOUT_MS",5000)),
         concurrent_commits(options.wal_group_size>1),
         idle_timeout(timeout_from_environment("EDUDB_IDLE_TIMEOUT_MS",600000)) {
        reaper=std::thread([this]{
            const auto interval=std::min(std::chrono::milliseconds(100),
                std::max(std::chrono::milliseconds(1),idle_timeout/4));
            while(!stopping.load()) {
                std::this_thread::sleep_for(interval);
                std::vector<std::shared_ptr<Session>> snapshots;
                {std::lock_guard<std::mutex> guard(sessions_mutex);for(auto& entry:sessions)snapshots.push_back(entry.second);}
                for(auto& session:snapshots) {
                    std::unique_lock<std::mutex> guard(session->mutex,std::try_to_lock);
                    if(guard.owns_lock()&&session->tx&&(locks.cancellation_token(session->tx->id)->load()||Clock::now()-session->touched>idle_timeout)) {
                        try{rollback(*session);}catch(...){}
                        session->aborted=true;
                    }
                }
            }
        });
    }
    ~Impl(){stopping=true;if(reaper.joinable())reaper.join();std::lock_guard<std::mutex> g(sessions_mutex);for(auto& s:sessions){std::lock_guard<std::mutex> sg(s.second->mutex);try{rollback(*s.second);}catch(...){}}}
    std::shared_ptr<Session> get(const std::string& id){std::lock_guard<std::mutex> g(sessions_mutex);auto it=sessions.find(id);if(it==sessions.end())fail("SESSION_NOT_FOUND","Unknown or closed session");return it->second;}
    std::string state(const Session& s)const{return s.aborted?"aborted":s.tx?"active":"idle";}
    void rollback(Session& s){if(s.tx){auto id=s.tx->id;try{storage.rollback(s.tx);}catch(...){locks.release(id);s.tree.clear();s.tx.reset();s.explicit_tx=false;s.aborted=false;throw;}locks.release(id);s.tx.reset();}s.tree.clear();s.explicit_tx=false;s.aborted=false;}
    void commit(Session& s){
        auto tx=s.tx;auto permit=locks.prepare_commit(tx->id,concurrent_commits);
        try{storage.commit(tx);}catch(...){locks.commit_failed(permit,storage.recovery_required());throw;}
        locks.commit_succeeded(permit);locks.release(tx->id);s.tree.clear();s.tx.reset();s.explicit_tx=false;s.aborted=false;
    }
    static bool schema_statement(const Statement& st){return st.kind=="CREATE_STATISTICS"||st.kind=="DROP_STATISTICS"||st.kind=="CREATE_CLUSTER"||st.kind=="DROP_CLUSTER"||st.kind=="CREATE_TABLE"||st.kind=="DROP_TABLE"||st.kind=="ALTER_TABLE"||st.kind=="CREATE_INDEX"||st.kind=="DROP_INDEX"||st.kind=="REINDEX"||st.kind=="REORGANIZE"||st.kind=="CREATE_MATERIALIZED_VIEW"||st.kind=="DROP_MATERIALIZED_VIEW"||st.kind=="REFRESH_MATERIALIZED_VIEW";}
    static std::string isolation(const TransactionOptions& options){return options.protocol==TransactionProtocol::Snapshot?(options.read_committed?"READ_COMMITTED":"SNAPSHOT"):"SERIALIZABLE";}
    TransactionOptions transaction_options(const json& configuration){
        TransactionOptions options;const bool explicit_protocol=configuration.contains("protocol");
        if(explicit_protocol)options.protocol=parse_transaction_protocol(configuration.at("protocol").get<std::string>());
        options.read_only=configuration.value("read_only",false);
        options.record_locks=configuration.value("record_locks",false);
        auto requested=configuration.value("isolation",std::string());
        if(requested=="READ_COMMITTED"||requested=="READ_UNCOMMITTED"){
            if(explicit_protocol&&options.protocol!=TransactionProtocol::Snapshot)fail("INVALID_TRANSACTION_OPTIONS","Statement snapshots require the SNAPSHOT protocol");
            options.protocol=TransactionProtocol::Snapshot;options.read_committed=true;
        }else if(requested=="REPEATABLE_READ"){
            if(!explicit_protocol)options.protocol=TransactionProtocol::Snapshot;
            options.read_committed=false;
        }else if(requested=="SERIALIZABLE"){
            if(options.protocol==TransactionProtocol::Snapshot)fail("INVALID_TRANSACTION_OPTIONS","SNAPSHOT alone is not serializable; use OCC_SERIALIZABLE or STRICT_2PL");
        }else if(!requested.empty())fail("INVALID_TRANSACTION_OPTIONS","Unknown isolation level");
        if(options.record_locks&&options.protocol!=TransactionProtocol::Strict2PL)fail("INVALID_LOCK_GRANULARITY","RECORD locking requires STRICT_2PL");
        return options;
    }
    void start_transaction(Session& s,const json& overrides=json::object()){
        auto configuration=s.next_transaction;configuration["record_locks"]=s.record_locks;for(auto it=overrides.begin();it!=overrides.end();++it)configuration[it.key()]=it.value();
        auto options=transaction_options(configuration);if(storage.read_only())options.read_only=true;auto tx=storage.begin(options);
        s.tx=std::move(tx);s.active_options=options;s.next_transaction=json::object();s.current_accesses.clear();
    }
    bool before_statement(Session& s,const Statement& st){
        s.range_candidate=range_shape(st);s.current_locking={{"requested",s.active_options.record_locks?"RECORD":"TABLE"},{"effective","TABLE"},{"reason",s.active_options.record_locks?"command or predicate requires coarse locking":"default or explicit TABLE setting"}};
        if(s.active_options.protocol==TransactionProtocol::Mvto)s.current_locking={{"requested","MVTO"},{"effective","VERSION_WTS_RTS"},{"reason","actual per-page-version WTS selection / RTS validation before WAL; no user table or cluster S/X"}};
        if(s.active_options.protocol==TransactionProtocol::Mv2pl)s.current_locking={{"requested","MV2PL"},{"effective",s.active_options.read_only?"COMMITTED_VERSION_READ":"STRICT_TABLE_2PL"},{"reason",s.active_options.read_only?"fixed BEGIN committed CSN; no user-table or cluster S lock":"updater holds real S/X and cluster locks to transaction end"},{"read_only",s.active_options.read_only}};
        std::map<std::string,TransactionAccess> accesses;
        auto add=[&](const std::string& name,bool read,bool write,bool blind=false){if(name.empty())return;auto& a=accesses[name];a.table=name;a.read=a.read||read;a.write=a.write||write;a.blind_write=blind;};
        const bool ddl=schema_statement(st);add("__catalog__",true,ddl);
        if(st.kind=="CREATE_MATERIALIZED_VIEW"){add(st.table,true,true);for(const auto& name:st.read_tables)add(name,true,false);}
        else if(st.kind=="SELECT"){for(const auto& source:st.sources)add(source.table,true,false);for(const auto& name:st.read_tables)add(name,true,false);}
        else if(st.kind=="TRUNCATE")add(st.table,false,true,true);
        else if(st.kind=="INSERT"||st.kind=="UPDATE"||st.kind=="DELETE"||st.kind=="ANALYZE"||ddl)add(st.table,true,true);
        else add(st.table,true,false);
        tree_guard(s,st);
        s.current_accesses.clear();for(const auto& item:accesses){if(item.second.write&&storage.read_only())fail("READ_ONLY_DATABASE","Legacy control3 is read-only; use offline migrate-v3 into a new directory");s.current_accesses.push_back(item.second);}
        locks.check_cancelled(s.tx->id);return storage.refresh_read_view(s.tx,s.current_accesses).ignore_write;
    }
    void acquire(Session& s,const std::string& r,bool x){
        const auto protocol=s.active_options.protocol;
        const bool table_resource=r.rfind("table:",0)==0;const bool data_resource=table_resource||r.rfind("cluster:",0)==0;
        if(data_resource&&protocol!=TransactionProtocol::Strict2PL&&
           !(protocol==TransactionProtocol::Mv2pl&&!s.active_options.read_only)&&
           !(protocol==TransactionProtocol::Snapshot&&s.active_options.read_committed&&x))return;
        if(table_resource){
            if(x&&protocol==TransactionProtocol::Snapshot&&s.active_options.read_committed&&!s.current_accesses.empty())storage.refresh_read_view(s.tx,s.current_accesses);
            std::string cluster;
            try{cluster=storage.cluster_lock_resource(s.tx,r.substr(6));}
            catch(const DbError& error){if(error.code!="TABLE_NOT_FOUND")throw;}
            // A newly created relation still needs its name lock before its
            // descriptor exists. Actual reads/DML validate existence later.
            if(!cluster.empty())acquire(s,cluster,x);
            locks.register_resource("database-data");locks.register_resource(r,"database-data");locks.acquire_path(s.tx->id,r,x?LockMode::X:LockMode::S);}
        else locks.acquire(s.tx->id,r,x);
        if(r.rfind("table:",0)==0){auto snapshot=locks.snapshot();for(const auto& held:snapshot["locks"])if(held["resource"]==r&&held["tx_id"]==s.tx->id)s.current_locking["coarse_table_held"][r.substr(6)]=held["mode"];}
        // A lock wait may outlive another commit. RC and timestamp protocols
        // take their statement view after acquiring the write/catalog lock.
        if(!s.current_accesses.empty()&&(tree_statement_snapshot(s.active_options)||(protocol==TransactionProtocol::Mv2pl&&!s.active_options.read_only)||s.active_options.record_locks||(x&&((protocol==TransactionProtocol::Snapshot&&s.active_options.read_committed)||protocol==TransactionProtocol::Timestamp||protocol==TransactionProtocol::Thomas))))
            storage.refresh_read_view(s.tx,s.current_accesses);
    }
    bool try_acquire_table(Session& s,const std::string& name){
        try{
            auto cluster=storage.cluster_lock_resource(s.tx,name);
            if(!cluster.empty()&&!locks.acquire(s.tx->id,cluster,false,true))return false;
            auto resource="table:"+name;locks.register_resource("database-data");locks.register_resource(resource,"database-data");
            return locks.acquire_path(s.tx->id,resource,LockMode::S,true);
        }catch(const DbError& error){if(error.code=="TABLE_NOT_FOUND")return false;throw;}
    }
    void refresh_cache(){
        std::shared_ptr<Transaction> tx;
        auto skipped=[&](const std::string& reason){std::lock_guard<std::mutex> guard(cache_mutex);table_cache_stale=true;table_cache_note=reason;};
        try{
            tx=storage.begin();
            if(!locks.acquire(tx->id,"catalog",false,true)){storage.rollback(tx);locks.release(tx->id);skipped("catalog locked");return;}
            json out=json::array();bool partial=false;auto cat=storage.catalog(tx);std::size_t encoded=2;bool capped=false;
            for(auto it=cat.begin();it!=cat.end();++it){
                auto cluster=storage.cluster_lock_resource(tx,it.key());if(!cluster.empty()&&!locks.acquire(tx->id,cluster,false,true)){partial=true;continue;}
                auto resource="table:"+it.key();locks.register_resource("database-data");locks.register_resource(resource,"database-data");
                if(!locks.acquire_path(tx->id,resource,LockMode::S,true)){partial=true;continue;}
                auto t=storage.read_table(tx,it.key(),false,false);
                if(t.contains("materialized_aux"))continue;
                json entry={{"name",it.key()},{"columns",t["columns"]},{"indexes",t["indexes"]},{"row_count",t.value("row_count",std::size_t(0))},{"is_materialized_view",materialized_metadata(t)}};
                if(materialized_metadata(t))entry["materialized_view"]=t.at("materialized_view");if(storage.logical_recovery_mode())entry["row_count_scope"]="committed under a coarse shared guard; locked pending tables are omitted and cache is marked stale";
                const auto bytes=entry.dump().size()+1;if(bytes>8u*1024u*1024u-encoded){partial=true;capped=true;break;}encoded+=bytes;out.push_back(std::move(entry));
            }
            storage.rollback(tx);locks.release(tx->id);std::lock_guard<std::mutex> guard(cache_mutex);
            table_cache=std::move(out);table_cache_stale=partial;table_cache_truncated=capped;table_cache_note=capped?"encoded catalog cache reached8MiB":partial?"locked tables omitted":"current committed catalog";
        }catch(const DbError& error){if(tx){try{storage.rollback(tx);}catch(...){}locks.release(tx->id);}skipped(error.code);}
         catch(...){if(tx){try{storage.rollback(tx);}catch(...){}locks.release(tx->id);}skipped("catalog refresh failed");}
    }
    bool exists(Session& s,const std::string& name){return storage.catalog(s.tx).contains(name);}
    json table(Session& s,const std::string& name,bool rows=true){if(!exists(s,name))fail("TABLE_NOT_FOUND","Table not found: "+name);return storage.read_table(s.tx,name,rows);}
    bool index_exists(Session& s,const std::string& name,std::string* owner=nullptr){auto cat=storage.catalog(s.tx);for(auto i=cat.begin();i!=cat.end();++i){acquire(s,"table:"+i.key(),false);auto t=storage.read_table(s.tx,i.key(),false,false);for(auto& x:t["indexes"])if(x["name"]==name){if(owner)*owner=i.key();return true;}}return false;}
#include "sql_range_locks.inc"
#include "sql_decorrelation.inc"
#include "sql_self_join.inc"
#include "sql_topk.inc"
#include "sql_executor.inc"
#include "sql_materialized_delta.inc"
#include "sql_materialized.inc"
#include "sql_materialized_rewrite.inc"
#include "sql_integrity.inc"
#include "sql_mutation.inc"
#include "sql_cluster.inc"
#include "sql_tree.inc"
#include "sql_ddl.inc"

};

Engine::Engine(const std::string& dir,std::size_t frames,std::size_t limit,const StorageOptions& options):impl_(new Impl(dir,frames,limit,options)){}
Engine::~Engine()=default;
std::string Engine::instance_id() const{return impl_->instance;}
std::string Engine::open_session(){auto s=std::make_shared<Impl::Session>();s->record_locks=impl_->storage.logical_recovery_mode();std::ostringstream id;id<<impl_->instance<<"-s"<<std::hex<<std::setw(16)<<std::setfill('0')<<++impl_->sequence;s->id=id.str();std::lock_guard<std::mutex> g(impl_->sessions_mutex);impl_->sessions[s->id]=s;return s->id;}
void Engine::close_session(const std::string& id){std::shared_ptr<Impl::Session>s;{std::lock_guard<std::mutex> g(impl_->sessions_mutex);auto it=impl_->sessions.find(id);if(it==impl_->sessions.end())return;s=it->second;impl_->sessions.erase(it);}std::lock_guard<std::mutex> g(s->mutex);s->closed=true;impl_->rollback(*s);}
json Engine::execute(const std::string& id, const std::string& sql) {
    const auto started = Clock::now();
    IoScope io_scope("sql");
    std::shared_ptr<Impl::Session> session;
    std::shared_ptr<Transaction> measured_transaction;
    std::unique_lock<std::mutex> guard;
    json before = json::object(), result;
    bool refresh = false, analyze = false;
    try {
        session = impl_->get(id);
        guard = std::unique_lock<std::mutex>(session->mutex);
        if (session->closed) fail("SESSION_NOT_FOUND", "Session has been closed");
        session->touched = Clock::now();
        auto statement = Parser(sql).parse();
        if(impl_->storage.logical_recovery_mode()){
            if(Impl::schema_statement(statement)&&session->explicit_tx)fail("LOGICAL_DDL_AUTOCOMMIT","Logical mode DDL requires its own automatic physical transaction");
            const std::set<std::string> unavailable={"CREATE_STATISTICS","DROP_STATISTICS","ALTER_TABLE","REINDEX","REORGANIZE","TRUNCATE","ANALYZE","CREATE_CLUSTER","DROP_CLUSTER","CREATE_MATERIALIZED_VIEW","DROP_MATERIALIZED_VIEW","REFRESH_MATERIALIZED_VIEW"};
            if(unavailable.count(statement.kind))fail("LOGICAL_UNSUPPORTED","Command is unavailable in bounded ARIES_LOGICAL experiment mode");
        }
        analyze = statement.analyze;
        if (session->aborted && statement.kind != "ROLLBACK" && statement.kind != "ROLLBACK_TO")
            fail("TRANSACTION_ABORTED", "Transaction is aborted; execute ROLLBACK");
        result = blank_result();
        auto measure = [&] {
            measured_transaction = session->tx;
            if (measured_transaction) before = impl_->storage.transaction_stats(measured_transaction);
        };
        if (statement.kind == "BEGIN") {
            if (session->tx) fail("TRANSACTION_ACTIVE", "Nested transactions are unsupported");
            impl_->start_transaction(*session,statement.options); session->explicit_tx = true; measure();
        } else if (statement.kind == "SET_TRANSACTION") {
            if(session->tx)fail("TRANSACTION_ACTIVE","SET TRANSACTION configures the next transaction and must precede BEGIN");
            auto configuration=session->next_transaction;configuration["record_locks"]=session->record_locks;for(auto it=statement.options.begin();it!=statement.options.end();++it)configuration[it.key()]=it.value();
            auto options=impl_->transaction_options(configuration);session->next_transaction=configuration;
            result["plan"]={{"node","NextTransactionConfiguration"},{"protocol",transaction_protocol_name(options.protocol)},{"effective_isolation",Impl::isolation(options)},{"read_only",options.read_only}};
        } else if(statement.kind=="SET_LOCK_GRANULARITY"){
            if(session->tx)fail("TRANSACTION_ACTIVE","SET LOCK GRANULARITY requires an idle session");
            auto granularity=statement.options.at("granularity").get<std::string>();if(granularity!="TABLE"&&granularity!="RECORD")fail("INVALID_LOCK_GRANULARITY","Use TABLE or RECORD");
            auto configuration=session->next_transaction;configuration["record_locks"]=granularity=="RECORD";impl_->transaction_options(configuration);
            if(granularity=="RECORD"&&impl_->storage.diagnostics().value("recovery_mode",std::string())=="ARIES_PAGE")fail("ARIES_RECORD_UNSUPPORTED","ARIES_PAGE full-page UNDO requires coarse TABLE locks");
            session->record_locks=granularity=="RECORD";result["plan"]={{"node","SessionLockGranularity"},{"granularity",granularity},{"scope","following transactions until changed; current session only"}};
        } else if (statement.kind == "SAVEPOINT" || statement.kind == "ROLLBACK_TO" || statement.kind == "RELEASE") {
            if(!session->tx||!session->explicit_tx)fail("NO_TRANSACTION","Savepoint operations require an explicit active transaction");
            measure();
            if(session->active_options.protocol==TransactionProtocol::Tree)fail("TREE_UNSUPPORTED","TREE savepoints are explicitly unsupported; full ROLLBACK preserves protocol legality");
            if(statement.kind=="SAVEPOINT")impl_->storage.savepoint(session->tx,statement.table);
            else if(statement.kind=="ROLLBACK_TO"){impl_->storage.rollback_to(session->tx,statement.table);session->aborted=false;}
            else impl_->storage.release_savepoint(session->tx,statement.table);
            result["plan"]={{"node",statement.kind},{"savepoint",statement.table}};
        } else if (statement.kind == "ROLLBACK") {
            measure(); impl_->rollback(*session); refresh = true;
        } else if (statement.kind == "COMMIT") {
            if (!session->tx) fail("NO_TRANSACTION", "No active transaction");
            measure(); impl_->commit(*session); refresh = true;
        } else if(statement.kind=="SET_BUFFER"||statement.kind=="SET_LOCK_POLICY"){
            if(session->tx)fail("TRANSACTION_ACTIVE","Instance configuration requires an idle session");
            if(statement.kind=="SET_BUFFER")result["configuration"]=impl_->storage.configure_buffer(statement.options);
            else{
                const auto name=statement.options.at("policy").get<std::string>();DeadlockPolicy policy;
                if(name=="DETECT")policy=DeadlockPolicy::Detect;else if(name=="WAIT_DIE")policy=DeadlockPolicy::WaitDie;else if(name=="WOUND_WAIT")policy=DeadlockPolicy::WoundWait;else fail("INVALID_DEADLOCK_POLICY","Use DETECT, WAIT_DIE or WOUND_WAIT");
                impl_->locks.set_policy(policy);result["configuration"]={{"deadlock_policy",LockManager::policy_name(policy)}};
            }
            result["plan"]={{"node",statement.kind=="SET_BUFFER"?"ConfigureBuffer":"ConfigureDeadlockPolicy"},{"scope","instance; runtime setting is not persisted"}};
        } else if(statement.kind=="SHOW_RECOVERY"){
            result["recovery"]=impl_->storage.recovery_identity();result["plan"]={{"node","RecoveryIdentity"}};
        } else if(statement.kind=="BACKUP_DATABASE"||statement.kind=="ARCHIVE_WAL"||statement.kind=="SET_ARCHIVING"||statement.kind=="PROMOTE_STANDBY"||statement.kind=="BECOME_STANDBY"){
            if(session->tx)fail("TRANSACTION_ACTIVE","Recovery maintenance must run outside a transaction");
            if(statement.kind=="BACKUP_DATABASE"){result["recovery"]=impl_->storage.backup(statement.table);result["plan"]={{"node","PausedWritePhysicalBackup"},{"label",statement.table}};}
            else if(statement.kind=="ARCHIVE_WAL"){result["recovery"]=impl_->storage.archive_wal();result["plan"]={{"node","ImmutableWalArchive"}};}
            else if(statement.kind=="SET_ARCHIVING"){result["recovery"]=impl_->storage.set_archiving(statement.options.at("enabled").get<bool>());result["plan"]={{"node","ConfigureWalArchiving"}};}
            else if(statement.kind=="PROMOTE_STANDBY"){result["recovery"]=impl_->storage.promote();result["plan"]={{"node","PromoteStandby"}};refresh=true;}
            else{result["recovery"]=impl_->storage.become_standby();result["plan"]={{"node","BecomeStandby"}};refresh=true;}
        } else if (statement.kind == "CHECKPOINT") {
            if (session->tx) fail("TRANSACTION_ACTIVE", "CHECKPOINT must run outside a transaction");
            result["metrics"]["checkpoint"] = impl_->storage.checkpoint();
        } else {
            if (!session->tx) impl_->start_transaction(*session);
            measure();
            if(impl_->before_statement(*session,statement)){
                result=blank_result();result["plan"]={{"node","ThomasIgnoredObsoleteWrite"},{"table",statement.table},{"ignored",true}};
            }else result = impl_->run(*session, statement);
            result["locking"]=session->current_locking;if(result["plan"].is_object()){result["plan"]["locking"]=session->current_locking;if(statement.explain)result["rows"]=json::array({json::array({result["plan"].dump(2)})});}
            if (!session->explicit_tx) {
                impl_->commit(*session); refresh = statement.kind != "SELECT";
            }
        }
        if(result.contains("recovery")&&result["recovery"].is_object()){
            result["columns"]=json::array({{{"name","property"},{"type","VARCHAR"}},{{"name","value"},{"type","VARCHAR"}}});
            for(auto item=result["recovery"].begin();item!=result["recovery"].end();++item)if(item.value().is_primitive())result["rows"].push_back(json::array({item.key(),item.value().is_string()?item.value().get<std::string>():item.value().dump()}));
        }
        result["transaction_state"] = impl_->state(*session);
        if(measured_transaction)result["transaction_options"]={{"protocol",transaction_protocol_name(session->active_options.protocol)},{"effective_isolation",Impl::isolation(session->active_options)},{"read_only",session->active_options.read_only},{"lock_granularity",session->active_options.record_locks?"RECORD":"TABLE"}};
        if (session->tx) result["tx_id"] = session->tx->id;
    } catch (const DbError& error) {
        if (session) {
            if (!session->explicit_tx || error.code == "DEADLOCK" || error.code == "LOCK_TIMEOUT") {
                bool was_explicit = session->explicit_tx || session->aborted;
                try { impl_->rollback(*session); } catch (...) {}
                session->aborted = was_explicit;
            } else session->aborted = true;
        }
        result = blank_result(session ? impl_->state(*session) : "idle");
        result["ok"] = false;
        result["error"] = {{"code", error.code}, {"message", error.what()}, {"position", error.position}};
    } catch (const std::exception& error) {
        if (session) {
            bool was_explicit = session->explicit_tx;
            try { impl_->rollback(*session); } catch (...) {}
            session->aborted = was_explicit;
        }
        result = blank_result(session ? impl_->state(*session) : "idle");
        result["ok"] = false;
        result["error"] = {{"code", "INTERNAL_ERROR"}, {"message", error.what()}, {"position", -1}};
    }
    if (measured_transaction) {
        try {
            auto after = impl_->storage.transaction_stats(measured_transaction);
            for (const char* key : {"heap_page_reads", "index_page_reads", "wal_bytes", "row_fetches", "cover_hits", "bitmap_words"}) {
                auto previous = before.value(key, std::uint64_t(0));
                auto current = after.value(key, std::uint64_t(0));
                result["metrics"][key] = current >= previous ? current - previous : 0;
            }
            result["metrics"]["dirty_pages"] = after.value("dirty_pages", std::uint64_t(0));
            result["metrics"]["private_bytes"] = after.value("private_bytes", std::uint64_t(0));
            result["metrics"]["io_scope"] = "this statement, transaction-local counters";
            result["metrics"]["dirty_scope"] = "transaction write-set pages at statement completion";
            if (analyze && result.value("ok", false) && result["plan"].is_object()) {
                for (const char* key : {"heap_page_reads", "index_page_reads", "wal_bytes", "dirty_pages", "row_fetches", "cover_hits", "bitmap_words"})
                    result["plan"]["actual"][key] = result["metrics"][key];
                result["rows"] = json::array({json::array({result["plan"].dump(2)})});
            }
        } catch (const std::exception&) {
            result["metrics"]["io_scope"] = "unavailable after failed transaction cleanup";
        }
    }
    if (session) session->touched = Clock::now();
    if (guard.owns_lock()) guard.unlock();
    if (refresh) impl_->refresh_cache();
    result["metrics"]["elapsed_ms"] = std::chrono::duration<double, std::milli>(Clock::now() - started).count();
    result["metrics"]["io"]=io_scope.snapshot();
    return result;
}

#include "sql_interchange.inc"
#include "sql_shared_batch.inc"

json Engine::diagnostics(){impl_->refresh_cache();auto out=impl_->locks.snapshot();out["query_limits"]=impl_->query_limits.describe();out["storage"]=impl_->storage.diagnostics();out["sessions"]=json::array();std::vector<std::shared_ptr<Impl::Session>> ss;{std::lock_guard<std::mutex> g(impl_->sessions_mutex);for(auto& s:impl_->sessions)ss.push_back(s.second);}for(auto& s:ss){std::unique_lock<std::mutex> g(s->mutex,std::try_to_lock);if(g.owns_lock())out["sessions"].push_back({{"id",s->id},{"transaction_state",impl_->state(*s)},{"tx_id",s->tx?json(s->tx->id):json(nullptr)}});else out["sessions"].push_back({{"id",s->id},{"transaction_state","executing"},{"tx_id",nullptr}});}std::lock_guard<std::mutex> g(impl_->cache_mutex);out["tables"]=impl_->table_cache;out["table_cache_stale"]=impl_->table_cache_stale;out["table_cache_truncated"]=impl_->table_cache_truncated;out["table_cache_note"]=impl_->table_cache_note;return out;}
json Engine::checkpoint(){return impl_->storage.checkpoint();}
json Engine::backup(const std::string& label,int timeout_ms){return impl_->storage.backup(label,timeout_ms);}
json Engine::archive_wal(){return impl_->storage.archive_wal();}
json Engine::set_archiving(bool enabled,int timeout_ms){return impl_->storage.set_archiving(enabled,timeout_ms);}
json Engine::become_standby(int timeout_ms){auto result=impl_->storage.become_standby(timeout_ms);impl_->refresh_cache();return result;}
json Engine::replay_archive(const std::string& manifest,int timeout_ms,bool validate_only){auto result=impl_->storage.replay_archive(manifest,timeout_ms,validate_only);if(!validate_only)impl_->refresh_cache();return result;}
json Engine::promote(int timeout_ms){auto result=impl_->storage.promote(timeout_ms);impl_->refresh_cache();return result;}
json Engine::recovery_identity(){return impl_->storage.recovery_identity();}

} // namespace edudb
