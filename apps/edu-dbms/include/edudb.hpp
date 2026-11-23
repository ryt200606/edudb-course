#pragma once
#include "json.hpp"
#include "transaction_control.hpp"
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace edudb {
using json = nlohmann::json;
struct DbError : std::runtime_error {
    std::string code;
    int position;
    DbError(std::string c, std::string message, int p = -1)
        : std::runtime_error(std::move(message)), code(std::move(c)), position(p) {}
};
struct Transaction {
    std::uint64_t id = 0;
    struct State;
    std::shared_ptr<State> state;
};
struct RowId {
    std::uint64_t page_id = 0;
    std::uint16_t slot_id = 0;
    std::uint32_t generation = 0;
    std::uint64_t logical_id = 0;
    bool operator==(const RowId& b) const { return page_id==b.page_id && slot_id==b.slot_id && generation==b.generation && logical_id==b.logical_id; }
    bool operator<(const RowId& b) const { return page_id!=b.page_id?page_id<b.page_id:slot_id!=b.slot_id?slot_id<b.slot_id:generation!=b.generation?generation<b.generation:logical_id<b.logical_id; }
};
struct StoredRow { RowId id; json values; };
struct ScanSpec {
    std::string index;
    std::optional<std::int64_t> lower, upper;
    bool lower_inclusive = true, upper_inclusive = true;
    json lower_key = nullptr, upper_key = nullptr, condition = nullptr;
    std::vector<std::string> indexes;
    std::vector<int> required_columns;
    std::string mode;
    bool projection_known = false;
    bool lazy_blobs = false;
};
class RowCursor {
public:
    virtual ~RowCursor() = default;
    virtual bool next(StoredRow& row) = 0;
};
struct StorageOptions {
    // Empty means follow the durable manifest, or SINGLE for a new database.
    // A selected managed layout requires an explicit member count on creation.
    std::string data_layout;
    std::size_t media_members = 0;
    // Empty follows an existing manifest; new databases default REDO_PRIVATE.
    // Recovery modes cannot be switched in-place.
    std::string recovery_mode;
    std::optional<bool> aries_steal;
    std::optional<bool> aries_force_at_commit;
    // Internal native-test override. Production leaves empty for LocalAppData.
    std::string fencing_directory;
    std::function<void(const char*)> recovery_phase_hook;
};
// Format-3 table interchange preserves typed rows, stable row_ids, physical
// organization, and index definitions (method/columns/INCLUDE/options).
// INTEGER is int64; BLOB user values are {"$blob":"HEX"}.
// Callers hold catalog S / table S or X for the entire transaction. DDL holds catalog X.
class Storage {
public:
    static json migrate_v2(const std::string& source, const std::string& destination);
    static json migrate_v3(const std::string& source, const std::string& destination);
    Storage(const std::string& directory, std::size_t frames = 128,
            std::size_t private_limit = 32u * 1024u * 1024u,
            const StorageOptions& options = {});
    ~Storage();
    std::shared_ptr<Transaction> begin();
    std::shared_ptr<Transaction> begin(const TransactionOptions& options);
    TransactionControl::StatementView refresh_read_view(const std::shared_ptr<Transaction>& tx,
                                                       const std::vector<TransactionAccess>& accesses);
    void savepoint(const std::shared_ptr<Transaction>& tx, const std::string& name);
    void rollback_to(const std::shared_ptr<Transaction>& tx, const std::string& name);
    void release_savepoint(const std::shared_ptr<Transaction>& tx, const std::string& name);
    void commit(const std::shared_ptr<Transaction>& tx);
    void rollback(const std::shared_ptr<Transaction>& tx);
    json catalog(const std::shared_ptr<Transaction>& tx);
    json schema_features(const std::shared_ptr<Transaction>& tx);
    json read_table(const std::shared_ptr<Transaction>& tx, const std::string& name,
                    bool include_rows = true, bool include_physical = true);
    void set_statistics(const std::shared_ptr<Transaction>& tx, const std::string& name, const json& statistics);
    void enable_changefeed(const std::shared_ptr<Transaction>& tx, const std::vector<std::string>& tables);
    json read_changefeed(const std::shared_ptr<Transaction>& tx, const std::string& table = "", std::uint64_t after_seq = 0);
    void set_materialized_metadata(const std::shared_ptr<Transaction>& tx, const std::string& table, const json& refresh_state);
    std::pair<std::string,std::string> query_workspace() const;
    json create_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name);
    json attach_cluster(const std::shared_ptr<Transaction>& tx,const std::string& cluster,const std::string& table,const std::vector<std::string>& key_columns);
    json detach_cluster(const std::shared_ptr<Transaction>& tx,const std::string& table);
    void drop_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name);
    json clusters(const std::shared_ptr<Transaction>& tx);
    json inspect_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name,std::size_t maximum_pages=256);
    std::string cluster_lock_resource(const std::shared_ptr<Transaction>& tx,const std::string& table);

    void write_table(const std::shared_ptr<Transaction>& tx, const std::string& name,
                     const json& table);
    void drop_table(const std::shared_ptr<Transaction>& tx, const std::string& name);
    json index_scan(const std::shared_ptr<Transaction>& tx, const std::string& table,
                    const std::string& index, const std::string& op, std::int64_t key);
    std::unique_ptr<RowCursor> scan(const std::shared_ptr<Transaction>& tx,
                                  const std::string& table, const ScanSpec& spec = {});
    RowId insert_row(const std::shared_ptr<Transaction>& tx, const std::string& table, const json& row);
    RowId update_row(const std::shared_ptr<Transaction>& tx, const std::string& table, const RowId& id, const json& row);
    void erase_row(const std::shared_ptr<Transaction>& tx, const std::string& table, const RowId& id);
    json transaction_stats(const std::shared_ptr<Transaction>& tx);
    std::optional<std::uint64_t> count(const std::shared_ptr<Transaction>& tx, const std::string& table, const ScanSpec& spec);
    void reindex(const std::shared_ptr<Transaction>& tx, const std::string& table, const std::string& index = "");
    void set_indexes(const std::shared_ptr<Transaction>& tx, const std::string& table, const json& indexes);
    json inspect(const std::shared_ptr<Transaction>& tx, const std::string& table, const std::string& index, std::size_t max_pages = 2048);
    json blob_value(const std::shared_ptr<Transaction>& tx, const json& reference, std::uint64_t start = 1, std::uint64_t length = UINT64_MAX);
    json verify(const std::shared_ptr<Transaction>& tx);
    json diagnostics();
    json wal_records(std::size_t limit = 4096);
    bool recovery_required() const;
    bool read_only() const;
    bool logical_recovery_mode() const;
    json configure_buffer(const json& request);
    json media_scrub();
    json media_rebuild(std::size_t member);
    static json media_maintenance(const std::string& directory, const json& request);
    json backup(const std::string& label,int timeout_ms=5000);
    json archive_wal();
    json set_archiving(bool enabled,int timeout_ms=5000);
    json become_standby(int timeout_ms=5000);
    json replay_archive(const std::string& manifest,int timeout_ms=5000,bool validate_only=false);
    json promote(int timeout_ms=5000);
    json recovery_identity();
    json checkpoint(int timeout_ms = 5000);
private:
    std::vector<std::uint64_t> bitmap_selection(const std::shared_ptr<Transaction>& tx, std::uint64_t owner, json& descriptor, const ScanSpec& spec);
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class Engine {
public:
    Engine(const std::string& directory, std::size_t frames = 128,
           std::size_t private_limit = 32u * 1024u * 1024u,
           const StorageOptions& options = {});
    ~Engine();
    std::string instance_id() const;
    std::string open_session();
    json execute(const std::string& session, const std::string& sql);
    json execute_shared_batch(const std::string& session, const std::vector<std::string>& queries);
    json import_data(const std::string& session, const json& request);
    json export_data(const std::string& session, const json& request);
    json inspect(const std::string& session, const json& request);
    void close_session(const std::string& session);
    json diagnostics();
    json backup(const std::string& label,int timeout_ms=5000);
    json archive_wal();
    json set_archiving(bool enabled,int timeout_ms=5000);
    json become_standby(int timeout_ms=5000);
    json replay_archive(const std::string& manifest,int timeout_ms=5000,bool validate_only=false);
    json promote(int timeout_ms=5000);
    json recovery_identity();
    json checkpoint();
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace edudb
