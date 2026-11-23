// Inside Storage::Impl. Data plane state still belongs to its existing mutexes.
std::mutex backup_mutex;
std::string backup_role="PRIMARY",backup_instance,backup_timeline,backup_fence_directory,backup_error;
std::uint64_t backup_epoch=0;
bool backup_archive_enabled=false,backup_identity_upgrade=false;
std::atomic<bool> backup_fenced{false};
json backup_metadata=json::object();
std::unique_ptr<recovery::FenceDomain> backup_fence;
std::function<void(const char*)> backup_phase_hook;

// A proof of entry admission, not a held lease or a transaction-lifetime cache.
// Every public mutation scope performs a fresh OS ownership check, including
// nested public calls. Only its internal page writes may reuse that check.
struct BackupWriteOperation {
    const void* previous_storage;const void* previous_state;
    BackupWriteOperation(Impl& storage,const std::shared_ptr<Transaction>& tx)
        :previous_storage(verified_write_storage),previous_state(verified_write_state){
        storage.require(tx);storage.require_writable();storage.backup_write_guard();
        verified_write_storage=&storage;verified_write_state=tx->state.get();
    }
    ~BackupWriteOperation()noexcept{verified_write_storage=previous_storage;verified_write_state=previous_state;}
    BackupWriteOperation(const BackupWriteOperation&)=delete;
    BackupWriteOperation& operator=(const BackupWriteOperation&)=delete;
};
