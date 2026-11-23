#pragma once
#include "managed_block_device.hpp"
#include <functional>
#include <vector>
namespace edudb::recovery {
struct Limits {
    std::uint64_t data_bytes=2ull*1024*1024*1024;
    std::uint64_t wal_bytes=512ull*1024*1024;
    std::uint64_t timeout_ms=60000;
    // Finite controlled test hook. Production leaves empty. It is not a timer
    // model or a simulated success path; all copying/syncing remains native.
    std::function<void(const char*)> phase_hook;
    // replay_archive only: perform all identity/lineage/byte checks but no I/O
    // mutation. Receiver validates an unpublished staging file before admitting
    // it into its forced, contiguous receive history.
    bool validate_only=false;
    std::uint64_t archive_directory_bytes=2ull*1024*1024*1024;
    std::size_t archive_files=4096;
};
// Opens with OS read-only permissions and never upgrades access or syncs input.
std::unique_ptr<DataDevice> open_read_only(const std::string& filename);
// Caller holds the database writer/maintenance barrier, flushes all committed
// pages and synchronizes data first. This is a paused-write physical backup.
json create_backup(DataDevice& data,const json& source_control,std::uint64_t end_lsn,
                   const std::string& new_directory,Limits limits={});
// Caller holds the WAL mutex. Source must contain only complete committed
// transactions; publication of the checked archive manifest is last.
json archive_wal(DataDevice& wal,const std::string& database_uuid,const std::string& timeline,
                 const std::string& archive_directory,Limits limits={});
json inspect_wal(DataDevice& wal,const std::string& expected_uuid,Limits limits={});
// Sources are immutable. Destination is create-new. target=0 selects the last
// complete archived commit; it can never precede the base backup's end LSN.
json restore_backup(const std::string& backup_directory,const std::vector<std::string>& archive_manifests,
                    std::uint64_t target_commit_lsn,const std::string& new_directory,Limits limits={});
json inspect_backup(const std::string& backup_directory,Limits limits={});
json inspect_archive(const std::string& archive_manifest,Limits limits={});
void check_archive_capacity(const std::string& directory,const std::string& database_uuid,
                            std::uint64_t additional_bytes,std::size_t additional_files,Limits limits={});
// STANDBY only. Caller has active=0 maintenance barrier, flushed data, and WAL
// mutex. Force appended complete transactions before writing any data page;
// then persist returned control and invalidate buffers/caches before queries.
json replay_archive(DataDevice& data,DataDevice& active_wal,const json& control,
                    std::uint64_t current_end_lsn,const std::string& archive_manifest,Limits limits={});
} // namespace edudb::recovery
