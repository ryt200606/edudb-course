#pragma once
#include "recovery_archive.hpp"
#include <cstdint>
#include <memory>
namespace edudb::recovery {
struct ReceiverOptions {
    std::string database_uuid,timeline,inbox_directory,token;
    std::string owner_directory;
    std::uint64_t base_lsn=0;
    unsigned port=0; // 0 asks the OS for an ephemeral loopback port.
    Limits limits;
    std::function<void(const char*)> phase_hook;
};
class StandbyReceiver {
public:
    // validate_only=true must check without mutating local WAL/data/control.
    using Replay=std::function<json(const std::string& archive_manifest,bool validate_only)>;
    StandbyReceiver(ReceiverOptions options,Replay apply);
    ~StandbyReceiver();
    void start();
    void stop();
    unsigned port()const;
    json status()const;
    StandbyReceiver(const StandbyReceiver&)=delete;
    StandbyReceiver& operator=(const StandbyReceiver&)=delete;
private:struct Impl;std::unique_ptr<Impl> impl_;
};
// Loopback only, bounded transfer, explicit call. It never retries an uncertain
// response. It transports immutable physical WAL, never re-executes SQL writes.
json ship_archive(const std::string& archive_manifest,unsigned port,const std::string& token,Limits limits={});
}
