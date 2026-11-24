#pragma once
#include "json.hpp"
#include <cstdint>
#include <memory>
#include <string>
namespace edudb::recovery {
using json=nlohmann::json;
struct WriterIdentity {std::string instance_id,timeline;std::uint64_t epoch=0;};
class FencePermit {
public:
    struct Impl;
    explicit FencePermit(std::unique_ptr<Impl> state);
    ~FencePermit();
    FencePermit(FencePermit&&)noexcept;
    FencePermit& operator=(FencePermit&&)noexcept;
    FencePermit(const FencePermit&)=delete;
    FencePermit& operator=(const FencePermit&)=delete;
    const json& state()const;
private:std::unique_ptr<Impl> impl_;
};
class FenceDomain {
public:
    // Empty directory selects the per-user, same-machine LocalAppData/EduDBFence
    // domain. Explicit paths are an internal test/C++ facility, never HTTP input.
    FenceDomain(const std::string& database_uuid,const std::string& directory={});
    ~FenceDomain();
    // Only epoch=0 may create an absent domain. Persist instance_id locally
    // BEFORE registering; do not silently replace a foreign owner.
    json register_primary(const WriterIdentity& identity,unsigned wait_ms=5000);
    FencePermit acquire_writer(const WriterIdentity& identity,unsigned wait_ms=5000);
    // Caller first establishes its local maintenance barrier (active=0), then
    // promotes while retaining this permit through local WAL/control switching.
    // A crash after global force can reduce availability, never grant two owners.
    FencePermit promote(const std::string& new_instance,const std::string& source_timeline,
                        std::uint64_t replayed_lsn,unsigned wait_ms=5000);
    json offer(const WriterIdentity& standby,std::uint64_t replayed,const std::string& key);
    json surrender(const WriterIdentity& owner,std::uint64_t durable,const json& offer,const std::string& key);
    FencePermit accept(const WriterIdentity& standby,std::uint64_t replayed,const json& ticket,const std::string& key,bool already_primary=false);
    json snapshot(unsigned wait_ms=5000)const;
    std::string directory()const;
private:struct Impl;std::unique_ptr<Impl> impl_;
};
}
