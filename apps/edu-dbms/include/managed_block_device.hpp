#pragma once
#include "edudb.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace edudb {
class DataDevice {
public:
    virtual ~DataDevice()=default;
    virtual std::uint64_t size()=0;
    virtual void read(std::uint64_t offset,unsigned char* destination,std::size_t length)=0;
    virtual void write(std::uint64_t offset,const unsigned char* source,std::size_t length)=0;
    virtual void sync()=0;
};
struct ManagedMediaOptions {
    // RAID2 selects actual Hamming bit lanes (7,4) or extended (8,4).
    // Member files share one host; they are not independent physical drives.
    std::string layout="RAID1";
    std::size_t members=2;
};
class ManagedBlockDevice final:public DataDevice {
public:
    ManagedBlockDevice(const std::string& directory,const std::string& database_identity,
                       const ManagedMediaOptions& options,bool create);
    ~ManagedBlockDevice();
    std::uint64_t size()override;
    void read(std::uint64_t offset,unsigned char* destination,std::size_t length)override;
    void write(std::uint64_t offset,const unsigned char* source,std::size_t length)override;
    void sync()override;
    json configuration()const;
    json diagnostics()const;
    json scrub(bool repair=false);
    json rebuild(std::size_t member);
    ManagedBlockDevice(const ManagedBlockDevice&)=delete;
    ManagedBlockDevice& operator=(const ManagedBlockDevice&)=delete;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace edudb
