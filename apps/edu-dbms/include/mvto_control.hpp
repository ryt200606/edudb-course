#pragma once
#include "json.hpp"
#include <cstdint>
#include <map>
#include <list>
#include <optional>
#include <set>
#include <vector>
namespace edudb {
// Protected by Storage's coordinator across read, validation, WAL and publish.
// This is actual per-physical-version WTS/RTS metadata, not SI/CSN labels.
class PageMvto {
public:
    struct Version {std::uint64_t lsn=0,wts=0,rts=0;};
    explicit PageMvto(std::size_t maximum_slots=131072):limit_(maximum_slots){}
    void reset();
    void set_limit(std::size_t maximum_slots);
    void ensure_current(std::uint64_t page,std::uint64_t lsn);
    std::optional<std::uint64_t> select(std::uint64_t page,const std::vector<std::uint64_t>& available,std::uint64_t timestamp) const;
    void read(std::uint64_t page,std::uint64_t lsn,std::uint64_t timestamp);
    void validate_write(std::uint64_t page,std::uint64_t base_lsn,std::uint64_t current_lsn,std::uint64_t timestamp) const;
    void prepare_publish(const std::vector<std::uint64_t>& pages,bool no_other_readers=false);
    void publish(std::uint64_t page,std::uint64_t lsn,std::uint64_t timestamp);
    void collect(const std::map<std::uint64_t,std::set<std::uint64_t>>& retained_history);
    nlohmann::json diagnostics() const;
private:
    // Reserve one actual list node before WAL, splice it without allocation at
    // publication. Erased historical stamps free nodes rather than keeping an
    // ever-growing vector capacity outside the advertised metadata bound.
    struct Page {bool current_known=false,replace_on_publish=false;std::uint64_t current=0;std::list<Version> versions,pending;};
    std::map<std::uint64_t,Page> pages_;
    std::size_t limit_,version_count_=0,reserved_slots_=0;
    std::uint64_t reads_=0,published_=0;
    const Version& version(std::uint64_t page,std::uint64_t lsn) const;
    std::size_t slots() const;
};
}
