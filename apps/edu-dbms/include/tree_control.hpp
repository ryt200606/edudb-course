#pragma once
#include "json.hpp"
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
namespace edudb {
class LockTreeTopology {
public:
    std::string table,key,parent_column;
    std::map<std::int64_t,std::optional<std::int64_t>> parents;
    LockTreeTopology(std::string name,std::string primary,std::string parent,std::map<std::int64_t,std::optional<std::int64_t>> edges);
    std::string resource(std::int64_t node) const;
};
struct LockTreeState {
    std::shared_ptr<const LockTreeTopology> topology;
    std::set<std::int64_t> held,released,written;
    void before_lock(std::int64_t node) const;
    void granted(std::int64_t node);
    void before_unlock(std::int64_t node) const;
    void unlocked(std::int64_t node);
    void access(std::int64_t node,bool write);
    void clear();
    nlohmann::json diagnostics() const;
};
}
