#pragma once
#include "edudb.hpp"
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace edudb::access {
using Key = json;
struct Entry {
    Key key = json::array();
    std::uint64_t id = 0;
    json payload = nullptr;
};
struct Pager {
    std::function<json(std::uint64_t)> read;
    std::function<void(std::uint64_t, const json&)> write;
    std::function<std::uint64_t(const json&)> allocate;
    std::function<void(std::uint64_t)> retire;
    std::size_t page_size_budget = 7800;
};
int compare_keys(const Key& a, const Key& b);
std::uint64_t hash_key(const Key& key);
std::uint64_t hash_key(const Key& key, const std::string& variant);
json capabilities(const std::string& method);

// Every page mutation goes through Pager, so the caller supplies locking,
// private page overlays, WAL, and atomic publication. The descriptor belongs
// to that same transaction; do not publish it separately from its pages.
class Index {
public:
    struct Impl; // Opaque; public only for translation-unit page helpers.
    Index(Pager pager, json& descriptor);
    ~Index();
    Index(Index&&) noexcept;
    Index& operator=(Index&&) noexcept;
    Index(const Index&) = delete;
    Index& operator=(const Index&) = delete;
    void initialize();
    void insert(const Entry& entry);
    bool erase(const Key& key, std::uint64_t id);
    std::vector<Entry> find(const Key& key);
    std::vector<Entry> range(std::optional<Key> lower = {}, std::optional<Key> upper = {},
                             bool lower_inclusive = true, bool upper_inclusive = true,
                             std::size_t limit = std::numeric_limits<std::size_t>::max());
    std::vector<Entry> all();
    void bulk_load(std::vector<Entry> entries);
    void bulk_load_sorted(std::function<bool(Entry&)> next);
    void clear();
    std::optional<std::uint64_t> locate(const Key& key, std::uint64_t id);
    std::optional<Entry> read_at(std::uint64_t page, const Key& key, std::uint64_t id);
    std::vector<Entry> page_heads();
    std::function<bool(Entry&)> cursor(std::optional<Key> lower = {}, std::optional<Key> upper = {},
                                      bool lower_inclusive = true, bool upper_inclusive = true);
    json inspect(std::size_t max_pages = 2048);
    json verify();
private:
    std::unique_ptr<Impl> impl_;
};
} // namespace edudb::access
