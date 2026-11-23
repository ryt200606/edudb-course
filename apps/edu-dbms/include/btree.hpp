#pragma once
#include "edudb.hpp"
#include <utility>
namespace edudb {
// Composite (integer key, row ordinal) entries bound duplicate-key leaf payloads.
class BPlusTree {
public:
    explicit BPlusTree(std::size_t max_keys = 31);
    void insert(std::int64_t key, std::uint64_t rowid);
    bool erase(std::int64_t key, std::uint64_t rowid);
    std::vector<std::uint64_t> search(const std::string& op, std::int64_t key) const;
    std::vector<std::pair<std::int64_t,std::vector<std::uint64_t>>> entries() const;
    bool validate() const;
    json stats() const;
    json serialize() const;
    static BPlusTree deserialize(const json& source);
private:
    using Key = std::pair<std::int64_t,std::uint64_t>;
    struct Node { bool leaf=true,alive=true; std::vector<Key> keys; std::vector<int> children; int next=-1; };
    std::vector<Node> nodes_;
    std::size_t maximum_;
    int root_=0;
    int add(bool leaf);
    Key first(int id) const;
    void refresh(int id);
    int insert_node(int id,Key key);
    bool erase_node(int id,Key key);
    void repair(int parent,std::size_t index);
};
}
