#pragma once
#include "edudb.hpp"
#include "query_spill.hpp"
#include <functional>

namespace edudb::query {
struct ExecContext {
    QuerySpillStore* spill=nullptr;
    std::size_t memory_limit=2u*1024u*1024u;
    std::function<void()> work;
    std::function<void(const json&)> materialize;
    std::function<std::size_t(const json&)> row_bytes;
};
class ExecNode {
public:
    virtual ~ExecNode()=default;
    virtual void open(ExecContext&)=0;
    virtual bool next(json&)=0;
    virtual void close() noexcept=0;
    virtual json diagnostics() const=0;
};
using Pull=std::function<bool(json&)>;
using Open=std::function<Pull()>;
using Key=std::function<json(const json&)>;
using Compare=std::function<int(const json&,const json&)>;
struct JoinSpec {
    Key left_key,right_key;
    Compare compare;
    std::function<bool(const json&)> predicate;
    std::vector<int> right_slots;
    std::function<std::unique_ptr<ExecNode>(const json&)> index_probe;
};
std::unique_ptr<ExecNode> scan(Open open,const std::string& name);
std::unique_ptr<ExecNode> sort(std::unique_ptr<ExecNode> child,Key key,Compare compare);
std::unique_ptr<ExecNode> join(const std::string& algorithm,std::unique_ptr<ExecNode> left,
                             std::unique_ptr<ExecNode> right,JoinSpec spec);
// Canonical SQL numeric equality (including INTEGER/REAL and signed zero),
// used only for transient hash joins, never persisted index routing.
std::string equality_key(json key);
} // namespace edudb::query
