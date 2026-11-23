#pragma once
#include <query_operators.hpp>
namespace edudb::query {
struct AliasCopySpec {
    std::size_t from=0,to=0,width=0,total_width=0;
    std::vector<int> required_slots;
};
// Physical alias reconstruction after a separately proved same-row self join.
// No SQL predicate, uniqueness or table identity is inferred by this node.
std::unique_ptr<ExecNode> alias_copy(std::unique_ptr<ExecNode> source,AliasCopySpec spec);
}
