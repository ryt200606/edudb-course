#pragma once
#include "query_operators.hpp"

namespace edudb::query {
// Inputs are already projected/coerced to compatible result schemas. The
// comparator implements SQL scalar equality, including NULL= NULL for sets.
std::unique_ptr<ExecNode> set_operation(const std::string& operation,bool all,
    std::unique_ptr<ExecNode> left,std::unique_ptr<ExecNode> right,Compare rows);
std::unique_ptr<ExecNode> distinct(std::unique_ptr<ExecNode> child,Compare rows);
// This is a real bounded block outer join with a rewindable CRC run, not a
// relabelled inner join. Slots retain their stable global binding identities.
std::unique_ptr<ExecNode> outer_join(const std::string& kind,
    std::unique_ptr<ExecNode> left,std::unique_ptr<ExecNode> right,
    JoinSpec spec,std::vector<int> left_slots);
struct ApplySpec {
    std::string mode; // EXISTS, NOT_EXISTS, IN, NOT_IN, SCALAR
    std::function<std::unique_ptr<ExecNode>(const json&)> inner;
    std::function<json(const json&)> value;
    Compare scalar_compare;
    int output_slot=-1;
};
// The inner factory builds in the same transaction/context, never calls the
// public Engine::execute recursively, and may bind the supplied outer row.
std::unique_ptr<ExecNode> apply(std::unique_ptr<ExecNode> outer,ApplySpec spec);
} // namespace edudb::query
