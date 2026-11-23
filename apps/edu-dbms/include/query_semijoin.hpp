#pragma once
#include <query_operators.hpp>
namespace edudb::query {
struct SemiJoinSpec {
    Key outer_key,inner_key;
    std::function<bool(const json&)> outer_filter;
    // Optional native-unit fault hook. SQL never supplies it or a file path.
    std::function<void()> after_build;
};
// Equality semi/anti join, not NULL-aware SQL NOT IN. NULL outer keys do not
// match, so they are discarded by SEMI and preserved by NOT EXISTS ANTI.
// Inner keys are read once. Bounded hash state spills through QuerySpillStore.
std::unique_ptr<ExecNode> semi_join(std::unique_ptr<ExecNode> outer,
    std::unique_ptr<ExecNode> inner,SemiJoinSpec spec,bool anti=false);
} // namespace edudb::query
