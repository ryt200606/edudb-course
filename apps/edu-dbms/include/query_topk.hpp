#pragma once
#include <query_operators.hpp>

namespace edudb::query {
// Optimistic bookkeeping capacity only. Actual row widths can require an
// earlier external fallback. SQL planning uses this to avoid a pointless heap.
std::size_t topk_maximum_candidates(std::size_t memory_limit);
// Returns only the first k rows under a stable total order. The node still
// consumes its input; LIMIT is not a claim that an unordered source stops early.
// An actual-width memory overflow switches to the existing CRC external sort.
std::unique_ptr<ExecNode> topk(std::unique_ptr<ExecNode> child,Key key,
                             Compare compare,std::size_t k);
}
