struct FinalSortChoice {bool bounded=false;std::size_t k=0;json description;};
static FinalSortChoice final_sort_choice(const Statement& statement){
    FinalSortChoice choice;
    const char* configured=std::getenv("EDUDB_OPTIMIZER_TOPK");const auto setting=upper(configured?configured:"ON");
    if(setting!="ON"&&setting!="OFF"&&setting!="1"&&setting!="0")fail("INVALID_CONFIGURATION","EDUDB_OPTIMIZER_TOPK must be ON or OFF");
    std::string reason;
    if(setting=="OFF"||setting=="0")reason="Bounded Top-K disabled explicitly";
    else if(statement.limit==std::numeric_limits<std::size_t>::max())reason="No finite LIMIT";
    else if(statement.offset>std::numeric_limits<std::size_t>::max()-statement.limit)reason="OFFSET plus LIMIT cannot be represented; full external sort preserves semantics";
    else {
        choice.k=statement.offset+statement.limit;
        if(choice.k>query::topk_maximum_candidates(operator_memory_limit()))reason="K exceeds optimistic bookkeeping capacity; choose external sort directly";
        else choice.bounded=true;
    }
    choice.description={{"node",choice.bounded?"BoundedTopK":"ExternalSort"},{"memory_limit",operator_memory_limit()},{"ordering_scope","all SQL directions and NULL placement; stable input order for equal keys"},{"source_early_stop",false}};
    if(choice.bounded){choice.description["k"]=choice.k;choice.description["fallback"]="Actual heap width or large K switches to CRC external sort";}
    else choice.description["reason"]=reason;
    return choice;
}
