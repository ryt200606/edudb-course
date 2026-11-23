// Limited exact logical-tree substitution, not arbitrary query containment.
// Discovery cache data never supplies a result or establishes freshness.
static bool& materialized_rewrite_active(){static thread_local bool active=false;return active;}
struct MaterializedRewriteScope{bool previous;MaterializedRewriteScope():previous(materialized_rewrite_active()){materialized_rewrite_active()=true;}~MaterializedRewriteScope(){materialized_rewrite_active()=previous;}};
static json materialized_expression_fingerprint(const E& expression,bool bound){
    if(!expression)return nullptr;
    json result={{"kind",int(expression->kind)}};
    if(expression->kind==Expr::Column){if(bound)result["slot"]=expression->bound;else{result["name"]=expression->text;result["qualifier"]=expression->qualifier;}}
    else{result["text"]=expression->text;if(expression->kind==Expr::Literal)result["value"]=expression->value.dump();}
    if(bound)result["type"]=expression->type;
    result["arguments"]=json::array();for(const auto& argument:expression->args)result["arguments"].push_back(materialized_expression_fingerprint(argument,bound));
    return result;
}
static json materialized_query_fingerprint(const Statement& query,bool bound){
    json result={{"distinct",query.distinct},{"items",json::array()},{"sources",json::array()},{"join_kinds",query.join_kinds},{"where",materialized_expression_fingerprint(query.where,bound)},{"on",materialized_expression_fingerprint(query.on,bound)},{"having",materialized_expression_fingerprint(query.having,bound)},{"group",json::array()}};
    for(const auto& source:query.sources)result["sources"].push_back(source.table);
    for(const auto& item:query.items)result["items"].push_back({{"expression",materialized_expression_fingerprint(item.value,bound)},{"alias",item.alias}});
    for(const auto& key:query.group){result["group"].push_back(materialized_expression_fingerprint(key,bound));}
    return result;
}
std::optional<json> materialized_rewrite(Session& session,Statement& statement,json& original,const json& output_schema,QueryBudget& budget,bool internal_output,double original_cost){
    if(shared_scan_state){original["materialized_view_rewrite"]={{"chosen",false},{"reason","explicit shared base scan batch preserves requested source"}};return std::nullopt;}
    if(internal_output||materialized_refresh_active()||materialized_rewrite_active()||session.active_options.protocol!=TransactionProtocol::Strict2PL)return std::nullopt;
    auto disabled=[&](const std::string& reason){original["materialized_view_rewrite"]={{"chosen",false},{"reason",reason}};};
    if(!statement.order.empty()||statement.offset||statement.limit!=std::numeric_limits<std::size_t>::max()||!statement.set_operation.empty()){disabled("This exact-tree rewrite stage excludes compound/order/offset/limit tails");return std::nullopt;}
    if(!statement.join_hint.empty()){disabled("Explicit USE JOIN controls the requested physical path");return std::nullopt;}
    for(const auto& source:statement.sources)if(source.force_scan||!source.index_hint.empty()){disabled("Explicit USE SCAN/USE INDEX controls the requested physical path");return std::nullopt;}
    json cached;{std::lock_guard<std::mutex> guard(cache_mutex);cached=table_cache;}
    if(std::none_of(cached.begin(),cached.end(),[](const json& entry){return entry.value("is_materialized_view",false);})){disabled("No committed materialized candidate in discovery cache");return std::nullopt;}
    const auto planning_before=storage.transaction_stats(session.tx);
    bool runtime=runtime_expression(statement.where)||runtime_expression(statement.on)||runtime_expression(statement.having);
    for(const auto& item:statement.items)runtime=runtime||runtime_expression(item.value);
    for(const auto& key:statement.group)runtime=runtime||runtime_expression(key);
    if(runtime){disabled("Subquery/runtime expressions retain the original SQL Apply path in this exact-tree rewrite stage");return std::nullopt;}
    std::size_t fingerprint_bytes=0,fingerprint_nodes=0;
    std::function<void(const E&)> estimate_fingerprint=[&](const E& e){if(!e||fingerprint_bytes>256u*1024u||fingerprint_nodes>10000)return;++fingerprint_nodes;fingerprint_bytes+=128+e->text.size()+e->qualifier.size()+json_allocation_estimate(e->value);for(const auto& arg:e->args)estimate_fingerprint(arg);};
    for(const auto& item:statement.items){estimate_fingerprint(item.value);}
    estimate_fingerprint(statement.where);estimate_fingerprint(statement.on);estimate_fingerprint(statement.having);for(const auto& key:statement.group){estimate_fingerprint(key);}
    if(fingerprint_bytes>256u*1024u||fingerprint_nodes>10000){disabled("Conservative fingerprint budget exceeded; original plan retained");return std::nullopt;}
    const auto syntax=materialized_query_fingerprint(statement,false),bound=materialized_query_fingerprint(statement,true);budget.materialize(syntax);budget.materialize(bound);
    json candidates=json::array();std::optional<Statement> chosen;std::string chosen_name;double best=original_cost;json chosen_versions;
    const auto search_started=Clock::now();std::size_t inspected=0;
    for(const auto& discovery:cached){
        if(inspected>=32||Clock::now()-search_started>std::chrono::milliseconds(500))break;
        if(!discovery.value("is_materialized_view",false)||!discovery.contains("materialized_view"))continue;
        const auto name=discovery.at("name").get<std::string>();if(std::any_of(statement.sources.begin(),statement.sources.end(),[&](const Source& source){return source.table==name;}))continue;
        // A bad/stale discovery entry can only lose an optimization opportunity.
        // Its definition is checked again from the locked persistent descriptor.
        Statement cached_definition;
        try{cached_definition=Parser(discovery["materialized_view"].at("sql").get<std::string>()).parse();}
        catch(const DbError&){continue;}
        if(!cached_definition.set_operation.empty()||!cached_definition.order.empty()||cached_definition.offset||cached_definition.limit!=std::numeric_limits<std::size_t>::max()||materialized_query_fingerprint(cached_definition,false)!=syntax)continue;
        ++inspected;
        if(!try_acquire_table(session,name)){candidates.push_back({{"view",name},{"eligible",false},{"reason","Candidate absent or table/cluster shared lock busy; original plan remains available"}});continue;}
        materialized_accesses(session,{name},"");auto metadata=table(session,name,false);
        if(!materialized_metadata(metadata)){candidates.push_back({{"view",name},{"eligible",false},{"reason","Discovery entry no longer names a materialized view"}});continue;}
        if(!materialized_fresh(session,metadata)){candidates.push_back({{"view",name},{"eligible",false},{"reason","Persistent versions or calculation semantics are stale/incompatible"}});continue;}
        auto definition=Parser(metadata["materialized_view"].at("sql").get<std::string>()).parse();definition.explain=true;
        auto binding=select(session,definition,&budget,true);
        if(materialized_query_fingerprint(definition,true)!=bound||binding["output_schema"]!=output_schema){candidates.push_back({{"view",name},{"eligible",false},{"reason","Locked bound logical tree or result schema differs"}});continue;}
        Statement replacement;replacement.kind="SELECT";replacement.sources.push_back({name,name,"",false});replacement.items.push_back({expr(Expr::Star,"*"),""});replacement.explain=true;
        const auto view_binding=select(session,replacement,&budget,true);
        if(view_binding["output_schema"]!=output_schema){candidates.push_back({{"view",name},{"eligible",false},{"reason","Stored materialized column types/names differ"}});continue;}
        const auto& source=view_binding["plan"]["sources"][0];const auto cost=source.value("estimated_cost",std::numeric_limits<double>::infinity());
        candidates.push_back({{"view",name},{"eligible",true},{"estimated_cost",cost},{"original_estimated_cost",original_cost},{"measurement","normalized page/tuple estimate; not elapsed time"}});
        if(cost<best){best=cost;chosen=std::move(replacement);chosen_name=name;chosen_versions=metadata["materialized_refresh_state"]["base_versions"];}
    }
    original["materialized_view_rewrite"]={{"chosen",bool(chosen)},{"candidates",candidates},{"rule","exact bound logical-tree and output-schema equality with fresh persistent version stamps"},{"original_estimated_cost",original_cost}};
    const auto planning_after=storage.transaction_stats(session.tx);
    for(const char* key:{"heap_page_reads","index_page_reads"})original["planning_metadata_io"][key]=original.value("planning_metadata_io",json::object()).value(key,std::uint64_t(0))+planning_after.value(key,std::uint64_t(0))-planning_before.value(key,std::uint64_t(0));
    if(!chosen)return std::nullopt;
    chosen->explain=statement.explain;chosen->analyze=statement.analyze;MaterializedRewriteScope active;
    auto result=select(session,*chosen,&budget,internal_output);
    for(const char* key:{"heap_page_reads","index_page_reads"})result["metrics"]["planning_metadata_io"][key]=result["metrics"].value("planning_metadata_io",json::object()).value(key,std::uint64_t(0))+original.value("planning_metadata_io",json::object()).value(key,std::uint64_t(0));
    const auto actual=result["plan"];
    result["plan"]={{"node","MaterializedViewScan"},{"view",chosen_name},{"estimated_cost",best},{"original_estimated_cost",original_cost},{"persistent_freshness_checked_under_locks",true},{"base_versions",chosen_versions},{"rewrite_rule","exact bound logical-tree substitution; explicit access hints disable substitution"},{"original_unopened_plan",original},{"chosen_scan",actual},{"planning_metadata_io",result["metrics"]["planning_metadata_io"]}};
    if(statement.explain){result["rows"]=json::array({json::array({result["plan"].dump(2)})});}
    return result;
}
