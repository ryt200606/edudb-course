// Persisted materialized relations. FULL and changefeed-backed SP/GROUP delta
// maintenance are distinct actual paths and are reported separately.
static bool materialized_metadata(const json& metadata){return metadata.contains("materialized_view")&&metadata["materialized_view"].is_object();}
json materialized_metadata_table(Session& session,const std::string& name){return storage.read_table(session.tx,name,false,false);}
static bool& materialized_refresh_active(){static thread_local bool active=false;return active;}
struct MaterializedRefreshScope{
    bool previous;
    MaterializedRefreshScope():previous(materialized_refresh_active()){materialized_refresh_active()=true;}
    ~MaterializedRefreshScope(){materialized_refresh_active()=previous;}
};
void materialized_accesses(Session& session,const std::vector<std::string>& dependencies,const std::string& target){
    std::map<std::string,TransactionAccess> accesses;
    for(const auto& access:session.current_accesses)accesses[access.table]=access;
    auto add=[&](const std::string& name,bool write){auto& access=accesses[name];access.table=name;access.read=true;access.write=access.write||write;access.blind_write=false;};
    add("__catalog__",false);for(const auto& name:dependencies)add(name,false);if(!target.empty())add(target,true);
    session.current_accesses.clear();for(const auto& item:accesses)session.current_accesses.push_back(item.second);
    storage.refresh_read_view(session.tx,session.current_accesses);
    std::set<std::string> names(dependencies.begin(),dependencies.end());if(!target.empty())names.insert(target);
    for(const auto& name:names)acquire(session,"table:"+name,name==target);
}
json materialized_versions(Session& session,const std::vector<std::string>& dependencies){
    json versions=json::object();
    for(const auto& dependency:dependencies){const auto source=materialized_metadata_table(session,dependency);versions[dependency]={{"data_version",source.value("data_version",std::uint64_t(0))},{"schema_version",source.value("schema_version",std::uint64_t(0))}};}
    return versions;
}
bool materialized_fresh(Session& session,const json& metadata){
    if(!materialized_metadata(metadata)||metadata.value("materialized_refresh_state",json::object()).value("calculation_semantics",std::string())!="exact-binary-v1")return false;
    const auto& definition=metadata["materialized_view"];
    std::vector<std::string> dependencies=definition["dependencies"].get<std::vector<std::string>>();
    for(const auto& name:dependencies)if(!exists(session,name))return false;
    return materialized_versions(session,dependencies)==metadata.value("materialized_refresh_state",json::object()).value("base_versions",json::object());
}
void materialized_protocol_guard(const Session& session){if(session.active_options.protocol!=TransactionProtocol::Strict2PL)fail("UNSUPPORTED_MATERIALIZED_PROTOCOL","Materialized-view maintenance currently requires STRICT_2PL; snapshot/OCC/timestamp writes are not silently mixed");}
void materialized_prepare(Session& session,const std::vector<std::string>& changed){
    if(changed.empty())return;
    const std::set<std::string> names(changed.begin(),changed.end());const auto catalog=storage.schema_features(session.tx);
    for(auto item=catalog.begin();item!=catalog.end();++item){
        if(!materialized_metadata(item.value())||item.value()["materialized_view"].value("refresh",std::string("manual"))!="immediate")continue;
        std::vector<std::string> affected;for(const auto& dependency:item.value()["materialized_view"]["dependencies"])if(names.count(dependency.get<std::string>()))affected.push_back(dependency.get<std::string>());
        if(affected.empty())continue;
        materialized_protocol_guard(session);acquire(session,"table:"+item.key(),true);auto metadata=materialized_metadata_table(session,item.key());
        if(metadata["materialized_view"].value("delta",json::object()).value("eligible",false))storage.enable_changefeed(session.tx,affected);
    }
}
void materialized_write_guard(Session& session,const std::string& target){
    if(materialized_refresh_active()||!exists(session,target))return;
    const auto features=storage.schema_features(session.tx);const auto metadata=features.value(target,json::object());
    if(metadata.contains("materialized_aux"))fail("READ_ONLY_MATERIALIZED_VIEW","Direct writes to persistent aggregate state are not allowed");
    if(materialized_metadata(metadata))fail("READ_ONLY_MATERIALIZED_VIEW","Use REFRESH MATERIALIZED VIEW; direct DML/CSV writes are not allowed: "+target);
    materialized_prepare(session,{target});
}
void materialized_schema_guard(Session& session,const Statement& statement){
    if(statement.kind!="DROP_TABLE"&&statement.kind!="ALTER_TABLE"&&statement.kind!="REORGANIZE")return;
    if(!exists(session,statement.table))return;
    if(materialized_metadata_table(session,statement.table).contains("materialized_aux"))fail("READ_ONLY_MATERIALIZED_VIEW","Aggregate auxiliary state belongs to its materialized view");
    if(materialized_metadata(materialized_metadata_table(session,statement.table)))fail("READ_ONLY_MATERIALIZED_VIEW","Use DROP MATERIALIZED VIEW rather than table schema changes");
    const auto catalog=storage.catalog(session.tx);
    for(auto item=catalog.begin();item!=catalog.end();++item){auto metadata=materialized_metadata_table(session,item.key());if(!materialized_metadata(metadata))continue;
        for(const auto& dependency:metadata["materialized_view"]["dependencies"])if(dependency==statement.table)fail("DEPENDENT_OBJECT","Materialized view "+item.key()+" depends on "+statement.table+"; drop the view first");
    }
}
json materialized_refresh(Session& session,const std::string& name,json metadata,QueryBudget* shared_budget=nullptr){
    materialized_protocol_guard(session);
    std::unique_ptr<QueryBudget> owned_budget;if(!shared_budget)owned_budget=std::make_unique<QueryBudget>(query_limits);
    QueryBudget& budget=shared_budget?*shared_budget:*owned_budget;budget.cancellation=locks.cancellation_token(session.tx->id);
    QueryBudgetScope budget_scope(budget);PageProgressScope page_scope([&]{budget.work();budget.check_time();});
    const auto definition=metadata["materialized_view"];
    const auto dependencies=definition["dependencies"].get<std::vector<std::string>>();
    materialized_accesses(session,dependencies,name);
    auto query=Parser(definition["sql"].get<std::string>()).parse();if(query.kind!="SELECT"||query.explain)fail("CORRUPT_DATABASE","Materialized definition must be a plain SELECT");
    MaterializedRefreshScope maintenance;
    auto selected=select(session,query,&budget,true);
    if(selected["columns"].size()!=metadata["columns"].size())fail("MATERIALIZED_SCHEMA_CHANGED","Refresh result width differs from stored definition");
    for(std::size_t i=0;i<selected["columns"].size();++i){const auto actual=selected["columns"][i]["type"].get<std::string>(),expected=metadata["columns"][i]["type"].get<std::string>();if(actual!="NULL"&&actual!=expected)fail("MATERIALIZED_SCHEMA_CHANGED","Refresh result type differs from stored schema");}
    metadata["rows"]=json::array();for(const auto& row:selected["rows"])metadata["rows"].push_back(validate_row(row,metadata["columns"]));
    auto delta=materialized_delta_plan(session,name,query);const auto policy=definition.value("maintenance_policy",std::string("auto"));
    if(policy=="incremental"&&!delta.eligible)fail("UNSUPPORTED_INCREMENTAL_VIEW",delta.reason);
    const bool eligible=delta.eligible&&policy!="full";
    metadata["materialized_view"]["delta"]={{"eligible",eligible},{"grouped",delta.grouped},{"aux",eligible?delta.aux:""},{"reason",policy=="full"?"Explicit full-maintenance policy":delta.reason}};
    const auto high_water=storage.read_changefeed(session.tx,"",std::numeric_limits<std::uint64_t>::max())["last_seq"];
    metadata["materialized_refresh_state"]={{"base_versions",materialized_versions(session,dependencies)},{"maintenance","FULL"},{"refresh_count",metadata.value("materialized_refresh_state",json::object()).value("refresh_count",std::uint64_t(0))+1},{"rows",metadata["rows"].size()},{"transaction_id",session.tx->id},{"calculation_semantics","exact-binary-v1"},{"feed_tx_id",session.tx->id},{"last_seq",high_water}};
    if(eligible&&delta.grouped)materialized_delta_bootstrap(session,name,metadata,delta,budget);else storage.write_table(session.tx,name,metadata);
    return {{"view",name},{"maintenance","FULL"},{"rows",selected["rows"].size()},{"delta_ready",metadata["materialized_view"]["delta"]},{"same_transaction",true},{"source_plan",selected["plan"]},{"query_budget",budget.snapshot()}};
}
json materialized_maintain(Session& session,const std::vector<std::string>& changed){
    json receipts=json::array();if(materialized_refresh_active()||changed.empty())return receipts;
    const std::set<std::string> changed_set(changed.begin(),changed.end());std::vector<std::pair<std::string,json>> affected_views;
    const auto catalog=storage.schema_features(session.tx);std::uint64_t metadata_bytes=0;
    for(auto item=catalog.begin();item!=catalog.end();++item){if(!materialized_metadata(item.value()))continue;
        const auto& definition=item.value()["materialized_view"];if(definition.value("refresh",std::string("manual"))!="immediate")continue;
        bool affected=false;for(const auto& dependency:definition["dependencies"])affected=affected||changed_set.count(dependency.get<std::string>())!=0;
        if(!affected)continue;
        acquire(session,"table:"+item.key(),true);auto metadata=materialized_metadata_table(session,item.key());
        const auto bytes=json_allocation_estimate(metadata);if(affected_views.size()>=128||bytes>query_limits.materialized_bytes-metadata_bytes)fail("MATERIALIZED_RESOURCE","Affected-view metadata exceeds128 views or configured estimated-byte budget");metadata_bytes+=bytes;affected_views.push_back({item.key(),std::move(metadata)});
    }
    // Ordinary DML without a dependent immediate MV keeps its existing DML
    // contract. A SELECT-only timeout must not start during empty discovery.
    if(affected_views.empty())return receipts;
    QueryBudget owned(query_limits);QueryBudget& budget=owned;budget.cancellation=locks.cancellation_token(session.tx->id);QueryBudgetScope budget_scope(budget);PageProgressScope page_scope(query_page_progress(budget));
    for(auto& item:affected_views){auto& metadata=item.second;const auto& definition=metadata["materialized_view"];budget.materialize(metadata);
        if(definition.value("delta",json::object()).value("eligible",false)&&metadata.value("materialized_refresh_state",json::object()).value("calculation_semantics",std::string())=="exact-binary-v1"){
            auto query=Parser(definition["sql"].get<std::string>()).parse();query.explain=true;auto binding=select(session,query,&budget,true);(void)binding;query.explain=false;
            auto plan=materialized_delta_plan(session,item.first,std::move(query));const auto state=metadata.value("materialized_refresh_state",json::object());const auto after=state.value("feed_tx_id",std::uint64_t(0))==session.tx->id?state.value("last_seq",std::uint64_t(0)):0;
            auto feed=storage.read_changefeed(session.tx,plan.source,after);bool reset=false;for(const auto& event:feed["events"])reset=reset||event["kind"]=="RESET";
            if(plan.eligible&&!reset){receipts.push_back(materialized_incremental(session,item.first,std::move(metadata),plan,feed,&budget));continue;}
        }
        auto refreshed=materialized_refresh(session,item.first,std::move(metadata),&budget);refreshed["fallback_reason"]="RESET, unsupported definition or historical calculation semantics require complete refresh";receipts.push_back(std::move(refreshed));
    }
    return receipts;
}
json materialized_command(Session& session,Statement& statement){
    auto result=blank_result();
    if(statement.kind=="SHOW_MATERIALIZED_VIEWS"){
        result["columns"]={{{"name","view_name"},{"type","VARCHAR"}},{{"name","refresh"},{"type","VARCHAR"}},{{"name","fresh"},{"type","BOOLEAN"}},{{"name","rows"},{"type","INTEGER"}},{{"name","dependencies"},{"type","VARCHAR"}}};
        const auto catalog=storage.schema_features(session.tx);for(auto item=catalog.begin();item!=catalog.end();++item){if(!materialized_metadata(item.value()))continue;auto dependencies=item.value()["materialized_view"]["dependencies"].get<std::vector<std::string>>();materialized_accesses(session,dependencies,"");acquire(session,"table:"+item.key(),false);auto metadata=materialized_metadata_table(session,item.key());result["rows"].push_back(json::array({item.key(),metadata["materialized_view"]["refresh"],materialized_fresh(session,metadata),metadata["row_count"],metadata["materialized_view"]["dependencies"].dump()}));}
        result["plan"]={{"node","MaterializedViewCatalog"},{"freshness","transaction-visible data/schema version equality"}};return result;
    }
    if(statement.kind=="DROP_MATERIALIZED_VIEW"){
        materialized_protocol_guard(session);
        if(!exists(session,statement.table)){if(statement.if_exists)return result;fail("TABLE_NOT_FOUND","Materialized view not found: "+statement.table);}
        if(!materialized_metadata(materialized_metadata_table(session,statement.table))){fail("NOT_MATERIALIZED_VIEW","Target is a base table");}
        auto owned=materialized_metadata_table(session,statement.table)["materialized_view"].value("delta",json::object()).value("aux",std::string());
        if(!owned.empty()&&exists(session,owned)){materialized_aux_check(session,owned,statement.table);materialized_accesses(session,{statement.table},owned);storage.drop_table(session.tx,owned);}
        acquire(session,"table:"+statement.table,true);storage.drop_table(session.tx,statement.table);result["plan"]={{"node","DropMaterializedView"}};return result;
    }
    if(statement.kind=="REFRESH_MATERIALIZED_VIEW"){
        materialized_protocol_guard(session);
        if(!exists(session,statement.table))fail("TABLE_NOT_FOUND","Materialized view not found: "+statement.table);
        const auto catalog=storage.schema_features(session.tx);auto feature=catalog.value(statement.table,json::object());if(!materialized_metadata(feature))fail("NOT_MATERIALIZED_VIEW","Target is a base table");
        materialized_accesses(session,feature["materialized_view"]["dependencies"].get<std::vector<std::string>>(),statement.table);
        auto metadata=materialized_metadata_table(session,statement.table);if(!materialized_metadata(metadata))fail("NOT_MATERIALIZED_VIEW","Target is a base table");result["refresh"]=materialized_refresh(session,statement.table,std::move(metadata));result["plan"]={{"node","RefreshMaterializedView"},{"maintenance","FULL"},{"same_transaction",true}};return result;
    }
    materialized_protocol_guard(session);
    if(statement.kind!="CREATE_MATERIALIZED_VIEW")fail("UNSUPPORTED_SQL","Unknown materialized-view command");
    if(exists(session,statement.table))fail("TABLE_EXISTS","Name is already used: "+statement.table);
    if(statement.table.rfind("__",0)==0)fail("INVALID_IDENTIFIER","Names beginning __ are reserved");
    for(auto option=statement.options.begin();option!=statement.options.end();++option)if(option.key()!="refresh"&&option.key()!="maintenance")fail("INVALID_OPTION","Materialized WITH supports refresh and maintenance");
    auto policy=statement.options.value("refresh",json("manual"));if(!policy.is_string()||(policy!="manual"&&policy!="immediate"))fail("INVALID_OPTION","Materialized refresh must be 'manual' or 'immediate'");
    auto requested=statement.options.value("maintenance",json("auto"));if(!requested.is_string()||(requested!="auto"&&requested!="full"&&requested!="incremental"))fail("INVALID_OPTION","Materialized maintenance must be 'auto', 'full' or 'incremental'");
    if(!statement.query_definition||statement.query_definition->kind!="SELECT"||statement.query_definition->explain)fail("INVALID_MATERIALIZED_DEFINITION","Materialized definition must be SELECT");
    if(statement.read_tables.size()>32)fail("QUERY_TOO_COMPLEX","Materialized view supports at most32 source tables");
    materialized_accesses(session,statement.read_tables,"");
    for(const auto& dependency:statement.read_tables){if(dependency==statement.table)fail("DEPENDENT_OBJECT","A materialized view cannot depend on itself");if(materialized_metadata(materialized_metadata_table(session,dependency)))fail("UNSUPPORTED_SQL","Nested materialized-view dependencies require a later topological-maintenance stage");}
    Statement binding=*statement.query_definition;binding.explain=true;binding.analyze=false;QueryBudget budget(query_limits);auto explained=select(session,binding,&budget,true);
    json columns=json::array();std::set<std::string> labels;
    for(const auto& output:explained["output_schema"]){auto label=output["name"].get<std::string>();if(label.empty()||label.size()>128||!labels.insert(label).second)fail("DUPLICATE_COLUMN","Materialized outputs require unique explicit aliases where necessary");auto type=output["type"].get<std::string>();if(type=="NULL")type="VARCHAR";columns.push_back({{"name",label},{"type",type},{"length",type=="VARCHAR"?4096:type=="BLOB"?16777216:0},{"not_null",false},{"primary",false},{"unique",false},{"default",nullptr}});}
    json metadata={{"columns",columns},{"rows",json::array()},{"indexes",json::array()},{"organization",{{"method","HEAP"},{"columns",json::array()},{"options",json::object()}}},{"materialized_view",{{"sql",statement.definition_sql},{"dependencies",statement.read_tables},{"refresh",policy},{"maintenance_policy",requested},{"maintenance","FULL"},{"definition_format",1}}}};
    validate_table(metadata);result["refresh"]=materialized_refresh(session,statement.table,std::move(metadata));result["plan"]={{"node","CreateMaterializedView"},{"maintenance","FULL"},{"refresh_policy",policy},{"same_transaction",true}};return result;
}

std::string materialized_dump(Session& session,const std::string& name,const json& metadata){
    if(!materialized_fresh(session,metadata))fail("STALE_MATERIALIZED_EXPORT","Refresh manual materialized views before logical SQL export; stale historical materializations require a physical backup");
    const auto& definition=metadata["materialized_view"];
    std::string sql="CREATE MATERIALIZED VIEW "+interchange::quote_identifier(name)+" WITH (refresh="+interchange::sql_literal(definition["refresh"])+", maintenance="+interchange::sql_literal(definition.value("maintenance_policy",json("auto")))+") AS "+definition["sql"].get<std::string>()+";\n";
    auto copy=metadata;copy["rows"]=json::array();for(const auto& text:interchange::split_sql_script(interchange::dump_table(name,copy))){auto parsed=Parser(text).parse();if(parsed.kind=="CREATE_INDEX")sql+=text+";\n";}
    return sql;
}
