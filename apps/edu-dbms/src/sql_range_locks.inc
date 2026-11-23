// Included inside Engine::Impl. Generic acquire remains a coarse S/X lock.
using IntegerRange=LockManager::IntegerRange;
static bool range_expression_safe(const E& e){if(!e)return true;if(e->query||e->kind==Expr::Subquery||e->kind==Expr::Parameter)return false;for(const auto& a:e->args)if(!range_expression_safe(a))return false;return true;}
static bool range_shape(const Statement& st){
    if(st.kind!="SELECT"&&st.kind!="INSERT"&&st.kind!="UPDATE"&&st.kind!="DELETE")return false;
    if(!st.set_operation.empty()||st.sources.size()>1||st.read_tables.size()>1||st.on||!st.join_conditions.empty())return false;
    if(st.kind=="SELECT"&&(st.sources.size()!=1||st.sources[0].table.empty()))return false;
    if(!range_expression_safe(st.where)||!range_expression_safe(st.having))return false;
    for(const auto& i:st.items)if(!range_expression_safe(i.value))return false;
    for(const auto& e:st.group)if(!range_expression_safe(e))return false;
    for(const auto& e:st.order)if(!range_expression_safe(e.value))return false;
    for(const auto& a:st.assignments)if(!range_expression_safe(a.second))return false;
    for(const auto& row:st.values)for(const auto& e:row)if(!range_expression_safe(e))return false;
    return true;
}
static bool range_integer(const E& e,std::int64_t& value){
    if(!e)return false;
    if(e->kind==Expr::Literal&&e->value.is_number_integer()){
        if(e->value.is_number_unsigned()&&e->value.get<std::uint64_t>()>std::uint64_t(std::numeric_limits<std::int64_t>::max()))return false;
        value=e->value.get<std::int64_t>();return true;
    }
    if(e->kind==Expr::Unary&&(e->text=="+"||e->text=="-")&&e->args.size()==1&&range_integer(e->args[0],value)){
        if(e->text=="-"){if(value==std::numeric_limits<std::int64_t>::min())return false;value=-value;}return true;
    }
    return false;
}
static bool range_predicate(const E& e,const std::string& key,const std::string& table_name,const std::string& alias,IntegerRange& range){
    if(!e||e->kind!=Expr::Binary||e->args.size()!=2)return false;
    if(e->text=="AND")return range_predicate(e->args[0],key,table_name,alias,range)&&range_predicate(e->args[1],key,table_name,alias,range);
    auto column=[&](const E& a){return a&&a->kind==Expr::Column&&a->text==key&&(a->qualifier.empty()||a->qualifier==alias||a->qualifier==table_name);};
    std::int64_t number=0;auto operation=e->text;
    if(column(e->args[0])&&range_integer(e->args[1],number)){}
    else if(column(e->args[1])&&range_integer(e->args[0],number)){if(operation=="<")operation=">";else if(operation==">")operation="<";else if(operation=="<=")operation=">=";else if(operation==">=")operation="<=";}
    else return false;
    auto lower=[&](bool inclusive){if(!range.lower||number>*range.lower){range.lower=number;range.lower_inclusive=inclusive;}else if(number==*range.lower)range.lower_inclusive=range.lower_inclusive&&inclusive;};
    auto upper_bound=[&](bool inclusive){if(!range.upper||number<*range.upper){range.upper=number;range.upper_inclusive=inclusive;}else if(number==*range.upper)range.upper_inclusive=range.upper_inclusive&&inclusive;};
    if(operation=="="){lower(true);upper_bound(true);}else if(operation==">")lower(false);else if(operation==">=")lower(true);else if(operation=="<")upper_bound(false);else if(operation=="<=")upper_bound(true);else return false;
    return true;
}
bool try_record_locks(Session& s,const Statement& st){
    if(!s.active_options.record_locks)return false;
    auto fallback=[&](const std::string& reason){s.current_locking={{"requested","RECORD"},{"effective","TABLE"},{"reason",reason},{"physical_view",storage.logical_recovery_mode()?"current physical mini-transaction authority; coarse S/X guard required":"statement committed-page snapshot"}};return false;};
    if(!s.range_candidate||!range_shape(st))return fallback("JOIN, subquery, compound query, import or unsupported command");
    const auto name=st.kind=="SELECT"?st.sources[0].table:st.table;
    const auto alias=st.kind=="SELECT"?st.sources[0].alias:name;
    const bool writing=st.kind!="SELECT";
    // Resolve membership from the catalog before ANY table intention lock.
    // Generic acquire then keeps the immutable cluster -> table lock order.
    if(!storage.cluster_lock_resource(s.tx,name).empty())return fallback("shared multi-table cluster requires coarse cluster/table guards");
    const auto features=storage.schema_features(s.tx);
    if(features.value(name,json::object()).contains("materialized_view")||features.value(name,json::object()).contains("materialized_aux"))return fallback("materialized relations require coarse table guards");
    if(materialized_refresh_active())return fallback("materialized maintenance requires coarse source/state guards");
    if(writing)for(const auto& feature:features)if(materialized_metadata(feature)&&feature["materialized_view"].value("refresh",std::string("manual"))=="immediate")
        for(const auto& dependency:feature["materialized_view"]["dependencies"])if(dependency==name)return fallback("base write maintains an immediate materialized view; coarse source guards required");
    locks.register_resource("database-data");locks.register_resource("table:"+name,"database-data");
    locks.acquire_path(s.tx->id,"table:"+name,writing?LockMode::IX:LockMode::IS);
    storage.refresh_read_view(s.tx,s.current_accesses);
    auto metadata=storage.read_table(s.tx,name,false,false);
    if(metadata.contains("cluster"))return fallback("shared multi-table cluster requires coarse cluster/table guards");
    if(metadata.contains("materialized_view"))return fallback("materialized-view maintenance requires coarse table guards");
    std::string primary,key;std::size_t key_slot=0;
    for(const auto& index:metadata.at("indexes"))if(index.value("primary",false)||index.value("name",std::string())=="__pk_"+name){auto columns=index_columns(index);if(columns.size()!=1)return fallback("composite primary key is outside INTEGER single-key stage");primary=index.at("name").get<std::string>();key=columns[0].get<std::string>();break;}
    if(primary.empty())return fallback("no single INTEGER primary key");
    bool found=false;for(std::size_t i=0;i<metadata.at("columns").size();++i)if(metadata["columns"][i]["name"]==key){key_slot=i;found=true;break;}
    if(!found||metadata["columns"][key_slot]["type"]!="INTEGER")return fallback("primary key is not INTEGER");
    std::vector<IntegerRange> ranges;
    if(st.kind=="INSERT"){
        std::optional<std::size_t> input_slot;
        if(st.names.empty())input_slot=key_slot;else for(std::size_t i=0;i<st.names.size();++i)if(st.names[i]==key)input_slot=i;
        for(const auto& row:st.values){std::int64_t value=0;bool literal=false;if(input_slot&&*input_slot<row.size())literal=range_integer(row[*input_slot],value);else if((!input_slot||row.empty())&&metadata["columns"][key_slot]["default"].is_number_integer()){value=metadata["columns"][key_slot]["default"].get<std::int64_t>();literal=true;}if(!literal)return fallback("INSERT primary key requires explicit/default INTEGER literal");ranges.push_back({value,value,true,true});}
        if(ranges.empty())return fallback("INSERT contains no proved primary key point");
        std::sort(ranges.begin(),ranges.end(),[](const IntegerRange& a,const IntegerRange& b){return *a.lower<*b.lower;});
    }else{
        IntegerRange range;if(!range_predicate(st.where,key,name,alias,range))return fallback("WHERE requires only AND comparisons of the INTEGER primary key and INTEGER constants");ranges.push_back(range);
        if(st.kind=="UPDATE")for(const auto& assignment:st.assignments)if(assignment.first==key){std::int64_t value=0;if(!range_integer(assignment.second,value))return fallback("primary-key UPDATE requires a proved target INTEGER literal");ranges.push_back({value,value,true,true});}
    }
    const auto domain="range:"+std::to_string(metadata.at("table_id").get<std::uint64_t>())+":"+primary;
    locks.register_resource(domain,"table:"+name);json actual=json::array();
    for(const auto& range:ranges){locks.acquire_range(s.tx->id,domain,range,writing?LockMode::X:LockMode::S);actual.push_back(LockManager::range_json(range));}
    // A wait can outlive an index split and descriptor rewrite. Rebind every
    // clean metadata cache at the granted physical statement view, before a
    // query creates a cursor or a mutation captures its qualifying RID set.
    storage.refresh_read_view(s.tx,s.current_accesses);
    std::string held_mode;auto lock_snapshot=locks.snapshot();for(const auto& held:lock_snapshot["locks"])if(held["resource"]=="table:"+name&&held["tx_id"]==s.tx->id)held_mode=held["mode"].get<std::string>();
    s.current_locking={{"requested","RECORD"},{"effective","RECORD"},{"table",name},{"primary_index",primary},{"table_requested_mode",writing?"IX":"IS"},{"table_mode",held_mode},{"range_mode",writing?"X":"S"},{"resource",domain},{"intervals",actual},{"physical_view",storage.logical_recovery_mode()?"current authority plus key/range locks and a bounded statement copy under short structure mutex; no physical-CSN snapshot":"statement committed-page snapshot; refreshed after range grant"},{"predicate_protection","absent keys protected by the same overlapping INTEGER interval"},{"write_conflicts",storage.logical_recovery_mode()?"user key/range locks persist through logical commit/undo; physical mini owner and structure mutex end per row operation":"whole-page first-committer-wins; disjoint logical writes may conservatively abort"}};
    return true;
}
