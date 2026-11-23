// Inside Engine::Impl. Delta maintenance is deliberately restricted to bound,
// single-base-table selection/projection and COUNT/SUM/AVG grouping. Complex
// definitions and RESET streams visibly select complete refresh instead.

static std::string materialized_aux_name(const std::string& name){
    std::uint64_t hash=1469598103934665603ull;for(unsigned char c:name){hash^=c;hash*=1099511628211ull;}std::ostringstream out;out<<"__mv_state_"<<std::hex<<hash;return out.str();
}
static json materialized_blob(const std::string& text){
    if(text.size()>16u*1024u*1024u)fail("MATERIALIZED_RESOURCE","Materialized state/key exceeds the16MiB object limit");
    static const char* digits="0123456789ABCDEF";std::string hex;hex.reserve(text.size()*2);for(unsigned char c:text){hex+=digits[c>>4];hex+=digits[c&15];}return {{"$blob",hex}};
}
static json materialized_unblob(const json& value){
    if(!blob_value(value)){fail("MATERIALIZED_STATE","Auxiliary state is not a materialized BLOB");}
    const auto& hex=value["$blob"].get_ref<const std::string&>();if(hex.size()%2||hex.size()>32u*1024u*1024u)fail("MATERIALIZED_STATE","Invalid auxiliary BLOB width");
    std::string text;text.reserve(hex.size()/2);auto digit=[](char c){return c>='0'&&c<='9'?c-'0':c>='A'&&c<='F'?c-'A'+10:-1;};for(std::size_t n=0;n<hex.size();n+=2){const auto a=digit(hex[n]),b=digit(hex[n+1]);if(a<0||b<0)fail("MATERIALIZED_STATE","Invalid auxiliary BLOB hex");text+=char((a<<4)|b);}
    try{return json::parse(text);}catch(...){fail("MATERIALIZED_STATE","Invalid auxiliary state JSON");}
}
MaterializedDeltaPlan materialized_delta_plan(Session& session,const std::string& name,Statement query){
    MaterializedDeltaPlan plan;plan.query=std::move(query);auto& q=plan.query;
    if(!q.set_operation.empty()||q.sources.size()!=1||q.on||q.distinct||!q.order.empty()||q.offset||q.limit!=std::numeric_limits<std::size_t>::max()){plan.reason="Compound/join/distinct/order/limit definition uses complete refresh";return plan;}
    plan.source=q.sources[0].table;const auto metadata=materialized_metadata_table(session,plan.source);for(const auto& column:metadata["columns"])if(column["type"]=="BLOB"){plan.reason="Encoded BLOB source uses complete refresh";return plan;}
    plan.schema=schema_for(metadata,q.sources[0].alias);
    std::function<bool(const E&)> inspect=[&](const E& e){if(!e)return true;if(e->kind==Expr::Subquery||e->kind==Expr::Parameter||e->type=="BLOB")return false;
        if(e->kind==Expr::Function&&aggregate_function(e->text)){
            if(e->text!="COUNT"&&e->text!="SUM"&&e->text!="AVG")return false;
            bool found=false;for(const auto& existing:plan.aggregates)found=found||same_expression(existing,e);
            if(!found){plan.aggregates.push_back(e);auto type=e->args[0]->type;if(type=="NULL"||e->args[0]->kind==Expr::Star)type="INTEGER";plan.specs.push_back({e->text,type});}
        }
        for(const auto& argument:e->args){if(!inspect(argument))return false;}
        return true;
    };
    bool allowed=inspect(q.where)&&inspect(q.having);for(const auto& item:q.items)allowed=allowed&&inspect(item.value);for(const auto& key:q.group)allowed=allowed&&inspect(key);
    if(!allowed){plan.reason="MIN/MAX, subquery, runtime parameter or BLOB expression uses complete refresh";return plan;}
    plan.grouped=!q.group.empty()||bool(q.having)||!plan.aggregates.empty();plan.aux=plan.grouped?materialized_aux_name(name):"";plan.eligible=true;plan.reason="Bound single-table delta plan";return plan;
}
static json materialized_group_key(const MaterializedDeltaPlan& plan,const json& row){json key=json::array();for(const auto& e:plan.query.group)key.push_back(eval(e,row));return key;}
static json materialized_arguments(const MaterializedDeltaPlan& plan,const json& row){json args=json::array();for(const auto& e:plan.aggregates)args.push_back(e->args[0]->kind==Expr::Star?json(1):eval(e->args[0],row));return args;}
static E materialized_group_expression(const E& expression,const MaterializedDeltaPlan& plan,const json& key,const json& values){
    if(!expression)return nullptr;
    for(std::size_t n=0;n<plan.aggregates.size();++n)if(same_expression(expression,plan.aggregates[n])){auto literal=expr(Expr::Literal);literal->value=values[n];return literal;}
    for(std::size_t n=0;n<plan.query.group.size();++n)if(same_expression(expression,plan.query.group[n])){auto literal=expr(Expr::Literal);literal->value=key[n];return literal;}
    if(expression->kind==Expr::Column)fail("MATERIALIZED_STATE","Ungrouped column escaped bound materialized definition");
    auto copy=std::make_shared<Expr>(*expression);copy->args.clear();for(const auto& arg:expression->args)copy->args.push_back(materialized_group_expression(arg,plan,key,values));return copy;
}
static std::optional<json> materialized_group_output(const MaterializedDeltaPlan& plan,const json& key,const mv::AggregateGroup& group){
    const auto values=group.values();auto evaluate=[&](const E& expression){auto substituted=materialized_group_expression(expression,plan,key,values);bind_expr(substituted,{},false);return eval(substituted,json::array());};
    if(plan.query.having){const auto truth=evaluate(plan.query.having);if(truth.is_null()||!truth.get<bool>())return std::nullopt;}
    json row=json::array();for(const auto& item:plan.query.items)row.push_back(evaluate(item.value));return row;
}
static json materialized_project(const MaterializedDeltaPlan& plan,const json& input){json row=json::array();for(const auto& item:plan.query.items)row.push_back(resolve_blob(eval(item.value,input)));return row;}
static bool materialized_rows_equal(const json& a,const json& b){if(a.size()!=b.size())return false;for(std::size_t n=0;n<a.size();++n)if(range_compare(a[n],b[n])!=0)return false;return true;}
json materialized_aux_metadata(const std::string& view){
    json columns=json::array();for(const auto& name:{"id","group_key","group_state"}){const bool id=std::string(name)=="id";columns.push_back({{"name",name},{"type",id?"INTEGER":"BLOB"},{"length",id?0:16777216},{"not_null",true},{"primary",id},{"unique",false},{"default",nullptr}});}
    json metadata={{"columns",columns},{"rows",json::array()},{"indexes",json::array()},{"organization",{{"method","HEAP"},{"columns",json::array()},{"options",json::object()}}},{"materialized_aux",{{"view",view},{"format",1}}}};return metadata;
}
void materialized_aux_check(Session& session,const std::string& aux,const std::string& view){
    if(!exists(session,aux)){return;}
    const auto metadata=materialized_metadata_table(session,aux);if(!metadata.contains("materialized_aux")||metadata["materialized_aux"].value("view",std::string())!=view)fail("MATERIALIZED_STATE","Generated auxiliary name belongs to another relation");
}
void materialized_delta_bootstrap(Session& session,const std::string& name,json& metadata,const MaterializedDeltaPlan& plan,QueryBudget& budget){
    struct Group{json key;mv::AggregateGroup aggregate;explicit Group(const json& k,const std::vector<mv::AggregateSpec>& specs):key(k),aggregate(specs){}};
    std::map<std::string,Group> groups;
    auto group_for=[&](const json& key)->Group&{auto encoded=equality_key(key);auto found=groups.find(encoded);if(found==groups.end()){budget.materialize(key);found=groups.emplace(encoded,Group(key,plan.specs)).first;budget.materialize(found->second.aggregate.state());}return found->second;};
    if(plan.query.group.empty())group_for(json::array());
    auto source=storage.scan(session.tx,plan.source);StoredRow row;while(source->next(row)){query_work();if(matches(plan.query.where,row.values)){auto& group=group_for(materialized_group_key(plan,row.values));group.aggregate.change(materialized_arguments(plan,row.values),1);}}
    materialized_aux_check(session,plan.aux,name);if(!exists(session,plan.aux))acquire(session,"catalog",true);
    materialized_accesses(session,{plan.source,name},plan.aux);
    std::map<std::string,std::uint64_t> expected,actual;for(const auto& projected:metadata["rows"])++expected[equality_key(projected)];
    metadata["rows"]=json::array();storage.write_table(session.tx,name,metadata);
    auto auxiliary=materialized_aux_metadata(name);add_constraint_indexes(auxiliary,plan.aux);std::uint64_t identifier=0;
    for(auto& entry:groups){query_work();auto& group=entry.second;std::uint64_t output_lid=0;auto output=materialized_group_output(plan,group.key,group.aggregate);
        if(output){auto typed=validate_row(*output,metadata["columns"]);++actual[equality_key(typed)];output_lid=storage.insert_row(session.tx,name,typed).logical_id;}
        auto state=json{{"format",1},{"aggregate",group.aggregate.state()},{"view_lid",output_lid}};
        auxiliary["rows"].push_back(json::array({++identifier,materialized_blob(group.key.dump()),materialized_blob(state.dump())}));budget.materialize(auxiliary["rows"].back());
    }
    if(expected!=actual)fail("MATERIALIZED_STATE","Independent full SQL result differs from delta bootstrap groups");
    validate_table(auxiliary);storage.write_table(session.tx,plan.aux,auxiliary);
}
json materialized_incremental(Session& session,const std::string& name,json metadata,MaterializedDeltaPlan& plan,const json& feed,QueryBudget* shared_budget=nullptr){
    materialized_protocol_guard(session);const auto& events=feed["events"];
    std::unique_ptr<QueryBudget> owned_budget;if(!shared_budget)owned_budget=std::make_unique<QueryBudget>(query_limits);
    QueryBudget& budget=shared_budget?*shared_budget:*owned_budget;budget.cancellation=locks.cancellation_token(session.tx->id);
    QueryBudgetScope budget_scope(budget);PageProgressScope progress([&]{budget.work();budget.check_time();});MaterializedRefreshScope maintenance;
    materialized_accesses(session,{plan.source},name);
    std::uint64_t view_rows=metadata.value("row_count",std::uint64_t(0)),view_changes=0,aux_examined=0,old_inputs=0,new_inputs=0;
    if(!plan.grouped){
        auto remove=[&](const json& projected){auto scan=storage.scan(session.tx,name);StoredRow existing;while(scan->next(existing)){budget.work();if(materialized_rows_equal(existing.values,projected)){scan.reset();storage.erase_row(session.tx,name,existing.id);--view_rows;++view_changes;return;}}fail("MATERIALIZED_STATE","Old projected bag occurrence was not found");};
        for(const auto& event:events){if(!event["old_row"].is_null()){++old_inputs;const auto& before=event["old_row"];if(matches(plan.query.where,before))remove(materialized_project(plan,before));}if(!event["new_row"].is_null()){++new_inputs;const auto& after=event["new_row"];if(matches(plan.query.where,after)){storage.insert_row(session.tx,name,validate_row(materialized_project(plan,after),metadata["columns"]));++view_rows;++view_changes;}}}
    }else{
        materialized_aux_check(session,plan.aux,name);if(!exists(session,plan.aux))fail("MATERIALIZED_STATE","Missing persistent delta aggregate state");materialized_accesses(session,{plan.source,name},plan.aux);
        struct Touched{json key;mv::AggregateGroup aggregate;RowId auxiliary_id;std::uint64_t id=0,view_lid=0;Touched(json k,const std::vector<mv::AggregateSpec>& specs):key(std::move(k)),aggregate(specs){}};
        std::map<std::string,Touched> touched;std::uint64_t largest_id=0;
        auto load=[&](const json& key)->Touched&{
            auto canonical=equality_key(key);auto present=touched.find(canonical);if(present!=touched.end())return present->second;
            Touched state(key,plan.specs);auto cursor=storage.scan(session.tx,plan.aux);StoredRow existing;
            while(cursor->next(existing)){budget.work();++aux_examined;largest_id=std::max(largest_id,existing.values[0].get<std::uint64_t>());if(equality_key(materialized_unblob(existing.values[1]))!=canonical)continue;
                if(state.id){fail("MATERIALIZED_STATE","Duplicate persisted aggregate group key");}
                const auto stored=materialized_unblob(existing.values[2]);if(stored.value("format",0)!=1)fail("MATERIALIZED_STATE","Invalid aggregate auxiliary row");state.aggregate=mv::AggregateGroup::restore(stored["aggregate"],plan.specs);state.view_lid=stored.at("view_lid").get<std::uint64_t>();state.id=existing.values[0].get<std::uint64_t>();state.auxiliary_id=existing.id;
            }
            budget.materialize(key);budget.materialize(state.aggregate.state());return touched.emplace(canonical,std::move(state)).first->second;
        };
        for(const auto& event:events){for(const auto& side:{std::string("old_row"),std::string("new_row")}){const auto& input=event[side];if(input.is_null())continue;if(side=="old_row")++old_inputs;else ++new_inputs;if(!matches(plan.query.where,input))continue;auto& state=load(materialized_group_key(plan,input));state.aggregate.change(materialized_arguments(plan,input),side=="old_row"?-1:1);}}
        for(auto& item:touched){budget.work();auto& state=item.second;RowId output_id;output_id.logical_id=state.view_lid;
            if(!state.aggregate.rows()&&!plan.query.group.empty()){
                if(state.view_lid){storage.erase_row(session.tx,name,output_id);--view_rows;++view_changes;}if(state.id)storage.erase_row(session.tx,plan.aux,state.auxiliary_id);continue;
            }
            auto output=materialized_group_output(plan,state.key,state.aggregate);
            if(output){auto typed=validate_row(*output,metadata["columns"]);if(state.view_lid)storage.update_row(session.tx,name,output_id,typed);else{state.view_lid=storage.insert_row(session.tx,name,typed).logical_id;++view_rows;}++view_changes;}
            else if(state.view_lid){storage.erase_row(session.tx,name,output_id);state.view_lid=0;--view_rows;++view_changes;}
            if(!state.id){if(largest_id==std::uint64_t(std::numeric_limits<std::int64_t>::max()))fail("MATERIALIZED_RESOURCE","Auxiliary group identifier exhausted");state.id=++largest_id;}
            const auto encoded=json{{"format",1},{"aggregate",state.aggregate.state()},{"view_lid",state.view_lid}};
            auto values=json::array({state.id,materialized_blob(state.key.dump()),materialized_blob(encoded.dump())});if(state.auxiliary_id.logical_id)storage.update_row(session.tx,plan.aux,state.auxiliary_id,values);else storage.insert_row(session.tx,plan.aux,values);
        }
    }
    auto state=metadata.value("materialized_refresh_state",json::object());state["base_versions"]=materialized_versions(session,{plan.source});state["maintenance"]=plan.grouped?"INCREMENTAL_GROUP":"INCREMENTAL_SP";state["feed_tx_id"]=session.tx->id;state["last_seq"]=feed["last_seq"];state["rows"]=view_rows;state["delta_count"]=state.value("delta_count",std::uint64_t(0))+1;storage.set_materialized_metadata(session.tx,name,state);
    return {{"view",name},{"maintenance",state["maintenance"]},{"events",events.size()},{"old_inputs",old_inputs},{"new_inputs",new_inputs},{"base_rescan_rows",0},{"auxiliary_rows_examined",aux_examined},{"view_row_operations",view_changes},{"same_transaction",true},{"query_budget",budget.snapshot()}};
}
