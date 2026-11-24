/* Included in Engine::Impl. The catalog edition and the data lock plan belong
   to the same SQL transaction. Default writes retain their coarse locks until
   commit, including the parent lookup and all cascading deletes. */
struct ForeignEdge {
    std::string child, parent;
    json rule;
};
struct IntegrityGraph {
    std::vector<ForeignEdge> edges;
    std::set<std::string> constrained;
};
static IntegrityGraph integrity_graph(const json& features) {
    IntegrityGraph graph;
    for(auto it=features.begin();it!=features.end();++it){
        if(!it.value().contains("integrity"))continue;
        graph.constrained.insert(it.key());
        for(const auto& rule:it.value().at("integrity")){
            if(rule.at("kind")!="foreign")continue;
            auto parent=rule.at("parent").get<std::string>();
            graph.edges.push_back({it.key(),parent,rule});
            graph.constrained.insert(parent);
        }
    }
    return graph;
}
// Constraint DDL requires the strict-2PL protocol epoch. The coordinator
// refuses to switch epochs until every transaction of the old epoch finishes.
// A snapshot therefore cannot overlap installation of a new constraint.
void integrity_protocol(Session& s,const std::string& name) {
    if(s.active_options.protocol==TransactionProtocol::Strict2PL)return;
    if(integrity_graph(storage.schema_features(s.tx)).constrained.count(name))
        fail("INTEGRITY_PROTOCOL","Writing a relation with CHECK or foreign-key dependencies requires STRICT_2PL");
}
static std::vector<int> integrity_slots(const json& metadata,const json& names) {
    std::vector<int> slots;
    std::set<int> seen;
    for(const auto& item:names){
        auto slot=column_id(metadata,item.get<std::string>());
        if(!seen.insert(slot).second)fail("INVALID_CONSTRAINT","A foreign key repeats a column");
        slots.push_back(slot);
    }
    return slots;
}
static json integrity_key(const json& row,const std::vector<int>& slots) {
    json key=json::array();for(int slot:slots){if(row.at(slot).is_null())return nullptr;key.push_back(row.at(slot));}return key;
}
static std::string integrity_index(const json& metadata,const json& keys,bool unique) {
    for(const auto& index:metadata.at("indexes")){
        if(unique&&!index.value("unique",false))continue;
        if(index_columns(index)==keys)return index.at("name").get<std::string>();
    }
    return {};
}
static E integrity_check_expr(const json& rule,const json& metadata) {
    auto parsed=Parser("SELECT "+rule.at("sql").get<std::string>()).parse();
    if(parsed.kind!="SELECT"||parsed.items.size()!=1||!parsed.sources.empty())fail("INVALID_CONSTRAINT","CHECK requires one row-local expression");
    const auto e=parsed.items.front().value;
    std::vector<E> pending{e};
    while(!pending.empty()){
        auto n=pending.back();pending.pop_back();
        if(n->kind==Expr::Subquery||n->kind==Expr::Function||n->kind==Expr::Parameter||n->kind==Expr::Star||!n->qualifier.empty())
            fail("INVALID_CONSTRAINT","CHECK supports local unqualified columns, constants, arithmetic and Boolean predicates",n->pos);
        pending.insert(pending.end(),n->args.begin(),n->args.end());
    }
    bind_expr(e,schema_for(metadata,""));require_boolean(e);return e;
}
struct BoundForeign {
    std::string name,parent,index;
    std::vector<int> slots;
};
struct BoundIntegrity {
    std::vector<std::pair<std::string,E>> checks;
    std::vector<BoundForeign> outgoing;
};
BoundIntegrity bind_integrity(Session& s,const std::string& name,const json& metadata) {
    BoundIntegrity bound;
    for(const auto& rule:metadata.value("integrity",json::array())){
        const auto label=rule.at("name").get<std::string>();
        if(rule.at("kind")=="check"){bound.checks.emplace_back(label,integrity_check_expr(rule,metadata));continue;}
        const auto parent=rule.at("parent").get<std::string>();
        const auto pm=parent==name?metadata:storage.read_table(s.tx,parent,false,false);
        if(materialized_metadata(pm)||pm.contains("materialized_aux"))fail("INVALID_CONSTRAINT","A foreign key must reference a base table");
        auto child_slots=integrity_slots(metadata,rule.at("columns"));
        auto parent_slots=integrity_slots(pm,rule.at("keys"));
        if(child_slots.size()!=parent_slots.size()||child_slots.empty())fail("INVALID_CONSTRAINT","Foreign key column counts differ");
        for(std::size_t i=0;i<child_slots.size();++i)
            if(metadata.at("columns")[child_slots[i]].at("type")!=pm.at("columns")[parent_slots[i]].at("type"))
                fail("INVALID_CONSTRAINT","Foreign and referenced column types differ");
        auto index=integrity_index(pm,rule.at("keys"),true);
        if(index.empty())fail("INVALID_CONSTRAINT","Foreign key requires an exact unique or primary key in the referenced column order");
        bound.outgoing.push_back({label,parent,index,std::move(child_slots)});
    }
    return bound;
}
void integrity_check_row(Session& s,const BoundIntegrity& bound,const json& row) {
    for(const auto& check:bound.checks){
        auto value=eval(check.second,row);
        if(!value.is_null()&&!value.get<bool>())fail("CHECK_VIOLATION","CHECK constraint failed: "+check.first);
    }
    for(const auto& fk:bound.outgoing){
        const auto key=integrity_key(row,fk.slots);
        if(key.is_null())continue; // SQL MATCH SIMPLE
        ScanSpec probe;probe.index=fk.index;probe.lower_key=key;probe.upper_key=key;
        auto cursor=storage.scan(s.tx,fk.parent,probe);StoredRow found;
        if(!cursor->next(found))fail("FOREIGN_KEY_VIOLATION","Missing parent for foreign key "+fk.name);
    }
}
void integrity_add(Session& s,const std::string& table_name,json& metadata,const json& incoming) {
    if(std::none_of(incoming.begin(),incoming.end(),[](const json& rule){return rule.at("kind")=="check"||rule.at("kind")=="foreign";}))return;
    if(s.active_options.protocol!=TransactionProtocol::Strict2PL||storage.logical_recovery_mode())
        fail("INTEGRITY_PROTOCOL","Constraint DDL requires STRICT_2PL and physical recovery");
    auto rules=metadata.value("integrity",json::array());
    std::set<std::string> names;
    for(const auto& r:rules)names.insert(r.at("name").get<std::string>());
    for(auto rule:incoming){
        if(rule.at("kind")!="check"&&rule.at("kind")!="foreign")continue;
        if(rules.size()>=64)fail("INVALID_CONSTRAINT","At most 64 CHECK/foreign constraints per table");
        auto label=rule.at("name").get<std::string>();
        if(label.empty()){
            std::size_t i=1;do{label=rule.at("kind").get<std::string>()+"_"+std::to_string(i++);}while(names.count(label));
        }
        if(!names.insert(label).second)fail("DUPLICATE_CONSTRAINT","Duplicate constraint: "+label);
        rule["name"]=label;
        if(rule.at("kind")=="check"){
            auto e=integrity_check_expr(rule,metadata);std::set<int> used;references(e,used);
            rule["columns"]=json::array();
            for(auto slot:used)rule["columns"].push_back(metadata.at("columns")[slot].at("name"));
        }
        rules.push_back(std::move(rule));
    }
    if(rules.empty())return;
    metadata["integrity"]=rules;
    // Validation includes pre-existing rows when ALTER adds a constraint.
    auto bound=bind_integrity(s,table_name,metadata);
    for(const auto& row:metadata.at("rows"))integrity_check_row(s,bound,row);
}
IntegrityGraph integrity_lock(Session& s,const std::string& name,const std::string& operation,const json& directory) {
    auto graph=integrity_graph(directory);
    if(!graph.constrained.count(name))return graph;
    if(s.active_options.protocol!=TransactionProtocol::Strict2PL)fail("INTEGRITY_PROTOCOL","Constrained writes require STRICT_2PL");
    std::map<std::string,bool> lock_set{{name,true}};
    std::vector<std::string> queue{name};std::set<std::string> visited;
    while(!queue.empty()){
        auto table_name=queue.back();queue.pop_back();if(!visited.insert(table_name).second)continue;
        for(const auto& e:graph.edges){
            if(e.child==table_name)lock_set.emplace(e.parent,false);
            if(e.parent!=table_name||operation=="INSERT")continue;
            const bool cascade=operation=="DELETE"&&e.rule.at("on_delete")=="CASCADE";
            auto& write=lock_set[e.child];write=write||cascade;
            if(cascade)queue.push_back(e.child);
        }
    }
    for(const auto& item:lock_set)acquire(s,"table:"+item.first,item.second);
    for(const auto& item:lock_set)if(item.second&&item.first!=name)materialized_write_guard(s,item.first);
    return graph;
}
struct RemovedKey {std::string table;json before,after;};
std::set<std::string> integrity_parent_changes(Session& s,const IntegrityGraph& graph,std::vector<RemovedKey>& removed) {
    std::set<std::string> changed;std::map<std::string,json> metadata;
    auto meta=[&](const std::string& name)->const json&{
        auto it=metadata.find(name);
        if(it==metadata.end())it=metadata.emplace(name,storage.read_table(s.tx,name,false,false)).first;
        return it->second;
    };
    for(std::size_t pos=0;pos<removed.size();++pos){
        // A child deletion enqueues another entry, so hold a value copy.
        auto item=std::move(removed[pos]);
        for(const auto& edge:graph.edges){
            if(edge.parent!=item.table)continue;
            const auto& rule=edge.rule;
            auto pk=integrity_slots(meta(edge.parent),rule.at("keys"));auto old=integrity_key(item.before,pk);
            if(old.is_null()||(!item.after.is_null()&&old==integrity_key(item.after,pk)))continue;
            const auto& child=meta(edge.child);
            auto slots=integrity_slots(child,rule.at("columns"));
            ScanSpec child_scan;child_scan.index=integrity_index(child,rule.at("columns"),false);
            if(!child_scan.index.empty()){child_scan.lower_key=old;child_scan.upper_key=old;}
            auto cursor=storage.scan(s.tx,edge.child,child_scan);StoredRow row;std::vector<StoredRow> matches;
            while(cursor->next(row)){
                if(integrity_key(row.values,slots)!=old)continue;
                if(!item.after.is_null()||rule.at("on_delete")!="CASCADE")
                    fail("FOREIGN_KEY_VIOLATION","Referenced parent is still used by "+edge.child+"."+rule.at("name").get<std::string>());
                matches.push_back(row);
            }
            cursor.reset();
            for(const auto& dependent:matches){
                storage.erase_row(s.tx,edge.child,dependent.id);changed.insert(edge.child);
                removed.push_back({edge.child,dependent.values,nullptr});
            }
        }
    }
    return changed;
}
void integrity_rename(Session& s,const Statement& st,json& target) {
    const bool column=st.action=="RENAME_COLUMN";
    auto rename_names=[&](json& names){for(auto& n:names)if(n==st.column)n=st.newname;};
    auto rewrite=[&](const std::string& owner,json& metadata){
        bool changed=false;
        if(!metadata.contains("integrity"))return changed;
        for(auto& rule:metadata["integrity"]){
            if(column&&owner==st.table){
                auto& slots=rule["columns"];
                if(std::find(slots.begin(),slots.end(),json(st.column))!=slots.end()){
                    if(rule.at("kind")=="check"){
                        // Replace just the bound column tokens, preserving strings, comments and grouping.
                        std::string source=rule.at("sql").get<std::string>();
                        auto expression=integrity_check_expr(rule,metadata);
                        std::vector<E> pending{expression};std::set<std::size_t,std::greater<std::size_t>> positions;
                        while(!pending.empty()){
                            auto n=pending.back();pending.pop_back();
                            if(n->kind==Expr::Column&&n->text==st.column)positions.insert(static_cast<std::size_t>(n->pos)-7);
                            pending.insert(pending.end(),n->args.begin(),n->args.end());
                        }
                        for(auto start:positions){
                            std::size_t end=start;
                            if(source.at(start)=='"'){
                                ++end;
                                while(end<source.size()){
                                    if(source[end++]!='"')continue;
                                    if(end<source.size()&&source[end]=='"'){++end;continue;}
                                    break;
                                }
                            }else end+=st.column.size();
                            source.replace(start,end-start,interchange::quote_identifier(st.newname));
                        }
                        rule["sql"]=std::move(source);
                    }
                    rename_names(slots);changed=true;
                }
            }
            if(rule.at("kind")=="foreign"&&rule.at("parent")==st.table){
                if(column){auto& keys=rule["keys"];if(std::find(keys.begin(),keys.end(),json(st.column))==keys.end())continue;rename_names(keys);}
                else rule["parent"]=st.newname;
                changed=true;
            }
        }
        return changed;
    };
    rewrite(st.table,target);
    std::set<std::string> children;
    for(const auto& edge:integrity_graph(storage.schema_features(s.tx)).edges)if(edge.parent==st.table&&edge.child!=st.table)children.insert(edge.child);
    for(const auto& child:children){
        acquire(s,"table:"+child,true);auto metadata=table(s,child);
        if(rewrite(child,metadata))storage.write_table(s.tx,child,metadata);
    }
}
