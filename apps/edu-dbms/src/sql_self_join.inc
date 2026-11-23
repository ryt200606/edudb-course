static bool self_join_enabled(){const char* raw=std::getenv("EDUDB_OPTIMIZER_SELF_JOIN");const auto value=upper(raw?raw:"ON");if(value=="ON"||value=="1")return true;if(value=="OFF"||value=="0")return false;fail("INVALID_CONFIGURATION","EDUDB_OPTIMIZER_SELF_JOIN must be ON or OFF");}
SelfJoinChoice self_join_choice(const Statement& statement,const std::vector<json>& tables,const Schema& schema,const std::vector<int>& offsets,const std::vector<E>& predicates,double original_cost,bool has_subquery){
    SelfJoinChoice choice;choice.description={{"enabled",self_join_enabled()},{"chosen",false},{"rule","two-alias INNER self join, one non-null unique INTEGER primary-key equality"}};
    auto reject=[&](const std::string& reason){choice.description["reason"]=reason;return choice;};
    if(!choice.description["enabled"].get<bool>())return reject("Rule disabled explicitly");
    if(statement.sources.size()!=2||tables.size()!=2)return reject("Exactly two base relations are required");
    if(schema.size()>65536)return reject("Alias-copy scratch schema exceeds the bounded65536-slot operator profile");
    if(statement.sources[0].table!=statement.sources[1].table)return reject("Different table identities retain the join");
    if(statement.join_kinds.size()!=1||statement.join_kinds[0]!="INNER")return reject("Only INNER association is proved");
    if(!statement.join_hint.empty())return reject("Explicit JOIN method preserves its physical path");
    for(const auto& source:statement.sources)if(source.force_scan||!source.index_hint.empty())return reject("Explicit source access method preserves its physical path");
    if(has_subquery||!decorrelation_total(statement.on)||!decorrelation_total(statement.where))return reject("Subquery or partial/runtime predicates retain the original join and error behavior");
    if(!tables[0].contains("table_id")||tables[0]["table_id"].is_null()||tables[0]["table_id"]!=tables[1]["table_id"]||tables[0]["columns"]!=tables[1]["columns"]||tables[0].value("schema_version",std::uint64_t(0))!=tables[1].value("schema_version",std::uint64_t(0))||tables[0].value("data_version",std::uint64_t(0))!=tables[1].value("data_version",std::uint64_t(0)))return reject("Visible table/schema/data identity could not be proved equal");
    int primary=-1;std::string constraint;
    for(const auto& index:tables[0]["indexes"]){
        if(!index.value("primary",false)||!index.value("unique",false))continue;
        auto columns=index_columns(index);if(columns.size()!=1)continue;const int column=column_id(tables[0],columns[0]);
        if(tables[0]["columns"][column]["type"]!="INTEGER")continue;
        primary=column;constraint=index["name"].get<std::string>();break;
    }
    if(primary<0)return reject("A single INTEGER PRIMARY KEY constraint is required; nullable UNIQUE is insufficient");
    std::vector<E> conditions;conjuncts(statement.on,conditions);bool equality=false;
    for(const auto& condition:conditions){if(condition->kind!=Expr::Binary||condition->text!="="||condition->args.size()!=2)continue;const auto& a=condition->args[0];const auto& b=condition->args[1];if(a->kind!=Expr::Column||b->kind!=Expr::Column)continue;const int x=offsets[0]+primary,y=offsets[1]+primary;if((a->bound==x&&b->bound==y)||(a->bound==y&&b->bound==x)){equality=true;break;}}
    if(!equality)return reject("Top-level AND must contain equality of the same primary-key column across aliases");
    std::set<int> required;auto need=[&](const E& e){if(e)references(e,required);};
    for(const auto& item:statement.items){need(item.value);}
    need(statement.where);need(statement.on);need(statement.having);
    for(const auto& e:statement.group){need(e);}
    for(const auto& order:statement.order){need(order.value);}
    std::set<int> local;const auto width=static_cast<int>(tables[0]["columns"].size());
    for(int slot:required){if(slot<0||slot>=static_cast<int>(schema.size()))return reject("A required slot lies outside the proved alias schema");local.insert(slot<offsets[1]?slot-offsets[0]:slot-offsets[1]);}
    std::vector<int> columns(local.begin(),local.end());choice.required_slots.assign(required.begin(),required.end());
    std::vector<ScanChoice> options;
    for(std::size_t source=0;source<2;++source){auto scan=choose_scan(tables[source],statement.sources[source],schema,offsets[source],predicates,columns);scan.spec.lazy_blobs=true;options.push_back(std::move(scan));}
    choice.source=options[1].cost<options[0].cost?1:0;choice.scan=options[choice.source];
    choice.cost=choice.scan.cost+choice.scan.estimated_rows*(0.02+0.0005*required.size());
    choice.description["proof"]={{"table",statement.sources[0].table},{"table_id",tables[0]["table_id"]},{"primary_constraint",constraint},{"non_null_from_primary_constraint",true},{"key_column",tables[0]["columns"][primary]["name"]},{"alias_width",width},{"pk_equality_pair_multiplicity",1}};
    choice.description["original_estimated_cost"]=original_cost;choice.description["single_scan_estimated_cost"]=choice.cost;choice.description["selected_source"]=choice.source;choice.description["candidate_access"]=choice.scan.description;choice.description["required_slots"]=choice.required_slots;
    if(!(choice.cost<original_cost))return reject("Proved but the normalized single-scan estimate is not strictly lower");
    choice.chosen=true;choice.description["chosen"]=true;choice.description["reason"]="Unique non-null key makes each matching pair the same visible row; original ON and WHERE remain evaluated";return choice;
}
