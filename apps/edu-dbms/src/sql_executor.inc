// Query binding, planning and execution. Included within Engine::Impl.

#include "sql_statistics_ddl.inc"

json analyze(Session& session,Statement& statement){
    QueryBudget budget(query_limits);budget.cancellation=locks.cancellation_token(session.tx->id);QueryBudgetScope scope(budget);
    PageProgressScope page_scope([&]{budget.work();budget.check_time();});
    acquire(session,"table:"+statement.table,true);
    auto metadata=table(session,statement.table,false);std::size_t target=16,sample_limit=256;
    for(auto it=statement.options.begin();it!=statement.options.end();++it){if(it.key()!="target"&&it.key()!="sample_rows")fail("INVALID_OPTION","ANALYZE supports target and sample_rows");if(!it.value().is_number_integer()||it.value().get<std::int64_t>()<1||it.value().get<std::int64_t>()>4096)fail("INVALID_OPTION","ANALYZE options must be integers1..4096");if(it.key()=="target")target=it.value().get<std::size_t>();else sample_limit=it.value().get<std::size_t>();}
    const auto memory=operator_memory_limit();const auto width=metadata["columns"].size();
    if(width>memory/2048)fail("STATISTICS_MEMORY","Too many columns for bounded NDV statistics");
    struct ColumnState{std::uint64_t nulls=0;json minimum=nullptr,maximum=nullptr;std::vector<unsigned char> hll=std::vector<unsigned char>(1024);};
    std::vector<ColumnState> columns(width);std::vector<json> sample;std::uint64_t rows=0,total_width=0;std::size_t sample_bytes=0;std::mt19937_64 random(13011301);
    ScanSpec spec;spec.lazy_blobs=true;auto cursor=storage.scan(session.tx,statement.table,spec);StoredRow stored;
    while(cursor->next(stored)){query_work();++rows;const auto& row=stored.values;
        for(std::size_t i=0;i<width;++i){auto& c=columns[i];const auto& v=row[i];if(v.is_null()){++c.nulls;continue;}auto type=metadata["columns"][i]["type"].get<std::string>();
            if(type=="BLOB"){total_width+=lazy_blob(v)?v.value("bytes",std::uint64_t(0)):v["$blob"].get_ref<const std::string&>().size()/2;continue;}
            total_width+=v.is_string()?v.get_ref<const std::string&>().size():v.is_boolean()?1:8;
            if(c.minimum.is_null()||compare_values(v,c.minimum)<0)c.minimum=v;
            if(c.maximum.is_null()||compare_values(v,c.maximum)>0)c.maximum=v;
            auto key=query::equality_key(json::array({v}));std::uint64_t h=1469598103934665603ull;for(unsigned char b:key){h^=b;h*=1099511628211ull;}h^=h>>30;h*=0xbf58476d1ce4e5b9ull;h^=h>>27;h*=0x94d049bb133111ebull;h^=h>>31;
            auto index=h&1023u;h>>=10;unsigned char rank=1;while(rank<55&&!(h&1)){++rank;h>>=1;}c.hll[index]=std::max(c.hll[index],rank);
        }
        std::size_t index=sample.size();if(sample.size()>=sample_limit){auto choice=std::uniform_int_distribution<std::uint64_t>(0,rows-1)(random);if(choice>=sample_limit)continue;index=std::size_t(choice);}
        json copy=row;for(std::size_t i=0;i<width;++i)if(metadata["columns"][i]["type"]=="BLOB"&&!copy[i].is_null())copy[i]=nullptr;
        auto n=std::size_t(json_allocation_estimate(copy)+32),old=index<sample.size()?std::size_t(json_allocation_estimate(sample[index])+32):0;
        if(sample_bytes-old+n>memory/2)fail("STATISTICS_MEMORY","ANALYZE sample exceeds its memory budget; use smaller sample_rows");
        sample_bytes=sample_bytes-old+n;query_materialize(copy);if(index==sample.size())sample.push_back(std::move(copy));else sample[index]=std::move(copy);
    }
    cursor.reset();json stats={{"version",1},{"data_version",metadata.value("data_version",std::uint64_t(0))},{"schema_version",metadata.value("schema_version",std::uint64_t(0))},{"analyzed_rows",rows},{"sample_rows",sample.size()},{"sampled",rows>sample.size()},{"seed",13011301},{"avg_row_width",rows?double(total_width)/rows:0},{"columns",json::array()},{"distribution_scope","Fixed-seed uniform reservoir; NULL/min/max counts and HLL consume the full scan; small-table NDV is exact"}};
    for(std::size_t i=0;i<width;++i){auto& c=columns[i];bool blob=metadata["columns"][i]["type"]=="BLOB";std::vector<json> values;for(const auto& row:sample)if(!row[i].is_null())values.push_back(row[i]);std::sort(values.begin(),values.end(),[](const json&a,const json&b){return compare_values(a,b)<0;});
        std::vector<std::pair<json,std::size_t>> counts;for(const auto& v:values){if(counts.empty()||compare_values(counts.back().first,v))counts.push_back({v,1});else ++counts.back().second;}
        double ndv=double(counts.size());if(rows>sample.size()&&!blob){double sum=0;std::size_t zero=0;for(auto r:c.hll){sum+=std::ldexp(1.0,-int(r));zero+=r==0;}ndv=0.7213/(1+1.079/1024)*1024*1024/sum;if(ndv<=2560&&zero)ndv=1024*std::log(1024.0/zero);ndv=std::min(double(rows-c.nulls),std::max(double(counts.size()),ndv));}
        json hist=json::array(),mcv=json::array();double denominator=sample.empty()?1:double(sample.size());auto ranked=counts;std::stable_sort(ranked.begin(),ranked.end(),[](const auto&a,const auto&b){return a.second>b.second;});for(std::size_t k=0;k<std::min(target,ranked.size());++k)mcv.push_back({{"value",ranked[k].first},{"frequency",ranked[k].second/denominator}});
        std::size_t bucket=0;for(std::size_t k=0;k<counts.size();++k){bucket+=counts[k].second;if(bucket>=std::max<std::size_t>(1,values.size()/target)||k+1==counts.size()){hist.push_back({{"upper",counts[k].first},{"frequency",bucket/denominator}});bucket=0;}}
        stats["columns"].push_back({{"column_id",i},{"name",metadata["columns"][i]["name"]},{"null_count",c.nulls},{"ndv",blob?json(nullptr):json(ndv)},{"ndv_exact",rows==sample.size()&&!blob},{"min",c.minimum},{"max",c.maximum},{"mcv",mcv},{"histogram",hist},{"type",metadata["columns"][i]["type"]}});
    }
    collect_joint_statistics(metadata,sample,target,rows,stats,memory);
    storage.set_statistics(session.tx,statement.table,stats);auto result=blank_result();result["columns"]={{{"name","table"},{"type","VARCHAR"}},{{"name","rows"},{"type","INTEGER"}},{{"name","sample_rows"},{"type","INTEGER"}}};result["rows"].push_back(json::array({statement.table,rows,sample.size()}));result["statistics"]=stats;result["plan"]={{"node","Analyze"},{"table",statement.table},{"full_scan_rows",rows},{"sample_rows",sample.size()},{"memory_bytes_estimate",sample_bytes}};return result;
}

#include "sql_relational.inc"
json select(Session& session,Statement& statement,QueryBudget* shared_budget=nullptr,bool internal_output=false) {
    if(!statement.set_operation.empty())return select_set(session,statement,shared_budget,internal_output);
    std::unique_ptr<QueryBudget> local_budget;if(!shared_budget)local_budget=std::make_unique<QueryBudget>(query_limits);
    QueryBudget& budget=shared_budget?*shared_budget:*local_budget;
    budget.cancellation=locks.cancellation_token(session.tx->id);
    QueryBudgetScope budget_scope(budget);InternalQueryOutputScope output_scope(internal_output);
    PageProgressScope page_scope([&]{budget.work();budget.check_time();});
    BlobReaderScope blob_scope([&](const json& v,std::uint64_t start,std::uint64_t length){return storage.blob_value(session.tx,v,start,length);});
    const auto planning_counter_start=storage.transaction_stats(session.tx);
    std::vector<json> tables;
    json planning_metadata_io=json::object();
    Schema schema;
    std::vector<int> offsets;
    std::set<std::string> aliases;
    std::vector<std::string> lock_names;
    for (const auto& source : statement.sources) lock_names.push_back(source.table);
    std::sort(lock_names.begin(), lock_names.end());
    if(!try_record_locks(session,statement))for (const auto& name : lock_names) acquire(session, "table:" + name, false);
    for (const auto& source : statement.sources) {
        if (!aliases.insert(source.alias).second) fail("DUPLICATE_ALIAS", "Duplicate alias: " + source.alias);
        offsets.push_back(int(schema.size()));
        auto metadata_before=storage.transaction_stats(session.tx);
        auto metadata = table(session, source.table, false);
        auto metadata_after=storage.transaction_stats(session.tx);
        for(const char* counter:{"heap_page_reads","index_page_reads"})planning_metadata_io[counter]=planning_metadata_io.value(counter,std::uint64_t(0))+metadata_after.value(counter,std::uint64_t(0))-metadata_before.value(counter,std::uint64_t(0));
        auto columns = schema_for(metadata, source.alias);
        for(std::size_t c=0;c<columns.size();++c){columns[c].relation_id=int(tables.size());columns[c].column_id=int(c);columns[c].slot_id=int(schema.size()+c);}
        schema.insert(schema.end(), columns.begin(), columns.end());
        tables.push_back(std::move(metadata));
    }

    json subquery_actual=json::array(),subquery_bindings=json::array();std::set<const Expr*> bound_subqueries;std::uint64_t subquery_evaluations=0,subquery_total_rows_examined=0;
    SubqueryCallbacksScope subquery_callbacks(
        [&](const E& expression,const Schema& visible){
            if((expression->text=="EXISTS"||expression->text=="NOT_EXISTS")&&(!expression->query->set_operation.empty()||(expression->query->distinct&&expression->query->offset)))fail("UNSUPPORTED_SQL","EXISTS over a compound query or DISTINCT with OFFSET is not supported in this Apply stage",expression->pos);
            OuterBindingScope outer(visible);Statement binding=*expression->query;binding.explain=true;binding.analyze=false;
            auto explained=select(session,binding,&budget,true);const auto columns=explained["output_schema"];
            if(expression->text!="EXISTS"&&expression->text!="NOT_EXISTS"&&columns.size()!=1)fail("SUBQUERY_COLUMN_COUNT","Scalar and IN subqueries require exactly one output column",expression->pos);
            if(expression->text=="SCALAR")expression->type=columns[0]["type"].get<std::string>();
            else{
                expression->type="BOOLEAN";
                if(expression->text=="IN"||expression->text=="NOT_IN"){
                    const auto a=expression->args[0]->type,b=columns[0]["type"].get<std::string>();
                    if(a!="NULL"&&b!="NULL"&&(a=="BLOB"||b=="BLOB"))fail("UNSUPPORTED_SQL","IN does not compare BLOB values",expression->pos);
                    if(a!="NULL"&&b!="NULL"&&a!=b&&!(numeric(a)&&numeric(b)))fail("TYPE_ERROR","IN operand types are incompatible",expression->pos);
                }
            }
            expression->subquery_plan=explained["plan"];if(bound_subqueries.insert(expression.get()).second&&subquery_bindings.size()<64)subquery_bindings.push_back({{"mode",expression->text},{"physical_plan",explained["plan"]}});
        },
        [&](const E& expression,const json& outer_row){
            query_work();++subquery_evaluations;
            auto singleton=[&](const json& value,const std::string& name){return query::scan([value](){auto sent=std::make_shared<bool>(false);return query::Pull([value,sent](json& row){if(*sent)return false;*sent=true;row=value;return true;});},name);};
            const auto input_value=expression->args.empty()?json():eval(expression->args[0],outer_row);
            query::ApplySpec spec;spec.mode=expression->text;spec.output_slot=int(outer_row.size());spec.value=[input_value](const json&){return input_value;};spec.scalar_compare=compare_values;
            json inner_plan=nullptr;
            spec.inner=[&](const json& row){
                OuterParameterScope parameters(row);Statement inner=*expression->query;
                if(expression->text=="EXISTS"||expression->text=="NOT_EXISTS"){
                    if(!inner.set_operation.empty()||(inner.distinct&&inner.offset))fail("UNSUPPORTED_SQL","EXISTS over a compound query or DISTINCT with OFFSET is not supported in this Apply stage",expression->pos);
                    bool aggregate_target=bool(inner.having);for(const auto& item:inner.items)aggregate_target=aggregate_target||aggregate(item.value);for(const auto& order:inner.order)aggregate_target=aggregate_target||aggregate(order.value);
                    auto one=expr(Expr::Literal);one->value=1;
                    if(aggregate_target&&inner.group.empty()){auto count=expr(Expr::Function,"COUNT");set_arguments(count,{expr(Expr::Star,"*")});inner.items={{count,""}};}
                    else inner.items={{one,""}};
                    inner.distinct=false;inner.order.clear();
                    if(inner.limit!=0)inner.limit=std::min(inner.limit,std::size_t(1));
                }
                inner.explain=inner.analyze=false;auto response=select(session,inner,&budget,true);inner_plan=response["plan"];subquery_total_rows_examined+=response.value("metrics",json::object()).value("rows_scanned",std::uint64_t(0));
                auto rows=std::make_shared<json>(std::move(response["rows"]));return query::scan([rows](){auto at=std::make_shared<std::size_t>(0);return query::Pull([rows,at](json& result){if(*at>=rows->size())return false;result=std::move((*rows)[(*at)++]);return true;});},"BoundedSameTransactionSubqueryResult");
            };
            query::ExecContext apply_context;apply_context.memory_limit=operator_memory_limit();apply_context.work=[](){query_work();};apply_context.materialize=[](const json& row){query_materialize(row);};apply_context.row_bytes=[](const json& row){return std::size_t(json_allocation_estimate(row)+32);};
            auto execution=query::apply(singleton(outer_row,"CurrentOuterRow"),std::move(spec));execution->open(apply_context);json applied;if(!execution->next(applied))fail("INTERNAL_ERROR","Apply did not return the current outer row");execution->close();
            if(subquery_actual.size()<32)subquery_actual.push_back({{"mode",expression->text},{"physical_actual",execution->diagnostics()},{"inner_plan",inner_plan}});
            return applied.back();
        });

    std::vector<SelectItem> expanded;
    for (const auto& item : statement.items) {
        if (item.value->kind != Expr::Star) { expanded.push_back(item); continue; }
        bool found = false;
        for (const auto& column : schema) {
            if (!item.value->qualifier.empty() && item.value->qualifier != column.table) continue;
            auto expression = expr(Expr::Column, column.name);
            expression->qualifier = column.table;
            expanded.push_back({expression, ""}); found = true;
        }
        if (!found) fail("TABLE_NOT_FOUND", "Wildcard has no matching table");
    }
    statement.items = std::move(expanded);
    for (auto& item : statement.items) bind_expr(item.value, schema, true);
    bind_expr(statement.where, schema); require_boolean(statement.where);
    if(statement.join_conditions.empty()){bind_expr(statement.on,schema);require_boolean(statement.on);}
    else{
        for(const auto& condition:statement.join_conditions){
            const auto count=condition.second;
            const auto end=offsets.at(count-1)+int(tables.at(count-1)["columns"].size());
            Schema visible(schema.begin(),schema.begin()+end);
            bind_expr(condition.first,visible);require_boolean(condition.first);
        }
        std::function<void(const E&)> mark_join_conjunctions=[&](const E& e){
            if(e&&e->kind==Expr::Binary&&e->text=="AND"){
                for(const auto& argument:e->args)mark_join_conjunctions(argument);
                e->type="BOOLEAN";
            }
        };
        mark_join_conjunctions(statement.on);
    }

    auto resolve_output_reference = [&](E expression, bool prefer_input = false) {
        if (expression->kind == Expr::Literal && expression->value.is_number_integer()) {
            auto ordinal = expression->value.get<std::int64_t>();
            if (ordinal < 1 || std::size_t(ordinal) > statement.items.size())
                fail("COLUMN_NOT_FOUND", "Output position outside SELECT list");
            return statement.items[ordinal - 1].value;
        }
        if (expression->kind == Expr::Column && expression->qualifier.empty()) {
            // SQL resolves GROUP BY name conflicts in favor of the input
            // column. ORDER BY intentionally resolves output aliases first.
            if (prefer_input) for (const auto& column : schema)
                if (column.name == expression->text) return expression;
            int found = -1;
            for (std::size_t i = 0; i < statement.items.size(); ++i) {
                const auto& item = statement.items[i];
                auto label = item.alias.empty() ? expression_label(item.value) : item.alias;
                if (label != expression->text) continue;
                if (found >= 0 && !same_expression(statement.items[found].value,item.value))
                    fail("AMBIGUOUS_COLUMN", "Ambiguous output name: " + expression->text);
                found = int(i);
            }
            if (found >= 0) return statement.items[found].value;
        }
        return expression;
    };
    for (auto& key : statement.group) {
        key = resolve_output_reference(key, true);
        bind_expr(key, schema, false);
        if(key->type=="BLOB")fail("UNSUPPORTED_SQL","GROUP BY BLOB is not supported");
    }
    bind_expr(statement.having, schema, true); require_boolean(statement.having);
    for (auto& sort : statement.order) {
        sort.value = resolve_output_reference(sort.value);
        bind_expr(sort.value, schema, true);
        if(sort.value->type=="BLOB")fail("UNSUPPORTED_SQL","ORDER BY BLOB is not supported");
    }
    if (statement.distinct) for (const auto& sort : statement.order) {
        bool projected = false;
        for (const auto& item : statement.items) projected = projected || same_expression(sort.value, item.value);
        if (!projected) fail("INVALID_ORDER", "DISTINCT query ORDER BY expression must occur in SELECT list");
    }
    bool grouped = !statement.group.empty() || bool(statement.having);
    for (const auto& item : statement.items) grouped = grouped || aggregate(item.value);
    for (const auto& sort : statement.order) grouped = grouped || aggregate(sort.value);
    if (grouped) {
        for (const auto& item : statement.items)
            if (!valid_group_expression(item.value, statement.group))
                fail("INVALID_AGGREGATE", "SELECT expression must be aggregated or depend on GROUP BY keys");
        if (!valid_group_expression(statement.having, statement.group))
            fail("INVALID_AGGREGATE", "HAVING expression must depend on GROUP BY keys or aggregates");
        for (const auto& sort : statement.order)
            if (!valid_group_expression(sort.value, statement.group))
                fail("INVALID_AGGREGATE", "ORDER BY expression must depend on GROUP BY keys or aggregates");
    }

    bool has_subquery=subquery_expression(statement.where)||subquery_expression(statement.on)||subquery_expression(statement.having);
    for(const auto& item:statement.items)has_subquery=has_subquery||subquery_expression(item.value);
    for(const auto& key:statement.group)has_subquery=has_subquery||subquery_expression(key);
    for(const auto& sort:statement.order)has_subquery=has_subquery||subquery_expression(sort.value);
    bool post_group_subquery=subquery_expression(statement.having);for(const auto& item:statement.items)post_group_subquery=post_group_subquery||subquery_expression(item.value);for(const auto& key:statement.group)post_group_subquery=post_group_subquery||subquery_expression(key);for(const auto& sort:statement.order)post_group_subquery=post_group_subquery||subquery_expression(sort.value);
    if(grouped&&post_group_subquery)fail("UNSUPPORTED_SQL","Subquery expressions after outer aggregation require the later grouped-Apply stage; WHERE/ON Apply is supported before grouping");
    const bool has_outer=std::any_of(statement.join_kinds.begin(),statement.join_kinds.end(),[](const std::string& k){return k!="INNER";});
    if(has_outer&&!statement.join_hint.empty())fail("INVALID_JOIN_METHOD","Outer joins currently use the actual spool-block outer operator; inner-only algorithm hints cannot rename it");
    std::vector<E> predicates;
    if(!has_outer){std::vector<E> all;conjuncts(statement.where,all);conjuncts(statement.on,all);for(const auto& e:all)if(!runtime_expression(e))predicates.push_back(e);}
    std::vector<ScanChoice> scans;
    std::vector<std::vector<E>> pushed(tables.size());
    json plan = {
        {"node", statement.limit != std::numeric_limits<std::size_t>::max() ? "Limit" : grouped ? "Aggregate" : "Projection"},
        {"sources", json::array()}, {"predicate_pushdown", false},
        {"join_outer", nullptr}, {"distinct", statement.distinct},
        {"sort", !statement.order.empty()}, {"group_keys", statement.group.size()},
        {"having", bool(statement.having)},
        {"limit", statement.limit == std::numeric_limits<std::size_t>::max() ? json(nullptr) : json(statement.limit)},
        {"cost_model", "normalized page/tuple heuristic; uniform integer span, no histogram"}
    };
    plan["query_limits"] = query_limits.describe();
    if(has_subquery){plan["subquery_bindings"]=subquery_bindings;plan["subquery_execution"]={{"node","CorrelatedApply"},{"scope","expression pull; bounded same-transaction inner SELECT; no public Engine recursion"},{"decorrelated",false},{"required_columns","conservatively retain all visible outer columns"}};}
    plan["planning_metadata_io"]=planning_metadata_io;
    plan["planning_metadata_io_scope"]="logical storage page accesses during metadata/statistics traversal; separate from execution-node counters";
    for (std::size_t i = 0; i < tables.size(); ++i) {
        int first = offsets[i], last = first + int(tables[i]["columns"].size());
        for (const auto& predicate : predicates) {
            std::set<int> references_set; references(predicate, references_set);
            bool local = !references_set.empty();
            for (int reference : references_set) if (reference < first || reference >= last) local = false;
            if (local) pushed[i].push_back(predicate);
        }
        std::set<int> needed;
        auto need_expression=[&](const E& e){if(e)references(e,needed);};
        for(auto& item:statement.items)need_expression(item.value);
        need_expression(statement.where);need_expression(statement.on);need_expression(statement.having);
        for(auto& e:statement.group)need_expression(e);
        for(auto& e:statement.order)need_expression(e.value);
        std::vector<int> local_needed;for(auto c:needed)if(c>=first&&c<last)local_needed.push_back(c-first);if(has_subquery){local_needed.clear();for(int c=first;c<last;++c)local_needed.push_back(c-first);}
        auto scan_source_option=statement.sources[i];
        std::string parameterized_hint;
        if(tables.size()>1&&statement.join_hint=="INDEX_NESTED_LOOP"&&!scan_source_option.index_hint.empty()){
            for(const auto& ix:tables[i]["indexes"])if(ix["name"]==scan_source_option.index_hint){
                auto keys=index_columns(ix);if(keys.size()!=1)break;
                const int key_slot=first+column_id(tables[i],keys[0]);
                for(const auto& e:predicates)if(e->kind==Expr::Binary&&e->text=="="&&e->args[0]->kind==Expr::Column&&e->args[1]->kind==Expr::Column){
                    const int a=e->args[0]->bound,b=e->args[1]->bound;
                    if((a==key_slot&&(b<first||b>=last))||(b==key_slot&&(a<first||a>=last))){parameterized_hint=scan_source_option.index_hint;scan_source_option.index_hint.clear();break;}
                }
                break;
            }
        }
        scans.push_back(choose_scan(tables[i],scan_source_option,schema,first,predicates,local_needed));
        if(!parameterized_hint.empty()){
            auto& d=scans.back().description;d["parameterized_index_hint"]=parameterized_hint;
            d["scan"]="ParameterizedIndexProbe";d["index"]=parameterized_hint;
            d["selection"]="validated outer-tuple parameter supplies the leading equality key";
        }
        scans.back().spec.lazy_blobs=true;
        if(shared_scan_matches(session.tx,statement.sources[i].table)){
            auto& shared=*shared_scan_state;
            if(!scans.back().empty&&statement.limit!=0)shared.needs_run=true;
            auto original=scans.back().description;
            scans.back().spec.mode="TABLE";scans.back().spec.index.clear();scans.back().spec.indexes.clear();
            scans.back().spec.lower.reset();scans.back().spec.upper.reset();scans.back().spec.lower_key=nullptr;scans.back().spec.upper_key=nullptr;
            scans.back().description["unexecuted_original_access"]=std::move(original);
            scans.back().description["scan"]=scans.back().empty?"EmptyScan":"SharedSpoolScan";
            scans.back().description["index"]=nullptr;scans.back().description["indexes"]=json::array();
            scans.back().description["selection"]="explicit one-base-scan read-only batch; CRC temporary spool reused";
        }
        scans.back().description["pushed_filters"] = pushed[i].size();
        plan["sources"].push_back(scans.back().description);
        plan["predicate_pushdown"] = plan["predicate_pushdown"].get<bool>() || !pushed[i].empty();
    }
    if(!shared_scan_state&&tables.size()==1&&!statement.where&&!statement.on&&statement.items.size()==1&&statement.items[0].value->kind==Expr::Function&&statement.items[0].value->text=="COUNT"&&statement.items[0].value->args[0]->kind==Expr::Star&&statement.group.empty()&&!statement.having&&!statement.distinct&&statement.order.empty()&&!statement.sources[0].force_scan&&statement.sources[0].index_hint.empty()){
        for(auto& ix:tables[0]["indexes"])if(ix.value("method",std::string("BPLUS"))=="BITMAP"){
            scans[0].spec.mode="BITMAP";scans[0].spec.index=ix["name"].get<std::string>();scans[0].spec.indexes={scans[0].spec.index};scans[0].spec.condition=nullptr;scans[0].spec.lower_key=nullptr;scans[0].spec.upper_key=nullptr;scans[0].spec.lower.reset();scans[0].spec.upper.reset();
            scans[0].description["scan"]="ExistenceBitmapScan";scans[0].description["index"]=ix["name"];scans[0].description["indexes"]=scans[0].spec.indexes;scans[0].description["selection"]="COUNT(*) uses persisted existence bitmap";plan["sources"][0]=scans[0].description;plan["node"]="BitmapCount";break;
        }
    }
    OptimizerResult optimized;
    if(scans.size()>1){optimized=has_outer?outer_join_plan(statement,tables,scans):optimize_joins(statement,tables,offsets,scans,predicates);plan["physical_plan"]=optimizer_plan_json(optimized.plan);plan["join_algorithm"]=optimized.plan->algorithm;plan["estimated_cost"]=optimized.plan->cost;plan["estimated_rows"]=optimized.plan->rows;plan["optimizer"]={{"search",optimized.fallback?"bounded greedy fallback":"subset dynamic programming with interesting orders"},{"candidates",optimized.candidates},{"fallback",optimized.fallback},{"fallback_reason",optimized.fallback_reason},{"exact_global_optimum_claimed",false},{"memory_limit",operator_memory_limit()}};}
    auto self_join=self_join_choice(statement,tables,schema,offsets,predicates,optimized.plan?optimized.plan->cost:0.0,has_subquery);
    plan["self_join_elimination"]=self_join.description;
    if(self_join.chosen){
        const auto chosen=self_join.source,removed=1-chosen;scans[chosen]=self_join.scan;scans[chosen].description["pushed_filters"]=pushed[chosen].size();
        plan["sources"][chosen]=scans[chosen].description;plan["sources"][removed]["eliminated"]=true;plan["sources"][removed]["elimination_reason"]="same visible row reconstructed from a non-null unique primary key";
        for(const char* counter:{"actual_rows","rows_examined","loops","heap_page_reads","index_page_reads","row_fetches","cover_hits","bitmap_words"})plan["sources"][removed][counter]=0;
        plan["sources"][removed]["opened"]=false;plan["optimizer"]["post_rule"]="proved same-row self join with strictly lower normalized estimate";
        plan["unexecuted_original_physical_plan"]=plan["physical_plan"];plan["original_join_algorithm"]=plan["join_algorithm"];
        plan["physical_plan"]={{"node","SelfJoinAliasCopy"},{"source",chosen},{"estimated_cost",self_join.cost},{"estimated_rows",self_join.scan.estimated_rows},{"constraint_proof",self_join.description["proof"]},{"input",scans[chosen].description}};
        plan["join_algorithm"]="SELF_JOIN_ELIMINATED";plan["estimated_cost"]=self_join.cost;plan["estimated_rows"]=self_join.scan.estimated_rows;
    }
    if(has_outer){plan["optimizer"]["search"]="fixed SQL outer-join association";plan["optimizer"]["reordering_disabled"]=true;plan["optimizer"]["unsafe_predicate_pushdown_disabled"]=true;}
    plan["cost_model"]="normalized sequential/random-page and tuple units; persisted ANALYZE statistics when fresh; never measured milliseconds";
    bool streaming = !grouped && !statement.distinct && statement.order.empty();
    plan["execution_mode"] = streaming ? "streaming" : (!grouped&&!statement.distinct?"blocking final ordering with bounded Top-K or external sort":"materialized group/distinct before final ordering");
    const auto sort_choice=statement.order.empty()?FinalSortChoice{}:final_sort_choice(statement);
    if(!statement.order.empty())plan["final_sort"]=sort_choice.description;
    json decorrelation_description;
    const double outer_rows_estimate=optimized.plan?optimized.plan->rows:scans.empty()?1.0:scans[0].estimated_rows;
    auto decorrelations=decorrelation_plans(session,statement,schema,outer_rows_estimate,decorrelation_description);
    const E execution_where=decorrelation_residual(statement.where,decorrelations);
    plan["decorrelation"]=decorrelation_description;plan["decorrelation_actual"]=json::array();
    for(const auto& rule:decorrelations)plan["decorrelation_actual"].push_back({{"operator",rule.anti?"HashAntiJoin":"HashSemiJoin"},{"inner_table",rule.source.table},{"inner_loops",0},{"rows_examined",0},{"actual_rows",0},{"heap_page_reads",0},{"index_page_reads",0}});
    const auto planning_counter_end=storage.transaction_stats(session.tx);
    for(const char* counter:{"heap_page_reads","index_page_reads"})planning_metadata_io[counter]=planning_counter_end.value(counter,std::uint64_t(0))-planning_counter_start.value(counter,std::uint64_t(0));
    plan["planning_metadata_io"]=planning_metadata_io;
    auto result = blank_result();
    result["metrics"]["planning_metadata_io"]=planning_metadata_io;
    for (const auto& item : statement.items)
        result["columns"].push_back({{"name", item.alias.empty() ? expression_label(item.value) : item.alias},
                                     {"type", item.value->type}});
    result["output_schema"]=result["columns"];
    const double unrewritten_cost=self_join.chosen?self_join.cost:optimized.plan?optimized.plan->cost:scans.empty()?0.0:scans[0].cost;
    if(auto rewritten=materialized_rewrite(session,statement,plan,result["output_schema"],budget,internal_output,unrewritten_cost))return *rewritten;
    planning_metadata_io=plan["planning_metadata_io"];result["metrics"]["planning_metadata_io"]=planning_metadata_io;
    if (statement.explain && !statement.analyze) {
        result["plan"] = plan;
        result["columns"] = {{{"name", "QUERY PLAN"}, {"type", "VARCHAR"}}};
        result["rows"].push_back(json::array({plan.dump(2)}));
        budget.check_time();result["metrics"]["query_budget"] = budget.snapshot();
        return result;
    }

    const auto execution_started = Clock::now();
    if(!shared_scan_state&&decorrelations.empty()&&tables.size()==1&&statement.items.size()==1&&statement.items[0].value->kind==Expr::Function&&statement.items[0].value->text=="COUNT"&&statement.items[0].value->args[0]->kind==Expr::Star&&statement.group.empty()&&!statement.having&&!statement.distinct&&statement.order.empty()&&statement.offset==0&&statement.limit!=0&&scans[0].spec.mode=="BITMAP"&&access_predicate(statement.where,0,int(schema.size()))==scans[0].spec.condition){
        auto count=storage.count(session.tx,statement.sources[0].table,scans[0].spec);
        if(count){json row=json::array({*count});query_output(row);result["rows"].push_back(row);plan["node"]="BitmapCount";plan["sources"][0]["actual_rows"]=*count;plan["sources"][0]["rows_examined"]=0;plan["sources"][0]["loops"]=1;plan["actual"]={{"actual_rows",1},{"loops",1},{"rows_examined",0},{"bitmap_matched_rows",*count},{"heap_access_required",false}};result["plan"]=plan;result["metrics"]={{"rows_scanned",0},{"actual_rows",1},{"loops",1},{"bitmap_matched_rows",*count},{"query_budget",budget.snapshot()}};result["metrics"]["planning_metadata_io"]=planning_metadata_io;if(statement.explain){result["columns"]={{{"name","QUERY PLAN"},{"type","VARCHAR"}}};result["rows"]=json::array({json::array({plan.dump(2)})});}return result;}
    }
    std::size_t examined = 0, filtered_count = 0, projected_count = 0, join_pairs = 0;
    auto project = [&](const json& row, const std::vector<json>* group) {
        json output = json::array();
        for (const auto& item : statement.items) output.push_back(resolve_blob(eval(item.value, row, group)));
        return output;
    };
    auto join_result_matches=[&](const json& row){return has_outer||matches(statement.on,row);};
    auto local_matches = [&](std::size_t source, const json& row) {
        json combined = json::array();
        for (std::size_t i = 0; i < schema.size(); ++i) combined.push_back(nullptr);
        for (std::size_t i = 0; i < row.size(); ++i) combined[offsets[source] + i] = row[i];
        for (const auto& predicate : pushed[source]) if (!matches(predicate, combined)) return false;
        return true;
    };
    auto scan_source = [&](std::size_t i, const std::function<bool(const json&)>& receive) {
        const auto scan_started = Clock::now();
        const auto io_before = storage.transaction_stats(session.tx);
        std::size_t rows_examined = 0, rows_returned = 0;
        if (!scans[i].empty) {
            auto cursor = shared_scan_matches(session.tx,statement.sources[i].table)?std::unique_ptr<RowCursor>(new SharedSpoolCursor(*shared_scan_state,scans[i].spec)):storage.scan(session.tx, statement.sources[i].table, scans[i].spec);
            StoredRow row;
            while (cursor->next(row)) {
                query_work();
                ++rows_examined; ++examined;
                if (!local_matches(i, row.values)) continue;
                ++rows_returned;
                if (!receive(row.values)) break;
            }
        }
        auto& actual = plan["sources"][i];
        actual["actual_rows"] = rows_returned;
        actual["rows_examined"] = rows_examined;
        actual["loops"] = 1;
        actual["actual_ms"] = std::chrono::duration<double, std::milli>(Clock::now() - scan_started).count();
        const auto io_after = storage.transaction_stats(session.tx);
        for (const char* counter : {"heap_page_reads", "index_page_reads", "row_fetches", "cover_hits", "bitmap_words"})
            actual[counter] = io_after.value(counter, std::uint64_t(0)) - io_before.value(counter, std::uint64_t(0));
    };

    auto workspace=storage.query_workspace();QuerySpillStore spill(workspace.first,workspace.second);
    query::ExecContext context;context.spill=&spill;context.memory_limit=operator_memory_limit();context.work=[](){query_work();};context.materialize=[](const json& r){query_materialize(r);};context.row_bytes=[](const json& r){return std::size_t(json_allocation_estimate(r)+32);};
    auto make_source=[&](std::size_t i,ScanSpec spec){
        return query::scan([&,i,spec](){
            auto& actual=plan["sources"][i];
            if(scans[i].empty){actual["actual_rows"]=0;actual["rows_examined"]=0;actual["loops"]=0;return query::Pull([](json&){return false;});}
            actual["loops"]=actual.value("loops",std::uint64_t(0))+1;
            auto open_before=storage.transaction_stats(session.tx);
            auto cursor=shared_scan_matches(session.tx,statement.sources[i].table)?std::shared_ptr<RowCursor>(new SharedSpoolCursor(*shared_scan_state,spec)):std::shared_ptr<RowCursor>(storage.scan(session.tx,statement.sources[i].table,spec));
            auto open_after=storage.transaction_stats(session.tx);
            for(const char* counter:{"heap_page_reads","index_page_reads","row_fetches","cover_hits","bitmap_words"}){auto delta=open_after.value(counter,std::uint64_t(0))-open_before.value(counter,std::uint64_t(0));actual[counter]=actual.value(counter,std::uint64_t(0))+delta;actual[std::string("open_")+counter]=actual.value(std::string("open_")+counter,std::uint64_t(0))+delta;}
            return query::Pull([&,i,cursor](json& output){StoredRow row;for(;;){query_work();auto before=storage.transaction_stats(session.tx);bool got=cursor->next(row);auto after=storage.transaction_stats(session.tx);auto& a=plan["sources"][i];for(const char* counter:{"heap_page_reads","index_page_reads","row_fetches","cover_hits","bitmap_words"})a[counter]=a.value(counter,std::uint64_t(0))+after.value(counter,std::uint64_t(0))-before.value(counter,std::uint64_t(0));if(!got)return false;++examined;a["rows_examined"]=a.value("rows_examined",std::uint64_t(0))+1;if(!local_matches(i,row.values))continue;output=json::array();for(std::size_t k=0;k<schema.size();++k)output.push_back(nullptr);for(std::size_t k=0;k<row.values.size();++k)output[offsets[i]+k]=row.values[k];a["actual_rows"]=a.value("actual_rows",std::uint64_t(0))+1;return true;}});
        },"Scan("+statement.sources[i].alias+")");
    };
    std::function<std::unique_ptr<query::ExecNode>(const std::shared_ptr<OptimizedPlan>&)> build;
    build=[&](const std::shared_ptr<OptimizedPlan>& node)->std::unique_ptr<query::ExecNode>{
        if(node->source>=0)return make_source(std::size_t(node->source),scans[node->source].spec);
        query::JoinSpec spec;auto keys=node->keys;
        spec.left_key=[keys](const json& row){json key=json::array();for(auto k:keys)key.push_back(row[k.first]);return key;};
        spec.right_key=[keys](const json& row){json key=json::array();for(auto k:keys)key.push_back(row[k.second]);return key;};
        spec.compare=[](const json&a,const json&b){return access::compare_keys(a,b);};
        std::vector<E> filters;for(const auto& e:predicates){std::set<int> refs;references(e,refs);std::uint64_t mask=0;for(int slot:refs){int rel=relation_for_slot(slot,offsets,tables);if(rel>=0)mask|=1ull<<rel;}if((mask&node->left->relations)&&(mask&node->right->relations)&&!(mask&~node->relations))filters.push_back(e);}
        if(node->join_condition)filters={node->join_condition};
        spec.predicate=[filters](const json& row){for(const auto& e:filters)if(!matches(e,row))return false;return true;};
        for(std::size_t i=0;i<tables.size();++i)if(node->right->relations&(1ull<<i))for(std::size_t col=0;col<tables[i]["columns"].size();++col)spec.right_slots.push_back(offsets[i]+int(col));
        if(node->algorithm=="INDEX_NESTED_LOOP"){
            int i=node->right->source,outer_slot=-1;for(const auto& ix:tables[i]["indexes"])if(ix["name"]==node->probe_index){int inner=offsets[i]+column_id(tables[i],index_columns(ix)[0]);for(auto k:keys)if(k.second==inner){outer_slot=k.first;break;}}
            if(outer_slot<0)fail("INTERNAL_ERROR","Parameterized join key disappeared");
            spec.index_probe=[&,i,outer_slot,index=node->probe_index](const json& row){auto scan=scans[i].spec;scan.index=index;scan.indexes.clear();scan.mode="INDEX";scan.lower.reset();scan.upper.reset();scan.lower_key=json::array({row[outer_slot]});scan.upper_key=scan.lower_key;scan.lower_inclusive=scan.upper_inclusive=true;return make_source(std::size_t(i),scan);};
        }
        if(node->algorithm.rfind("OUTER_",0)==0){
            std::vector<int> left_slots;for(std::size_t i=0;i<tables.size();++i)if(node->left->relations&(1ull<<i))for(std::size_t col=0;col<tables[i]["columns"].size();++col)left_slots.push_back(offsets[i]+int(col));
            return query::outer_join(node->algorithm.substr(6),build(node->left),build(node->right),std::move(spec),std::move(left_slots));
        }
        return query::join(node->algorithm,build(node->left),build(node->right),std::move(spec));
    };

    auto base_execution=[&]()->std::unique_ptr<query::ExecNode>{
        if(tables.empty())return query::scan([](){auto done=std::make_shared<bool>(false);return query::Pull([done](json& row){if(*done)return false;*done=true;row=json::array();return true;});},"SingleConstantRow");
        if(tables.size()==1)return make_source(0,scans[0].spec);
        if(self_join.chosen){query::AliasCopySpec spec;spec.from=offsets[self_join.source];spec.to=offsets[1-self_join.source];spec.width=tables[0]["columns"].size();spec.total_width=schema.size();spec.required_slots=self_join.required_slots;return query::alias_copy(make_source(self_join.source,scans[self_join.source].spec),std::move(spec));}
        return build(optimized.plan);
    };
    auto decorate_execution=[&](std::unique_ptr<query::ExecNode> origin){
        for(std::size_t index=0;index<decorrelations.size();++index){const auto rule=decorrelations[index];
            auto inner=query::scan([&,rule,index](){
                auto& actual=plan["decorrelation_actual"][index];actual["inner_loops"]=actual.value("inner_loops",std::uint64_t(0))+1;
                const auto before=storage.transaction_stats(session.tx);auto cursor=std::shared_ptr<RowCursor>(storage.scan(session.tx,rule.source.table,rule.scan.spec));const auto after=storage.transaction_stats(session.tx);
                for(const char* counter:{"heap_page_reads","index_page_reads"})actual[counter]=actual.value(counter,std::uint64_t(0))+after.value(counter,std::uint64_t(0))-before.value(counter,std::uint64_t(0));
                return query::Pull([&,rule,index,cursor](json& result){StoredRow row;for(;;){query_work();const auto before=storage.transaction_stats(session.tx);const bool present=cursor->next(row);const auto after=storage.transaction_stats(session.tx);auto& actual=plan["decorrelation_actual"][index];for(const char* counter:{"heap_page_reads","index_page_reads"})actual[counter]=actual.value(counter,std::uint64_t(0))+after.value(counter,std::uint64_t(0))-before.value(counter,std::uint64_t(0));if(!present)return false;actual["rows_examined"]=actual.value("rows_examined",std::uint64_t(0))+1;if(!matches(rule.residual,row.values))continue;result=json::array({eval(rule.inner_key,row.values)});actual["actual_rows"]=actual.value("actual_rows",std::uint64_t(0))+1;return true;}});
            },"DecorrelatedInner("+rule.source.alias+")");
            query::SemiJoinSpec specification;specification.outer_key=[slot=rule.outer_slot](const json& row){return json::array({row.at(slot)});};specification.inner_key=[](const json& row){return row;};
            if(index==0)specification.outer_filter=[&](const json& row){return matches(execution_where,row);};
            origin=query::semi_join(std::move(origin),std::move(inner),std::move(specification),rule.anti);
        }
        return origin;
    };

    std::function<std::uint64_t(const json&)> pair_count=[&](const json& n){
        std::uint64_t total=n.value("pairs_examined",std::uint64_t(0));
        for(const auto& child:n.value("children",json::array()))if(child.is_object())total+=pair_count(child);
        return total;
    };
    auto run_physical=[&](const std::function<bool(const json&)>& receive){
        auto execution=decorate_execution(base_execution());execution->open(context);json row;
        while(execution->next(row))if(matches(execution_where,row)&&join_result_matches(row)&&!receive(row))break;
        execution->close();plan["physical_actual"]=execution->diagnostics();
        join_pairs=pair_count(plan["physical_actual"]);plan["temporary_io"]=spill.diagnostics();
    };
    auto compare_order_keys=[&](const json& a,const json& b){
        for(std::size_t i=0;i<statement.order.size();++i){
            int comparison=a[i].is_null()?(b[i].is_null()?0:1):b[i].is_null()?-1:compare_values(a[i],b[i]);
            if(comparison)return statement.order[i].desc?-comparison:comparison;
        }
        return 0;
    };
    auto emit_sorted=[&](std::unique_ptr<query::ExecNode> records){
        auto keys=[](const json& record){return record[1];};
        auto sorted=sort_choice.bounded?query::topk(std::move(records),keys,compare_order_keys,sort_choice.k):query::sort(std::move(records),keys,compare_order_keys);
        sorted->open(context);json record;std::size_t seen=0;
        while(sorted->next(record)){
            if(seen++<statement.offset)continue;
            if(projected_count>=statement.limit)break;
            query_output(record[0]);result["rows"].push_back(std::move(record[0]));++projected_count;
            if(projected_count>=statement.limit)break;
        }
        sorted->close();plan["final_sort_actual"]=sorted->diagnostics();plan["temporary_io"]=spill.diagnostics();
    };

    if(statement.limit==0){
        for(std::size_t i=0;i<tables.size();++i){auto& source=plan["sources"][i];source["actual_rows"]=0;source["rows_examined"]=0;source["loops"]=0;}
        if(self_join.chosen){plan["physical_actual"]={{"node","SelfJoinAliasCopy"},{"opened",false},{"loops",0},{"actual_rows",0},{"reason","LIMIT0 does not open the single source"}};}
        else if(optimized.plan){
            static const std::map<std::string,std::string> names={{"NESTED_LOOP","NestedLoop"},{"BLOCK_NESTED_LOOP","BlockNestedLoop"},{"INDEX_NESTED_LOOP","IndexNestedLoop"},{"MERGE","MergeJoin"},{"HASH","HashJoin"},{"OUTER_LEFT","SpoolBlockLEFTOuterJoin"},{"OUTER_RIGHT","SpoolBlockRIGHTOuterJoin"},{"OUTER_FULL","SpoolBlockFULLOuterJoin"}};
            plan["physical_actual"]={{"node",names.at(optimized.plan->algorithm)},{"opened",false},{"loops",0},{"actual_rows",0},{"reason","LIMIT0 validated and planned without opening child operators"}};
        }
    } else if (streaming) {
        auto receive = [&](const json& row) {
            if (!matches(execution_where,row)||!join_result_matches(row)) return true;
            ++filtered_count;
            if (filtered_count <= statement.offset) return true;
            if (projected_count >= statement.limit) return false;
            auto output=project(row, nullptr);query_output(output);
            result["rows"].push_back(std::move(output)); ++projected_count;
            return projected_count < statement.limit;
        };
        if(tables.empty())receive(json::array());
        else if(tables.size()==1&&decorrelations.empty())scan_source(0,receive);
        else run_physical(receive);
    } else if(!grouped&&!statement.distinct&&!statement.order.empty()){
        std::shared_ptr<query::ExecNode> origin=decorate_execution(base_execution());
        auto records=query::scan([&,origin](){
            origin->open(context);
            return query::Pull([&,origin](json& record){
                json row;while(origin->next(row)){
                    if(!matches(execution_where,row)||!join_result_matches(row))continue;
                    ++filtered_count;auto values=project(row,nullptr);json keys=json::array();
                    for(const auto& order:statement.order)keys.push_back(eval(order.value,row));
                    record=json::array({std::move(values),std::move(keys)});return true;
                }
                origin->close();return false;
            });
        },"FinalProjectionAndOrderKeys");
        emit_sorted(std::move(records));origin->close();
        if(tables.size()>1||!decorrelations.empty()){plan["physical_actual"]=origin->diagnostics();join_pairs=pair_count(plan["physical_actual"]);}
    } else {
        std::vector<json> input;
        if (tables.empty()) {
            if (matches(execution_where, json::array())) {query_materialize(json::array());input.push_back(json::array());}
        } else if (tables.size() == 1&&decorrelations.empty()) {
            scan_source(0, [&](const json& row) { if (matches(execution_where, row)) {query_materialize(row);input.push_back(row);} return true; });
        } else {
            run_physical([&](const json& row){query_materialize(row);input.push_back(row);return true;});
        }
        filtered_count = input.size();
        struct Output { json values; std::vector<json> keys; };
        std::vector<Output> output;
        auto append = [&](const json& representative, const std::vector<json>* group) {
            if (statement.having) {
                auto truth = eval(statement.having, representative, group);
                if (truth.is_null() || !truth.get<bool>()) return;
            }
            Output row; row.values = project(representative, group);
            for (const auto& sort : statement.order) row.keys.push_back(eval(sort.value, representative, group));
            query_materialize(row.values);
            if(!row.keys.empty())query_materialize(json(row.keys));
            output.push_back(std::move(row));
        };
        if (grouped) {
            std::map<std::string, std::vector<json>> groups;
            if (statement.group.empty()) groups[""] = std::move(input);
            else for (const auto& row : input) {
                json key = json::array();
                for (const auto& expression : statement.group) key.push_back(eval(expression, row));
                query_materialize(row);
                groups[equality_key(key)].push_back(row);
            }
            for (const auto& group : groups)
                append(group.second.empty() ? json::array() : group.second[0], &group.second);
            plan["aggregate_actual_groups"] = groups.size();
        } else for (const auto& row : input) append(row, nullptr);
        if (statement.distinct) {
            std::set<std::string> seen;
            output.erase(std::remove_if(output.begin(), output.end(), [&](const Output& row) {
                return !seen.insert(equality_key(row.values)).second;
            }), output.end());
        }
        if(!statement.order.empty()){
            auto records=query::scan([&output](){
                auto position=std::make_shared<std::size_t>(0);
                return query::Pull([&output,position](json& record){
                    if(*position>=output.size())return false;
                    auto& row=output[(*position)++];record=json::array({std::move(row.values),json(std::move(row.keys))});return true;
                });
            },"MaterializedGroupOrDistinctOutput");
            emit_sorted(std::move(records));
        }else for(std::size_t i=statement.offset;i<output.size()&&i-statement.offset<statement.limit;++i){
            query_output(output[i].values);result["rows"].push_back(output[i].values);
        }
        projected_count = result["rows"].size();
    }
    plan["actual"] = {{"actual_rows", projected_count}, {"loops", statement.limit==0?0:1}, {"rows_examined", examined},
                       {"rows_after_filter", filtered_count}, {"join_pairs", join_pairs},
                       {"execution_ms", std::chrono::duration<double, std::milli>(Clock::now() - execution_started).count()},
                       {"early_stop", streaming && statement.limit != std::numeric_limits<std::size_t>::max() &&
                                      projected_count == statement.limit}};
    if(has_subquery){plan["subquery_actual"]=subquery_actual;plan["subquery_evaluations"]=subquery_evaluations;plan["subquery_total_rows_examined"]=subquery_total_rows_examined;plan["subquery_sample_limit"]=32;}
    result["plan"] = plan;
    std::uint64_t decorrelated_inner_rows=0;for(const auto& inner:plan["decorrelation_actual"])decorrelated_inner_rows+=inner.value("rows_examined",std::uint64_t(0));
    result["metrics"]["decorrelated_inner_rows_scanned"]=decorrelated_inner_rows;
    result["metrics"]["rows_scanned_scope"]="outer physical sources; decorrelated inner scans reported separately";
    result["metrics"]["rows_scanned"] = examined;
    result["metrics"]["rows_after_filter"] = filtered_count;
    result["metrics"]["actual_rows"] = projected_count;
    result["metrics"]["loops"] = 1;
    result["metrics"]["streaming"] = streaming;
    budget.check_time();result["metrics"]["query_budget"] = budget.snapshot();
    if (statement.explain) {
        result["columns"] = {{{"name", "QUERY PLAN"}, {"type", "VARCHAR"}}};
        result["rows"] = json::array({json::array({plan.dump(2)})});
    }
    return result;
}
