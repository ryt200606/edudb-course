// Compound SELECT uses real relational nodes in the same transaction. Operand
// SELECT results are bounded materializations under the parent's shared budget;
// this is deliberately not described as a fully streaming compound executor.
json select_set(Session& session,Statement& statement,QueryBudget* shared_budget,bool internal_output){
    std::unique_ptr<QueryBudget> local_budget;if(!shared_budget)local_budget=std::make_unique<QueryBudget>(query_limits);
    QueryBudget& budget=shared_budget?*shared_budget:*local_budget;
    budget.cancellation=locks.cancellation_token(session.tx->id);
    QueryBudgetScope budget_scope(budget);InternalQueryOutputScope output_scope(internal_output);
    PageProgressScope page_scope([&]{budget.work();budget.check_time();});
    std::set<std::string> tables;for(const auto& source:statement.sources)tables.insert(source.table);
    for(const auto& table:tables)acquire(session,"table:"+table,false);
    Statement left=*statement.set_left,right=*statement.set_right;
    left.explain=right.explain=true;left.analyze=right.analyze=false;
    auto left_binding=select(session,left,&budget,true),right_binding=select(session,right,&budget,true);
    const auto left_columns=left_binding["output_schema"],right_columns=right_binding["output_schema"];
    if(left_columns.size()!=right_columns.size())fail("COLUMN_COUNT_MISMATCH","Set operands require equal output column counts");
    json columns=left_columns;Schema schema;
    for(std::size_t i=0;i<columns.size();++i){
        auto a=left_columns[i]["type"].get<std::string>(),b=right_columns[i]["type"].get<std::string>();
        std::string common=a=="NULL"?b:b=="NULL"?a:a==b?a:numeric(a)&&numeric(b)?"REAL":"";
        if(common.empty())fail("TYPE_ERROR","Set operands have incompatible column types: "+a+" and "+b);
        columns[i]["type"]=common;schema.push_back({columns[i]["name"],common,""});
    }
    for(auto& order:statement.order){
        auto& e=order.value;
        if(e->kind==Expr::Literal&&e->value.is_number_integer()){
            auto ordinal=e->value.get<std::int64_t>();if(ordinal<1||std::size_t(ordinal)>columns.size())fail("COLUMN_NOT_FOUND","Set ORDER BY output position is out of range");
            e=expr(Expr::Column,columns[ordinal-1]["name"]);e->bound=int(ordinal-1);e->type=columns[ordinal-1]["type"];
        }else bind_expr(e,schema);
        if(e->type=="BLOB")fail("UNSUPPORTED_SQL","ORDER BY BLOB is not supported");
    }
    json plan={{"node","SetOperation"},{"operation",statement.set_operation},{"all",statement.set_all},{"operands",json::array({left_binding["plan"],right_binding["plan"]})},{"execution_mode","bounded materialized operands; actual external set merge"},{"memory_limit",operator_memory_limit()},{"output_schema",columns},{"numeric_common_type","INTEGER/REAL operands are converted to the declared common REAL type before set comparison"}};
    json planning=json::object();for(const char* key:{"heap_page_reads","index_page_reads"})planning[key]=left_binding.value("metrics",json::object()).value("planning_metadata_io",json::object()).value(key,std::uint64_t(0))+right_binding.value("metrics",json::object()).value("planning_metadata_io",json::object()).value(key,std::uint64_t(0));
    const auto sort_choice=statement.order.empty()?FinalSortChoice{}:final_sort_choice(statement);
    if(!statement.order.empty())plan["final_sort"]=sort_choice.description;
    plan["planning_metadata_io"]=planning;
    auto result=blank_result();result["columns"]=columns;result["output_schema"]=columns;result["metrics"]["planning_metadata_io"]=planning;
    if(statement.explain&&!statement.analyze){result["plan"]=plan;result["columns"]={{{"name","QUERY PLAN"},{"type","VARCHAR"}}};result["rows"]=json::array({json::array({plan.dump(2)})});return result;}
    if(statement.limit==0){plan["actual"]={{"loops",0},{"actual_rows",0},{"opened",false}};result["plan"]=plan;return result;}
    left.explain=right.explain=false;left.analyze=right.analyze=false;
    plan["operand_actual"]=json::array({nullptr,nullptr});
    auto input=[&](Statement operand,std::size_t side,const std::string& name){
        return query::scan([&,operand,side,columns]()mutable{
            auto actual=select(session,operand,&budget,true);plan["operand_actual"][side]=actual["plan"];
            auto child_planning=actual.value("metrics",json::object()).value("planning_metadata_io",json::object());
            for(const char* key:{"heap_page_reads","index_page_reads"})planning[key]=planning.value(key,std::uint64_t(0))+child_planning.value(key,std::uint64_t(0));
            plan["planning_metadata_io"]=planning;result["metrics"]["planning_metadata_io"]=planning;
            auto data=std::make_shared<json>(std::move(actual["rows"]));
            auto position=std::make_shared<std::size_t>(0);
            return query::Pull([data,columns,position](json& row){
                if(*position>=data->size())return false;
                row=std::move((*data)[(*position)++]);
                for(std::size_t i=0;i<columns.size();++i)if(columns[i]["type"]=="REAL"&&row[i].is_number_integer())row[i]=row[i].get<double>();
                return true;
            });
        },name);
    };
    auto compare_rows=[](const json& a,const json& b){
        if(a.size()!=b.size())fail("COLUMN_COUNT_MISMATCH","Set execution row widths differ");
        for(std::size_t i=0;i<a.size();++i){int c=a[i].is_null()?(b[i].is_null()?0:-1):b[i].is_null()?1:compare_values(a[i],b[i]);if(c)return c;}return 0;
    };
    auto workspace=storage.query_workspace();QuerySpillStore spill(workspace.first,workspace.second);
    query::ExecContext context;context.spill=&spill;context.memory_limit=operator_memory_limit();context.work=[](){query_work();};context.materialize=[](const json& r){query_materialize(r);};context.row_bytes=[](const json& r){return std::size_t(json_allocation_estimate(r)+32);};
    std::unique_ptr<query::ExecNode> execution=query::set_operation(statement.set_operation,statement.set_all,input(left,0,"LeftMaterializedOperand"),input(right,1,"RightMaterializedOperand"),compare_rows);
    if(!statement.order.empty()){
        auto keys=[&](const json& row){json k=json::array();for(const auto& sort:statement.order)k.push_back(eval(sort.value,row));return k;};
        auto compare=[&](const json& a,const json& b){for(std::size_t i=0;i<statement.order.size();++i){int c=a[i].is_null()?(b[i].is_null()?0:1):b[i].is_null()?-1:compare_values(a[i],b[i]);if(c)return statement.order[i].desc?-c:c;}return 0;};
        // Captures reference the statement for this whole private execution.
        execution=sort_choice.bounded?query::topk(std::move(execution),keys,compare,sort_choice.k):query::sort(std::move(execution),keys,compare);
    }
    execution->open(context);json row;std::size_t seen=0;
    while(execution->next(row)){if(seen++<statement.offset)continue;query_output(row);result["rows"].push_back(row);if(result["rows"].size()>=statement.limit)break;}
    execution->close();plan["physical_actual"]=execution->diagnostics();plan["actual"]={{"loops",1},{"actual_rows",result["rows"].size()}};plan["temporary_io"]=spill.diagnostics();result["plan"]=plan;result["metrics"]["query_budget"]=budget.snapshot();
    if(statement.explain){result["columns"]={{{"name","QUERY PLAN"},{"type","VARCHAR"}}};result["rows"]=json::array({json::array({plan.dump(2)})});}
    return result;
}
