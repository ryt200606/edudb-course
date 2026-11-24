// Sound first-stage decorrelation. The original logical Statement remains
// unchanged; only execution_where removes predicates implemented by real nodes.
static bool decorrelation_enabled(){
    const char* value=std::getenv("EDUDB_OPTIMIZER_DECORRELATE");if(!value||!*value)return true;const auto option=upper(value);
    if(option=="ON"||option=="1"){return true;}
    if(option=="OFF"||option=="0"){return false;}
    fail("INVALID_CONFIGURATION","EDUDB_OPTIMIZER_DECORRELATE must be ON or OFF");
}
static bool decorrelation_total(const E& expression){
    if(!expression)return true;
    if(expression->kind==Expr::Literal||expression->kind==Expr::Column)return true;
    if(expression->kind==Expr::IsNull)return decorrelation_total(expression->args[0]);
    if(expression->kind==Expr::Unary&&expression->text=="NOT")return decorrelation_total(expression->args[0]);
    if(expression->kind!=Expr::Binary)return false;
    static const std::set<std::string> safe={"AND","OR","=","!=","<>","<","<=",">",">="};if(!safe.count(expression->text))return false;
    for(const auto& arg:expression->args){if(!decorrelation_total(arg))return false;}
    return true;
}
static E decorrelation_residual(const E& expression,const std::vector<SqlDecorrelationPlan>& plans){
    if(!expression)return nullptr;
    for(const auto& plan:plans)if(expression==plan.target){auto yes=expr(Expr::Literal);yes->value=true;yes->type="BOOLEAN";return yes;}
    if(expression->kind==Expr::Binary&&expression->text=="AND"){auto copy=std::make_shared<Expr>(*expression);copy->args={decorrelation_residual(expression->args[0],plans),decorrelation_residual(expression->args[1],plans)};return copy;}
    return expression;
}
std::vector<SqlDecorrelationPlan> decorrelation_plans(Session& session,const Statement& statement,const Schema& outer_schema,double outer_rows,json& description){
    description={{"enabled",decorrelation_enabled()},{"chosen",false},{"rule","top-level AND EXISTS/NOT EXISTS, one equality correlation and total inner-local residual"},{"candidates",json::array()}};
    if(!description["enabled"].get<bool>()||!statement.where)return {};
    if(!decorrelation_total(statement.on)){description["reason"]="Runtime/partial outer ON retains original Apply";return {};}
    std::vector<E> terms;conjuncts(statement.where,terms);std::vector<SqlDecorrelationPlan> plans;
    for(const auto& term:terms){
        E expression=term;bool anti=false;
        if(expression->kind==Expr::Unary&&expression->text=="NOT"&&expression->args[0]->kind==Expr::Subquery){anti=true;expression=expression->args[0];}
        if(expression->kind!=Expr::Subquery||expression->text!="EXISTS"){
            if(!decorrelation_total(term)){description["reason"]="OR/runtime/scalar/IN or non-total outer predicate retains original Apply";return {};}
            continue;
        }
        auto inner=*expression->query;bool aggregates=false;for(const auto& item:inner.items)aggregates=aggregates||aggregate(item.value);
        if(!inner.set_operation.empty()||inner.sources.size()!=1||inner.on||!inner.group.empty()||inner.having||aggregates||inner.distinct||!inner.order.empty()||inner.offset||inner.limit!=std::numeric_limits<std::size_t>::max()||!inner.sources[0].index_hint.empty()){
            description["reason"]="Unproved inner grouping/set/distinct/order/limit/hint retains original Apply";return {};
        }
        SqlDecorrelationPlan plan;plan.target=term;plan.anti=anti;plan.source=inner.sources[0];std::vector<E> inner_terms,locals;conjuncts(inner.where,inner_terms);int correlations=0;
        for(const auto& predicate:inner_terms){
            if(predicate->kind==Expr::Binary&&predicate->text=="="){
                auto column=predicate->args[0],parameter=predicate->args[1];if(column->kind==Expr::Parameter)std::swap(column,parameter);
                if(column->kind==Expr::Column&&parameter->kind==Expr::Parameter&&parameter->outer_depth==1){++correlations;plan.inner_key=column;plan.outer_slot=parameter->bound;continue;}
            }
            if(!decorrelation_total(predicate)){description["reason"]="Additional/deep correlation or non-total inner residual retains original Apply";return {};}
            locals.push_back(predicate);
        }
        if(correlations!=1||plan.outer_slot<0||std::size_t(plan.outer_slot)>=outer_schema.size()){description["reason"]="Exactly one immediate equality correlation was not proved";return {};}
        acquire(session,"table:"+plan.source.table,false);plan.metadata=table(session,plan.source.table,false);plan.schema=schema_for(plan.metadata,plan.source.alias);
        for(const auto& predicate:locals){if(!plan.residual)plan.residual=predicate;else{auto both=expr(Expr::Binary,"AND");set_arguments(both,{plan.residual,predicate});both->type="BOOLEAN";plan.residual=both;}}
        std::set<int> required;references(plan.inner_key,required);if(plan.residual)references(plan.residual,required);std::vector<int> columns(required.begin(),required.end());
        plan.scan=choose_scan(plan.metadata,plan.source,plan.schema,0,locals,columns);plan.scan.spec.lazy_blobs=true;
        const auto inner_rows=plan.scan.estimated_rows;
        const double key_width=plan.inner_key->type=="VARCHAR"?32.0:16.0;
        // A top-level conjunct fixes this outer key to NULL. Ordinary SQL
        // equality cannot match an inner row, so the lazy semi/anti operator
        // never opens its inner input, even when the estimated outer count <1.
        const bool only_null_keys=std::any_of(terms.begin(),terms.end(),[&](const E& term){
            return term->kind==Expr::IsNull&&term->text=="IS NULL"&&
                   term->args[0]->kind==Expr::Column&&term->args[0]->bound==plan.outer_slot;
        });
        const bool estimated_spill=!only_null_keys&&inner_rows*(key_width+128)>double(operator_memory_limit())/3;
        const double spill_cost=estimated_spill?(inner_rows*(key_width+16)*4+outer_rows*(key_width+16)*2)/8192:0;
        const double apply_cost=outer_rows*plan.scan.cost;
        const double inner_cost=only_null_keys?0:plan.scan.cost+inner_rows*0.03+spill_cost;
        const double semi_cost=inner_cost+outer_rows*0.02;
        plan.explanation={{"operator",anti?"HashAntiJoin":"HashSemiJoin"},{"inner_table",plan.source.table},{"outer_slot",plan.outer_slot},{"inner_slot",plan.inner_key->bound},{"inner_access",plan.scan.description},{"outer_estimated_rows",outer_rows},{"inner_estimated_rows",inner_rows},{"outer_key_proven_null",only_null_keys},{"apply_estimated_cost",apply_cost},{"decorrelated_estimated_cost",semi_cost},{"estimated_spill",estimated_spill},{"estimated_spill_cost",spill_cost},{"cost_assumption","Apply repeats estimated inner access; early matches or temporary partition reloads may change actual work"},{"profitable",semi_cost<apply_cost}};
        description["candidates"].push_back(plan.explanation);plans.push_back(std::move(plan));
    }
    if(plans.empty())return {};
    for(const auto& plan:plans)if(!plan.explanation["profitable"].get<bool>()){description["reason"]="AND bundle keeps Apply when a candidate cost advantage is not estimated";return {};}
    description["chosen"]=true;description["decorrelated_predicates"]=plans.size();description["scope"]="Actual semi/anti nodes, one base inner scan; IN/NOT IN remain Apply";return plans;
}
