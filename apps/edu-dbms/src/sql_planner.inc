// Internal planner helpers. Included in sql.cpp's anonymous namespace.
// Costs are teaching estimates in normalized page/tuple units, not timings.
struct ScanChoice {
    ScanSpec spec;
    bool empty = false;
    double estimated_rows = 0;
    double cost = 0;
    json description;
};

struct IntegerRange {
    std::optional<std::int64_t> lower, upper;
    bool lower_inclusive = true, upper_inclusive = true;
    bool contradictory = false;

    void constrain(const std::string& op, std::int64_t key) {
        if (op == "=" || op == ">" || op == ">=") {
            bool inclusive = op != ">";
            if (!lower || key > *lower) { lower = key; lower_inclusive = inclusive; }
            else if (key == *lower) lower_inclusive = lower_inclusive && inclusive;
        }
        if (op == "=" || op == "<" || op == "<=") {
            bool inclusive = op != "<";
            if (!upper || key < *upper) { upper = key; upper_inclusive = inclusive; }
            else if (key == *upper) upper_inclusive = upper_inclusive && inclusive;
        }
        contradictory = lower && upper &&
            (*lower > *upper || (*lower == *upper && (!lower_inclusive || !upper_inclusive)));
    }
    bool equality() const {
        return !contradictory && lower && upper && *lower == *upper &&
               lower_inclusive && upper_inclusive;
    }
};

bool constant_integer(const E& expression, std::int64_t& result) {
    std::set<int> refs;
    references(expression, refs);
    if (!refs.empty() || aggregate(expression)||runtime_expression(expression)) return false;
    auto value = eval(expression, json::array());
    if (!value.is_number_integer() || value.is_boolean()) return false;
    result = value.get<std::int64_t>();
    return true;
}

[[maybe_unused]] std::map<std::string, IntegerRange> integer_ranges(const Schema& schema, int offset,
                                                  int width, const std::vector<E>& predicates) {
    std::map<std::string, IntegerRange> result;
    for (const auto& predicate : predicates) {
        if (predicate->kind != Expr::Binary) continue;
        auto op = predicate->text;
        if (op != "=" && op != "<" && op != "<=" && op != ">" && op != ">=") continue;
        auto column = predicate->args[0], value = predicate->args[1];
        if (column->kind != Expr::Column && value->kind == Expr::Column) {
            std::swap(column, value);
            if (op == "<") op = ">";
            else if (op == ">") op = "<";
            else if (op == "<=") op = ">=";
            else if (op == ">=") op = "<=";
        }
        if (column->kind != Expr::Column || column->bound < offset ||
            column->bound >= offset + width || column->type != "INTEGER") continue;
        std::int64_t key;
        if (constant_integer(value, key)) result[schema[column->bound].name].constrain(op, key);
    }
    return result;
}

// A restricted, typed predicate protocol. Unsupported expressions remain SQL
// residuals; an access method is never allowed to reinterpret an expression.
json access_predicate(const E& e,int offset,int width){
    if(!e)return nullptr;
    if(e->kind==Expr::Binary&&(e->text=="AND"||e->text=="OR")){
        auto a=access_predicate(e->args[0],offset,width),b=access_predicate(e->args[1],offset,width);
        if(a.is_null()||b.is_null())return nullptr;
        return {{"op",e->text},{"args",json::array({a,b})}};
    }
    if(e->kind==Expr::Unary&&e->text=="NOT"){
        auto a=access_predicate(e->args[0],offset,width);if(a.is_null())return nullptr;
        return {{"op","NOT"},{"args",json::array({a})}};
    }
    if(e->kind==Expr::IsNull&&e->args[0]->kind==Expr::Column){auto c=e->args[0];if(c->bound<offset||c->bound>=offset+width)return nullptr;return {{"op",e->text},{"column",c->bound-offset},{"value",nullptr}};}
    if(e->kind!=Expr::Binary)return nullptr;
    auto op=e->text;if(op!="="&&op!="!="&&op!="<>"&&op!="<"&&op!="<="&&op!=">"&&op!=">=")return nullptr;
    auto c=e->args[0],v=e->args[1];if(c->kind!=Expr::Column&&v->kind==Expr::Column){std::swap(c,v);if(op=="<")op=">";else if(op==">")op="<";else if(op=="<=")op=">=";else if(op==">=")op="<=";}
    if(c->kind!=Expr::Column||c->bound<offset||c->bound>=offset+width||c->type=="BLOB")return nullptr;
    std::set<int> refs;references(v,refs);if(!refs.empty()||aggregate(v)||runtime_expression(v))return nullptr;
    return {{"op",op=="<>"?"!=":op},{"column",c->bound-offset},{"value",eval(v,json::array())}};
}
int range_compare(const json& a,const json& b){if(a.is_null()||b.is_null())return a.is_null()?(b.is_null()?0:-1):1;return compare_values(a,b);}
struct TypedRange{
    json lo=nullptr,hi=nullptr;bool li=true,ui=true,has_lo=false,has_hi=false,bad=false;
    void add(std::string op,const json& v){
        if(op=="IS NULL")op="=";else if(op=="IS NOT NULL")op=">";else if(v.is_null())return;
        bool lower=op=="="||op==">"||op==">=",upper=op=="="||op=="<"||op=="<=";
        if(lower){bool incl=op!=">";if(!has_lo||range_compare(v,lo)>0){lo=v;li=incl;has_lo=true;}else if(range_compare(v,lo)==0)li=li&&incl;}
        if(upper){bool incl=op!="<";if(!has_hi||range_compare(v,hi)<0){hi=v;ui=incl;has_hi=true;}else if(range_compare(v,hi)==0)ui=ui&&incl;}
        bad=has_lo&&has_hi&&(range_compare(lo,hi)>0||(range_compare(lo,hi)==0&&(!li||!ui)));
    }
    bool equal()const{return has_lo&&has_hi&&li&&ui&&!bad&&range_compare(lo,hi)==0;}
};

#include "sql_optimizer.inc"

ScanChoice choose_scan(const json& table,const Source& source,const Schema& schema,int offset,
                       const std::vector<E>& predicates,const std::vector<int>& required={}){
    const double rows=table.value("row_count",0.0);const auto width=int(table["columns"].size());
    double pages=table.value("heap_pages",json(1)).is_array()?double(table["heap_pages"].size()):table.value("heap_pages",1.0);if(table.contains("physical_stats"))pages=table["physical_stats"].value("table_pages",pages);pages=std::max(1.0,pages);
    ScanChoice selected;selected.estimated_rows=rows;selected.cost=pages+rows*0.02;selected.spec.mode="TABLE";selected.spec.required_columns=required;selected.spec.projection_known=true;
    json candidates=json::array({{{"scan","SeqScan"},{"index",nullptr},{"estimated_rows",rows},{"estimated_cost",selected.cost},{"reason","heap_pages + row_count * 0.02"}}});
    json conjunction=nullptr;std::map<int,TypedRange> ranges;
    for(const auto& e:predicates){auto p=access_predicate(e,offset,width);if(p.is_null())continue;
        if(conjunction.is_null())conjunction=p;else conjunction={{"op","AND"},{"args",json::array({conjunction,p})}};
        if(p.contains("column"))ranges[p["column"].get<int>()].add(p["op"].get<std::string>(),p["value"]);
    }
    selected.spec.condition=conjunction;
    bool have_statistics=table.contains("statistics")&&table["statistics"].is_object()&&!table["statistics"].value("stale",false)&&table["statistics"].contains("columns");
    if(have_statistics){selected.estimated_rows=rows*estimate_truth(table,conjunction).t;candidates[0]["estimated_rows"]=selected.estimated_rows;}

    for(auto& r:ranges)if(r.second.bad){selected.empty=true;selected.estimated_rows=0;selected.cost=0;selected.description={{"table",source.table},{"alias",source.alias},{"scan","EmptyScan"},{"index",nullptr},{"estimated_rows",0},{"estimated_cost",0},{"candidates",candidates},{"selection","contradictory typed bounds"}};return selected;}
    if(source.force_scan){selected.description={{"table",source.table},{"alias",source.alias},{"scan","SeqScan"},{"index",nullptr},{"estimated_rows",rows},{"estimated_cost",selected.cost},{"candidates",candidates},{"selection","explicit USE SCAN"}};return selected;}
    bool hint_exists=source.index_hint.empty();for(auto& ix:table["indexes"])if(ix["name"]==source.index_hint)hint_exists=true;if(!hint_exists)fail("INDEX_NOT_FOUND","Hint index not found on table: "+source.index_hint);
    bool best_unique=false;std::string selected_scan="SeqScan";std::vector<std::string> independent;
    std::map<int,std::vector<std::string>> bitmap_columns;
    for(const auto& ix:table["indexes"]){auto method=ix.value("method",std::string("BPLUS"));auto cols=index_columns(ix);if(method=="BITMAP"&&cols.size()==1)bitmap_columns[column_id(table,cols[0].get<std::string>())].push_back(ix["name"].get<std::string>());
        json lo=json::array(),hi=json::array();bool li=true,ui=true,usable=false,all_equal=true;std::size_t constrained=0;
        for(auto& c:cols){auto f=ranges.find(column_id(table,c.get<std::string>()));if(f==ranges.end()){all_equal=false;break;}const auto& r=f->second;if(!r.has_lo&&!r.has_hi){all_equal=false;break;}usable=true;++constrained;if(r.equal()){lo.push_back(r.lo);hi.push_back(r.hi);}else{if(r.has_lo){lo.push_back(r.lo);li=r.li;}if(r.has_hi){hi.push_back(r.hi);ui=r.ui;}all_equal=false;break;}}
        bool hash=method.rfind("HASH_",0)==0;all_equal=all_equal&&constrained==cols.size();if(hash&&!all_equal)usable=false;if(!usable)continue;
        auto name=ix["name"].get<std::string>();bool null_key=false;for(auto& v:lo)null_key=null_key||v.is_null();bool uniq=ix.value("unique",false)&&all_equal&&!null_key;double estimate=uniq?std::min(1.0,rows):rows*(all_equal?0.0625:0.25);double height=1;
        for(const auto& stat:table.value("index_stats",json::array()))if(stat.value("name",std::string())==name){height=stat.value("height",1.0);if(cols.size()==1&&schema[offset+column_id(table,cols[0].get<std::string>())].type=="INTEGER"&&ranges.count(column_id(table,cols[0].get<std::string>()))){const auto& r=ranges.at(column_id(table,cols[0].get<std::string>()));auto minimum=stat.value("min_key",json()),maximum=stat.value("max_key",json());if(minimum.is_array()&&minimum.size()==1)minimum=minimum[0];if(maximum.is_array()&&maximum.size()==1)maximum=maximum[0];if(minimum.is_number_integer()&&maximum.is_number_integer()&&!uniq&&(!r.has_lo||r.lo.is_number_integer())&&(!r.has_hi||r.hi.is_number_integer())){long double mn=minimum.get<std::int64_t>(),mx=maximum.get<std::int64_t>();long double first=r.has_lo?r.lo.get<std::int64_t>():mn,last=r.has_hi?r.hi.get<std::int64_t>():mx;if(r.has_lo&&!r.li)++first;if(r.has_hi&&!r.ui)--last;first=std::max(first,mn);last=std::min(last,mx);estimate=last<first?0:rows*double((last-first+1)/(mx-mn+1));}}}
        if(have_statistics&&!uniq){json bounds=nullptr;for(std::size_t k=0;k<constrained;++k){int col=column_id(table,cols[k]);const auto& r=ranges.at(col);auto add=[&](const std::string& op,const json& v){json e={{"op",op},{"column",col},{"value",v}};bounds=bounds.is_null()?e:json{{"op","AND"},{"args",json::array({bounds,e})}};};if(r.equal())add(r.lo.is_null()?"IS NULL":"=",r.lo);else{if(r.has_lo)add(r.li?">=":">",r.lo);if(r.has_hi)add(r.ui?"<=":"<",r.hi);}}estimate=rows*estimate_truth(table,bounds).t;}
        estimate=std::max(0.0,estimate);if(estimate>0)estimate=std::max(1.0,estimate);
        std::set<int> covered;for(auto& n:cols)covered.insert(column_id(table,n.get<std::string>()));for(auto& n:ix.value("include",json::array()))covered.insert(column_id(table,n.get<std::string>()));bool cover=!required.empty();for(auto c:required)cover=cover&&covered.count(c);if(method=="ORDERED_SPARSE")cover=false;
        double cost=(hash?0.5:std::max(1.0,height)*0.35)+estimate*0.03+(cover?0:std::min(pages,estimate)*0.5);
        std::string scan=method=="BPLUS"?"IndexScan":method=="BTREE"?"BTreeScan":method=="HASH_STATIC"?"StaticHashLookup":method=="HASH_EXTENDIBLE"?"ExtendibleHashLookup":method=="HASH_LINEAR"?"LinearHashLookup":method=="BITMAP"?"BitmapScan":method=="ORDERED_DENSE"?"DenseIndexScan":"SparseIndexScan";
        if(cover)scan="IndexOnlyScan";
        json d={{"scan",scan},{"index",name},{"method",method},{"columns",cols},{"lower_key",lo.empty()?json(nullptr):lo},{"upper_key",hi.empty()?json(nullptr):hi},{"lower_inclusive",li},{"upper_inclusive",ui},{"estimated_rows",estimate},{"estimated_cost",cost},{"unique_equality",uniq},{"covering",cover},{"reason","typed key-prefix bounds; normalized page/tuple heuristic"}};candidates.push_back(d);independent.push_back(name);
        if((!source.index_hint.empty()&&name==source.index_hint)||(source.index_hint.empty()&&cost<selected.cost)){selected.spec.index=name;selected.spec.mode="INDEX";selected.spec.lower_key=lo.empty()?json(nullptr):lo;selected.spec.upper_key=hi.empty()?json(nullptr):hi;selected.spec.lower_inclusive=li;selected.spec.upper_inclusive=ui;selected.spec.lower.reset();selected.spec.upper.reset();if(lo.size()==1&&lo[0].is_number_integer())selected.spec.lower=lo[0].get<std::int64_t>();if(hi.size()==1&&hi[0].is_number_integer())selected.spec.upper=hi[0].get<std::int64_t>();selected.estimated_rows=have_statistics?std::min(estimate,rows*estimate_truth(table,conjunction).t):estimate;selected.cost=cost;best_unique=uniq;selected_scan=scan;}
    }
    if(!source.index_hint.empty()&&selected.spec.index!=source.index_hint)fail("INDEX_NOT_APPLICABLE","Hint requires a supported leading-key condition: "+source.index_hint);
    // A boolean bitmap plan is legal only when every local predicate is
    // represented and each leaf has its own real persistent bitmap index.
    std::vector<std::string> bitmap_indexes;
    std::function<bool(const json&)> bitmap_covered=[&](const json& p){if(p.is_null())return false;if(p.contains("args")){for(auto& a:p["args"])if(!bitmap_covered(a))return false;return true;}if(!p.contains("column"))return false;auto f=bitmap_columns.find(p["column"].get<int>());if(f==bitmap_columns.end())return false;bitmap_indexes.push_back(f->second[0]);return true;};
    bool complete=!predicates.empty();for(auto& e:predicates){std::set<int> refs;references(e,refs);bool local=!refs.empty();for(int r:refs)local=local&&r>=offset&&r<offset+width;if(local&&access_predicate(e,offset,width).is_null())complete=false;}
    if(!best_unique&&source.index_hint.empty()&&complete&&bitmap_covered(conjunction)){
        std::sort(bitmap_indexes.begin(),bitmap_indexes.end());bitmap_indexes.erase(std::unique(bitmap_indexes.begin(),bitmap_indexes.end()),bitmap_indexes.end());
        double bitmap_rows=have_statistics?rows*estimate_truth(table,conjunction).t:std::min(selected.estimated_rows,rows*0.1);double bitmap_cost=std::max(1.0,rows/64.0*0.01)+std::min(pages,bitmap_rows)*0.5;
        if(bitmap_cost<selected.cost){selected.spec.mode="BITMAP";selected.spec.index=bitmap_indexes[0];selected.spec.indexes=bitmap_indexes;selected.spec.lower_key=nullptr;selected.spec.upper_key=nullptr;selected.spec.lower.reset();selected.spec.upper.reset();selected_scan="BitmapBooleanScan";selected.estimated_rows=std::min(selected.estimated_rows,rows*0.1);selected.cost=std::max(1.0,rows/64.0*0.01)+std::min(pages,selected.estimated_rows)*0.5;candidates.push_back({{"scan",selected_scan},{"indexes",bitmap_indexes},{"estimated_rows",selected.estimated_rows},{"estimated_cost",selected.cost},{"reason","all typed boolean leaves have persistent bitmap indexes; SQL 3VL retained"}});}
    }else if(!best_unique&&source.index_hint.empty()&&independent.size()>1){
        // Intersect independently indexed conjunctions. The adapter obtains a
        // safe candidate superset per index, then SQL evaluates the residual.
        std::set<int> distinct_keys;std::vector<std::string> separate;
        for(auto& name:independent)for(auto& ix:table["indexes"])if(ix["name"]==name){int c=column_id(table,index_columns(ix)[0].get<std::string>());if(distinct_keys.insert(c).second)separate.push_back(name);}
        if(separate.size()>1){double estimate=rows,access_cost=0;for(auto& name:separate)for(auto& c:candidates)if(c.value("index",json())==name){auto e=c.value("estimated_rows",rows);estimate*=rows?std::min(1.0,e/rows):0;access_cost+=0.35+e*0.03;break;}double cost=access_cost+std::min(pages,estimate)*0.5;candidates.push_back({{"scan","IndexIntersect"},{"indexes",separate},{"estimated_rows",estimate},{"estimated_cost",cost},{"reason","distinct indexed AND columns; independent-selectivity teaching estimate"}});if(cost<selected.cost){selected.spec.mode="INTERSECT";selected.spec.indexes=separate;selected_scan="IndexIntersect";selected.spec.lower_key=nullptr;selected.spec.upper_key=nullptr;selected.spec.lower.reset();selected.spec.upper.reset();selected.estimated_rows=estimate;selected.cost=cost;}}

    }
    selected.description={{"table",source.table},{"alias",source.alias},{"scan",selected_scan},{"index",selected.spec.index.empty()?json(nullptr):json(selected.spec.index)},{"indexes",selected.spec.indexes},{"lower",selected.spec.lower?json(*selected.spec.lower):json(nullptr)},{"upper",selected.spec.upper?json(*selected.spec.upper):json(nullptr)},{"lower_key",selected.spec.lower_key},{"upper_key",selected.spec.upper_key},{"lower_inclusive",selected.spec.lower_inclusive},{"upper_inclusive",selected.spec.upper_inclusive},{"estimated_rows",selected.estimated_rows},{"estimated_cost",selected.cost},{"candidates",candidates},{"selection",!source.index_hint.empty()?"explicit USE INDEX":best_unique?"unique equality preferred":"lowest estimated cost; bitmap/intersection for eligible boolean predicates"}};
    return selected;
}

bool same_expression(const E& left, const E& right) {
    if (!left || !right) return left == right;
    if (left->kind != right->kind || left->text != right->text ||
        left->bound != right->bound || left->outer_depth!=right->outer_depth || left->query!=right->query || left->value != right->value ||
        left->args.size() != right->args.size()) return false;
    for (std::size_t i = 0; i < left->args.size(); ++i)
        if (!same_expression(left->args[i], right->args[i])) return false;
    return true;
}

// JSON distinguishes -0.0 and +0.0 when serialized, whereas SQL numeric
// equality, GROUP BY and DISTINCT treat both zeros as the same value.
std::string equality_key(json values) {
    for (auto& value : values)
        if (value.is_number() && value.get<double>() == 0.0) value = 0;
    return values.dump();
}

bool valid_group_expression(const E& expression, const std::vector<E>& keys) {
    if (!expression || expression->kind == Expr::Literal || (expression->kind == Expr::Function && aggregate_function(expression->text))) return true;
    for (const auto& key : keys) if (same_expression(expression, key)) return true;
    if (expression->kind == Expr::Column) return false;
    for (const auto& argument : expression->args)
        if (!valid_group_expression(argument, keys)) return false;
    return true;
}

json validate_row(const json& input, const json& columns) {
    if (!input.is_array() || input.size() != columns.size())
        fail("COLUMN_COUNT_MISMATCH", "Row width differs from table schema");
    json result = json::array();
    for (std::size_t i = 0; i < columns.size(); ++i) result.push_back(convert(input[i], columns[i]));
    return result;
}

std::chrono::milliseconds timeout_from_environment(const char* name, std::uint64_t fallback) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw) return std::chrono::milliseconds(fallback);
    try {
        std::string text(raw); std::size_t consumed = 0;
        auto value = std::stoull(text, &consumed);
        if (consumed != text.size() || !value || value > 3600000) throw std::invalid_argument("range");
        return std::chrono::milliseconds(value);
    } catch (...) { fail("INVALID_CONFIG", std::string(name) + " must be 1..3600000 milliseconds"); }
}

QueryLimits query_limits_from_environment() {
    QueryLimits result;
    auto read=[](const char* name,std::uint64_t fallback) {
        const char* raw=std::getenv(name);if(!raw||!*raw)return fallback;
        try {
            std::string value(raw);std::size_t consumed=0;auto limit=std::stoull(value,&consumed);
            if(consumed!=value.size()||!limit||limit>1000000000000ULL)throw std::invalid_argument("range");
            return std::uint64_t(limit);
        } catch(...){fail("INVALID_CONFIG",std::string(name)+" must be 1..1000000000000");}
    };
    result.work_units=read("EDUDB_QUERY_WORK_LIMIT",result.work_units);
    result.materialized_rows=read("EDUDB_QUERY_MATERIALIZED_ROW_LIMIT",result.materialized_rows);
    result.materialized_bytes=read("EDUDB_QUERY_MATERIALIZED_BYTE_LIMIT",result.materialized_bytes);
    result.result_rows=read("EDUDB_QUERY_RESULT_ROW_LIMIT",result.result_rows);
    result.result_bytes=read("EDUDB_QUERY_RESULT_BYTE_LIMIT",result.result_bytes);
    result.timeout_ms=static_cast<std::uint64_t>(timeout_from_environment("EDUDB_QUERY_TIMEOUT_MS",result.timeout_ms).count());
    return result;
}

struct SqlDecorrelationPlan {
    E target,inner_key,residual;bool anti=false;int outer_slot=-1;
    Source source;Schema schema;json metadata;ScanChoice scan;
    json explanation;
};

// Physical proof result for the restricted same-row self-join rule.
struct SelfJoinChoice {
    bool chosen=false;std::size_t source=0;double cost=0;ScanChoice scan;std::vector<int> required_slots;json description;
};
