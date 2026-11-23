// Internal typed statistics and bounded physical-plan search. Included after
// ScanChoice/TypedRange; no public header is included inside this namespace.
std::size_t operator_memory_limit(){
    auto raw=std::getenv("EDUDB_QUERY_OPERATOR_MEMORY_BYTES");if(!raw)return 2u*1024u*1024u;
    try{std::size_t at=0;auto n=std::stoull(raw,&at);if(raw[at]||n<65536||n>64u*1024u*1024u)throw std::runtime_error("range");return std::size_t(n);}catch(...){fail("INVALID_CONFIGURATION","EDUDB_QUERY_OPERATOR_MEMORY_BYTES must be65536..67108864");}
}
const json* column_statistics(const json& table,int column){
    if(!table.contains("statistics")||!table["statistics"].is_object())return nullptr;
    const auto& s=table["statistics"];if(s.value("stale",false))return nullptr;
    if(!s.contains("columns"))return nullptr;
    for(const auto& c:s["columns"])if(c.value("column_id",-1)==column)return &c;
    return nullptr;
}
double clamp_probability(double p){return std::max(0.0,std::min(1.0,p));}
struct EstimatedTruth{double t=1,u=0;};
EstimatedTruth estimate_truth(const json& table,const json& p){
    if(p.is_null())return {};
    const auto op=p.value("op",std::string());
    if(p.contains("args")){
        if(op=="NOT"){auto a=estimate_truth(table,p["args"][0]);return {clamp_probability(1-a.t-a.u),a.u};}
        EstimatedTruth out=op=="AND"?EstimatedTruth{}:EstimatedTruth{0,0};
        for(const auto& a:p["args"]){auto b=estimate_truth(table,a);double f=1-out.t-out.u,g=1-b.t-b.u;double t=op=="AND"?out.t*b.t:out.t+b.t-out.t*b.t;double z=op=="AND"?f+g-f*g:f*g;out={clamp_probability(t),clamp_probability(1-t-z)};}return out;
    }
    const auto* s=column_statistics(table,p.value("column",-1));double rows=table.value("row_count",0.0);
    const double nulls=s&&rows?clamp_probability(s->value("null_count",0.0)/rows):0.05;
    if(op=="IS NULL")return {nulls,0};
    if(op=="IS NOT NULL")return {1-nulls,0};
    const auto v=p.value("value",json());if(v.is_null())return {0,1};
    double equal=(1-nulls)/16;
    if(s){double ndv=std::max(1.0,s->value("ndv",16.0)),sum=0;bool found=false;std::size_t m=0;
        for(const auto& item:s->value("mcv",json::array())){sum+=item.value("frequency",0.0);++m;if(range_compare(item["value"],v)==0){equal=item.value("frequency",0.0);found=true;}}
        if(!found)equal=ndv>m?std::max(0.0,1-nulls-sum)/(ndv-m):0;
    }
    if(op=="=")return {clamp_probability(equal),nulls};
    if(op=="!="||op=="<>")return {clamp_probability(1-nulls-equal),nulls};
    double less=(1-nulls)*0.25;
    if(s&&s->contains("histogram")&&!(*s)["histogram"].empty()){
        less=0;json previous=s->value("min",json());
        for(const auto& bucket:(*s)["histogram"]){auto upper=bucket["upper"];double f=bucket.value("frequency",0.0);int cmp=range_compare(v,upper);
            if(cmp>0)less+=f;else if(cmp==0){less+=std::max(0.0,f-equal);break;}
            else{if(v.is_number()&&previous.is_number()&&upper.is_number()){long double lo=previous.get<long double>(),hi=upper.get<long double>(),x=v.get<long double>();if(hi>lo)less+=f*clamp_probability(double((x-lo)/(hi-lo)));}break;}previous=upper;
        }
    }
    double t=op=="<"?less:op=="<="?less+equal:op==">"?1-nulls-less-equal:1-nulls-less;
    return {clamp_probability(t),nulls};
}
double estimate_ndv(const json& table,int column,double filtered_rows){
    const auto* s=column_statistics(table,column);double n=s?s->value("ndv",16.0):std::max(1.0,table.value("row_count",0.0)/16);
    return std::max(1.0,std::min(n,std::max(1.0,filtered_rows)));
}
struct OptimizedPlan{
    std::uint64_t relations=0;bool parameterized_only=false;int source=-1;std::string algorithm="Scan",probe_index;
    std::shared_ptr<OptimizedPlan> left,right;
    E join_condition;
    std::vector<std::pair<int,int>> keys;
    std::vector<int> ordering;
    double rows=0,width=0,cost=0;json explanation;
};
struct OptimizerResult{std::shared_ptr<OptimizedPlan> plan;std::uint64_t candidates=0;bool fallback=false;std::string fallback_reason;};
int relation_for_slot(int slot,const std::vector<int>& offsets,const std::vector<json>& tables){for(std::size_t i=0;i<offsets.size();++i)if(slot>=offsets[i]&&slot<offsets[i]+int(tables[i]["columns"].size()))return int(i);return -1;}
std::vector<std::pair<int,int>> join_keys(std::uint64_t left,std::uint64_t right,const std::vector<E>& predicates,const std::vector<int>& offsets,const std::vector<json>& tables){
    std::vector<std::pair<int,int>> keys;
    for(const auto& e:predicates)if(e->kind==Expr::Binary&&e->text=="="&&e->args[0]->kind==Expr::Column&&e->args[1]->kind==Expr::Column){int a=e->args[0]->bound,b=e->args[1]->bound;int ar=relation_for_slot(a,offsets,tables),br=relation_for_slot(b,offsets,tables);if(ar<0||br<0)continue;
        if((left&(1ull<<ar))&&(right&(1ull<<br)))keys.push_back({a,b});else if((left&(1ull<<br))&&(right&(1ull<<ar)))keys.push_back({b,a});}
    std::sort(keys.begin(),keys.end());keys.erase(std::unique(keys.begin(),keys.end()),keys.end());return keys;
}
json optimizer_plan_json(const std::shared_ptr<OptimizedPlan>& p){
    json j={{"node",p->algorithm},{"relation_mask",p->relations},{"estimated_rows",p->rows},{"estimated_width",p->width},{"estimated_cost",p->cost},{"provided_order_slots",p->ordering}};
    if(p->source>=0){j["source"]=p->source;j["access"]=p->explanation;}else{j["join_keys"]=p->keys;j["probe_index"]=p->probe_index.empty()?json(nullptr):json(p->probe_index);j["children"]=json::array({optimizer_plan_json(p->left),optimizer_plan_json(p->right)});}return j;
}
OptimizerResult optimize_joins(const Statement& statement,const std::vector<json>& tables,const std::vector<int>& offsets,const std::vector<ScanChoice>& scans,const std::vector<E>& predicates){
    OptimizerResult result;const auto n=tables.size();const auto all=(1ull<<n)-1;const auto started=Clock::now();
    const std::uint64_t work_limit=200000;const double memory=double(operator_memory_limit());
    std::vector<std::shared_ptr<OptimizedPlan>> base;
    for(std::size_t i=0;i<n;++i){auto p=std::make_shared<OptimizedPlan>();p->relations=1ull<<i;p->source=int(i);p->rows=scans[i].empty?0:scans[i].estimated_rows;p->cost=scans[i].cost;const auto statistics=tables[i].value("statistics",json());double row_width=double(tables[i]["columns"].size())*16;if(statistics.is_object())row_width=statistics.value("avg_row_width",row_width);p->width=std::max(8.0,row_width);p->explanation=scans[i].description;p->parameterized_only=p->explanation.contains("parameterized_index_hint");base.push_back(p);}
    auto candidates=[&](const std::shared_ptr<OptimizedPlan>& l,const std::shared_ptr<OptimizedPlan>& r){
        std::vector<std::shared_ptr<OptimizedPlan>> out;if(l->parameterized_only)return out;auto keys=join_keys(l->relations,r->relations,predicates,offsets,tables);double joined=l->rows*r->rows;
        for(auto k:keys){int a=relation_for_slot(k.first,offsets,tables),b=relation_for_slot(k.second,offsets,tables);joined/=std::max(estimate_ndv(tables[a],k.first-offsets[a],l->rows),estimate_ndv(tables[b],k.second-offsets[b],r->rows));}
        joined=std::max(0.0,joined);
        auto add=[&](const std::string& method,double cost,const std::string& index=""){
            if(!statement.join_hint.empty()&&statement.join_hint!=method)return;
            if(r->parameterized_only&&(method!="INDEX_NESTED_LOOP"||index!=r->explanation["parameterized_index_hint"]))return;
            auto p=std::make_shared<OptimizedPlan>();p->relations=l->relations|r->relations;p->left=l;p->right=r;p->algorithm=method;p->keys=keys;p->rows=joined;p->width=l->width+r->width;p->cost=cost;p->probe_index=index;if(method=="MERGE")for(auto k:keys)p->ordering.push_back(k.first);else if(method=="NESTED_LOOP"||method=="INDEX_NESTED_LOOP")p->ordering=l->ordering;out.push_back(p);++result.candidates;
        };
        double pairs=l->rows*r->rows;add("NESTED_LOOP",l->cost+l->rows*r->cost+pairs*0.02);
        double blocks=std::max(1.0,std::ceil(l->rows*l->width/std::max(1.0,memory/2)));add("BLOCK_NESTED_LOOP",l->cost+blocks*r->cost+pairs*0.02);
        if(!keys.empty()){
            double spill=r->rows*r->width>memory/2?(l->rows*l->width+r->rows*r->width)/8192*4:0;
            add("HASH",l->cost+r->cost+(l->rows+r->rows)*0.06+joined*0.01+spill);
            auto sorting=[&](const std::shared_ptr<OptimizedPlan>& p){double rows=std::max(1.0,p->rows);double io=rows*p->width>memory?rows*p->width/8192*4:0;return rows*std::log2(rows)*0.015+io;};
            add("MERGE",l->cost+r->cost+sorting(l)+sorting(r)+(l->rows+r->rows+joined)*0.015);
            if(r->source>=0&&!statement.sources[r->source].force_scan){int source=r->source;for(const auto& ix:tables[source]["indexes"]){auto cols=index_columns(ix);if(cols.size()!=1)continue;if(!statement.sources[source].index_hint.empty()&&statement.sources[source].index_hint!=ix["name"])continue;int slot=offsets[source]+column_id(tables[source],cols[0]);bool found=false;for(auto k:keys)found=found||k.second==slot;if(!found)continue;
                    double ndv=estimate_ndv(tables[source],slot-offsets[source],r->rows),per=r->rows/ndv,height=1;for(auto& stat:tables[source].value("index_stats",json::array()))if(stat.value("name",std::string())==ix["name"])height=stat.value("height",1.0);
                    add("INDEX_NESTED_LOOP",l->cost+l->rows*(std::max(1.0,height)*0.35+per*0.53)+joined*0.01,ix["name"]);
                }}
        }
        return out;
    };
    auto greedy=[&](){std::shared_ptr<OptimizedPlan> best;for(auto start:base){auto current=start;bool feasible=true;while(current->relations!=all){std::shared_ptr<OptimizedPlan> chosen;for(auto b:base)if(!(current->relations&b->relations)){for(auto p:candidates(current,b))if(!chosen||p->cost<chosen->cost)chosen=p;for(auto p:candidates(b,current))if(!chosen||p->cost<chosen->cost)chosen=p;}if(!chosen){feasible=false;break;}current=chosen;}if(feasible&&(!best||current->cost<best->cost))best=current;}return best;};
    if(n>8){result.fallback=true;result.fallback_reason="More than8 relations; bounded greedy physical search";result.plan=greedy();}
    else{
        std::map<std::uint64_t,std::map<std::vector<int>,std::shared_ptr<OptimizedPlan>>> memo;for(auto p:base)memo[p->relations][p->ordering]=p;
        bool exhausted=false;
        for(std::uint64_t mask=1;mask<=all&&!exhausted;++mask){if((mask&(mask-1))==0)continue;
            for(std::uint64_t l=(mask-1)&mask;l&&!exhausted;l=(l-1)&mask){auto r=mask^l;if(!r||!memo.count(l)||!memo.count(r))continue;
                for(const auto& a:memo[l])for(const auto& b:memo[r])for(auto p:candidates(a.second,b.second)){
                    auto& saved=memo[mask][p->ordering];if(!saved||p->cost<saved->cost)saved=p;
                    if(result.candidates>work_limit||Clock::now()-started>std::chrono::milliseconds(1000)){exhausted=true;break;}
                }
            }
        }
        if(exhausted){result.fallback=true;result.fallback_reason="Optimization candidate/time budget reached; greedy fallback";result.plan=greedy();}
        else if(memo.count(all))for(auto& item:memo[all])if(!result.plan||item.second->cost<result.plan->cost)result.plan=item.second;
    }
    if(!result.plan)fail("INVALID_JOIN_METHOD","No feasible plan satisfies the requested join algorithm and index hints");
    return result;
}

OptimizerResult outer_join_plan(const Statement& statement,const std::vector<json>& tables,const std::vector<ScanChoice>& scans){
    OptimizerResult result;std::vector<std::shared_ptr<OptimizedPlan>> bases;
    for(std::size_t i=0;i<tables.size();++i){auto p=std::make_shared<OptimizedPlan>();p->source=int(i);p->relations=1ull<<i;p->rows=tables[i].value("row_count",0.0);p->width=std::max(8.0,double(tables[i]["columns"].size())*16);p->cost=scans[i].cost;p->explanation=scans[i].description;bases.push_back(p);}
    auto current=bases.at(0);
    for(std::size_t i=1;i<bases.size();++i){auto p=std::make_shared<OptimizedPlan>();p->left=current;p->right=bases[i];p->relations=current->relations|bases[i]->relations;p->join_condition=statement.join_conditions.at(i-1).first;auto kind=statement.join_kinds.at(i-1);p->algorithm=kind=="INNER"?"NESTED_LOOP":"OUTER_"+kind;p->width=current->width+bases[i]->width;double pairs=current->rows*bases[i]->rows;p->rows=kind=="INNER"?pairs:std::max(pairs,kind=="LEFT"?current->rows:kind=="RIGHT"?bases[i]->rows:current->rows+bases[i]->rows);p->cost=current->cost+bases[i]->cost+pairs*0.02;current=p;++result.candidates;}
    result.plan=current;return result;
}
