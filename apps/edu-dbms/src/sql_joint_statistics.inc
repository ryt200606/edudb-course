// The sample belongs to ANALYZE. Sort row offsets so long strings are copied
// only for retained value pairs. Ranking has a deterministic tie break.
void collect_joint_statistics(const json& table,const std::vector<json>& sample,std::size_t target,
                              std::uint64_t rows,json& statistics,std::size_t memory){
    if(!table.contains("statistics_definitions"))return;
    statistics["joint_mcv"]=json::object();
    std::size_t retained=0;
    for(const auto& definition:table.at("statistics_definitions").items()){
        const auto& names=definition.value();
        const int first=column_id(table,names[0]),second=column_id(table,names[1]);
        std::vector<std::size_t> order(sample.size());
        for(std::size_t i=0;i<order.size();++i)order[i]=i;
        auto cmp=[&](std::size_t a,std::size_t b){
            int diff=range_compare(sample[a][first],sample[b][first]);
            return diff?diff:range_compare(sample[a][second],sample[b][second]);
        };
        std::sort(order.begin(),order.end(),[&](auto a,auto b){return cmp(a,b)<0;});
        struct Group{std::size_t row,count;};
        std::vector<Group> groups;
        for(auto index:order){
            query_work();
            if(groups.empty()||cmp(groups.back().row,index)!=0)groups.push_back({index,1});
            else ++groups.back().count;
        }
        std::stable_sort(groups.begin(),groups.end(),[](const Group& a,const Group& b){return a.count>b.count;});
        const auto count=std::min(target,groups.size());
        json entries=json::array();
        for(std::size_t i=0;i<count;++i){
            const auto& row=sample[groups[i].row];
            json entry={{"values",json::array({row[first],row[second]})},{"count",groups[i].count}};
            retained+=json_allocation_estimate(entry)+32;
            if(retained+sample.size()*(sizeof(Group)*2+sizeof(std::size_t))>memory/2)
                fail("STATISTICS_MEMORY","Joint statistics exceed their budget; reduce target or sample_rows");
            entries.push_back(std::move(entry));
        }
        statistics["joint_mcv"][definition.key()]={{"columns",names},{"column_ids",{first,second}},
            {"sample_rows",sample.size()},{"sample_distinct",groups.size()},{"complete",sample.size()==rows&&groups.size()==count},
            {"mcv",std::move(entries)}};
    }
}

EstimatedTruth combine_estimates(EstimatedTruth a,EstimatedTruth b,bool conjunction){
    double f=clamp_probability(1-a.t-a.u),g=clamp_probability(1-b.t-b.u);
    double t=conjunction?a.t*b.t:1-(1-a.t)*(1-b.t);
    double z=conjunction?1-(1-f)*(1-g):f*g;
    return {clamp_probability(t),clamp_probability(1-t-z)};
}
EstimatedTruth observed_truth(const json& p,const json& v){
    const auto& op=p.at("op");
    if(op=="IS NULL")return {v.is_null()?1.0:0.0,0};
    if(op=="IS NOT NULL")return {v.is_null()?0.0:1.0,0};
    const auto& literal=p.at("value");
    if(v.is_null()||literal.is_null())return {0,1};
    const int diff=range_compare(v,literal);
    const bool match=op=="="?diff==0:op=="!="?diff!=0:op=="<"?diff<0:op=="<="?diff<=0:op==">"?diff>0:diff>=0;
    return {match?1.0:0.0,0};
}
EstimatedTruth joint_truth(const json& entry,const json& a,const json& b,
                           EstimatedTruth marginal_a,EstimatedTruth marginal_b,bool sampled){
    const double n=entry.at("sample_rows");
    if(n==0)return {0,0};
    // Keep one observation of prior weight when sampling: an unobserved pair
    // in a reservoir is not evidence that it is absent from the whole table.
    const double denom=sampled?n+1:n;
    double known=0;
    EstimatedTruth total{0,0},covered_a{0,0},covered_b{0,0};
    auto accumulate=[](EstimatedTruth& sum,EstimatedTruth value,double w){sum.t+=w*value.t;sum.u+=w*value.u;};
    bool retained_exact_pair=false;
    const bool point=(a.at("op")=="="||a.at("op")=="IS NULL")&&(b.at("op")=="="||b.at("op")=="IS NULL");
    for(const auto& item:entry.at("mcv")){
        const double w=item.at("count").get<double>()/denom;
        auto x=observed_truth(a,item.at("values")[0]),y=observed_truth(b,item.at("values")[1]);
        accumulate(covered_a,x,w);accumulate(covered_b,y,w);
        auto pair=combine_estimates(x,y,true);
        if(point&&pair.t==1)retained_exact_pair=true;
        accumulate(total,pair,w);known+=w;
    }
    const double rest=clamp_probability(1-known);
    if(rest>1e-12){
        auto residual=[&](EstimatedTruth marginal,EstimatedTruth covered){
            double t=clamp_probability((marginal.t-covered.t)/rest);
            return EstimatedTruth{t,std::min(1-t,clamp_probability((marginal.u-covered.u)/rest))};
        };
        auto other=combine_estimates(residual(marginal_a,covered_a),residual(marginal_b,covered_b),true);
        // Full-scan counts for a retained point are exact even if other pairs
        // were omitted because of the target limit.
        if(retained_exact_pair&&!sampled)other.t=0;
        accumulate(total,other,rest);
    }
    total.t=clamp_probability(total.t);total.u=std::min(1-total.t,clamp_probability(total.u));
    return total;
}
