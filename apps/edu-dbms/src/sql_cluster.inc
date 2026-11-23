// Engine cluster commands. Cluster S/X are real independent resources, taken
// before the existing immutable database-data -> table intention-lock path.
std::string acquire_named_cluster(Session& session,const std::string& name,bool write){
    auto catalog=storage.clusters(session.tx);if(!catalog.contains(name))fail("CLUSTER_NOT_FOUND","Cluster not found: "+name);auto resource="cluster:"+std::to_string(catalog.at(name).get<std::uint64_t>());acquire(session,resource,write);return resource;
}
std::optional<json> run_cluster(Session& session,Statement& st){
    auto result=blank_result();
    if(st.kind=="CREATE_CLUSTER"){
        auto names=storage.clusters(session.tx);if(names.contains(st.cluster_name)){if(st.if_not_exists)return result;fail("CLUSTER_EXISTS","Cluster already exists: "+st.cluster_name);}
        auto created=storage.create_cluster(session.tx,st.cluster_name);acquire(session,"cluster:"+std::to_string(created.at("oid").get<std::uint64_t>()),true);result["cluster"]=created;result["plan"]={{"node","CreateMultiTableCluster"}};return result;
    }
    if(st.kind=="DROP_CLUSTER"){
        if(!storage.clusters(session.tx).contains(st.cluster_name)){if(st.if_exists)return result;fail("CLUSTER_NOT_FOUND","Cluster not found: "+st.cluster_name);}acquire_named_cluster(session,st.cluster_name,true);storage.drop_cluster(session.tx,st.cluster_name);result["plan"]={{"node","DropEmptyCluster"},{"cluster",st.cluster_name}};return result;
    }
    if(st.kind=="SHOW_CLUSTERS"){
        result["columns"]={{{"name","cluster_name"},{"type","VARCHAR"}},{{"name","cluster_oid"},{"type","INTEGER"}}};auto names=storage.clusters(session.tx);for(auto i=names.begin();i!=names.end();++i)result["rows"].push_back(json::array({i.key(),i.value()}));result["plan"]={{"node","ClusterCatalogScan"}};return result;
    }
    if(st.kind=="INSPECT_CLUSTER"){
        acquire_named_cluster(session,st.cluster_name,false);result["inspection"]=storage.inspect_cluster(session.tx,st.cluster_name,st.limit==std::numeric_limits<std::size_t>::max()?256:st.limit);result["columns"]={{{"name","inspection"},{"type","VARCHAR"}}};result["rows"].push_back(json::array({result["inspection"].dump(2)}));result["plan"]={{"node","InspectMultiTableCluster"},{"cluster",st.cluster_name}};return result;
    }
    if(st.kind=="ALTER_TABLE"&&(st.action=="CLUSTER"||st.action=="NO_CLUSTER")){
        if(st.action=="CLUSTER"){
            if(!storage.cluster_lock_resource(session.tx,st.table).empty())fail("CLUSTER_MEMBER","Detach the table from its current cluster first");
            acquire_named_cluster(session,st.cluster_name,true);acquire(session,"table:"+st.table,true);result["cluster"]=storage.attach_cluster(session.tx,st.cluster_name,st.table,st.cluster_columns.get<std::vector<std::string>>());result["plan"]={{"node","TransactionalClusterAttach"},{"table",st.table},{"cluster",st.cluster_name}};
        }else{acquire(session,"table:"+st.table,true);result["cluster"]=storage.detach_cluster(session.tx,st.table);result["plan"]={{"node","TransactionalClusterDetach"},{"table",st.table}};}
        return result;
    }
    return {};
}
