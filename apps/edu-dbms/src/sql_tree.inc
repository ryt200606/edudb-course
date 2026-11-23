// Restricted actual logical-row TREE protocol. Included inside Engine::Impl.
std::int64_t tree_integer(const E& e){
    std::function<bool(const E&)> constant=[&](const E& n){if(!n)return false;if(n->kind==Expr::Literal)return true;if(n->kind==Expr::Unary)return n->args.size()==1&&constant(n->args[0]);return false;};
    if(!constant(e))fail("TREE_UNSUPPORTED","Tree node requires an INTEGER literal");
    auto v=eval(e,json::array());if(!v.is_number_integer()||v.is_boolean())fail("TREE_UNSUPPORTED","Tree node requires int64 INTEGER");return v.get<std::int64_t>();
}
void tree_guard(Session& s,const Statement& st){
    if(s.active_options.protocol!=TransactionProtocol::Tree)return;
    s.current_locking={{"requested","TREE"},{"effective","TREE_NODE_X"},{"reason","static logical tree; no table S/X; unwritten node may release early, written node held to END"},{"tree",s.tree.diagnostics()}};
    if(st.kind=="TREE_OPEN"||st.kind=="TREE_LOCK"||st.kind=="TREE_UNLOCK"||st.kind=="TREE_STATUS"||st.kind=="SHOW_LOCKS"||st.kind=="SHOW_TRANSACTIONS"||st.kind=="SHOW_WAL"||st.kind=="SHOW_BUFFER")return;
    if(!s.tree.topology)fail("TREE_NOT_OPEN","TREE OPEN table PARENT column is required");
    if(st.kind!="SELECT"&&st.kind!="UPDATE")fail("TREE_UNSUPPORTED","TREE allows guarded single-node SELECT/UPDATE only; topology mutations and other statements are rejected");
    const auto& topo=*s.tree.topology;std::string alias=topo.table;
    if(st.kind=="SELECT"){
        if(st.sources.size()!=1||st.sources[0].table!=topo.table||!st.set_operation.empty()||st.on||!st.group.empty()||st.having||st.query_definition)fail("TREE_UNSUPPORTED","TREE query must target its one bound table and one node");
        alias=st.sources[0].alias;
        for(const auto& name:st.read_tables)if(name!=topo.table)fail("TREE_UNSUPPORTED","Cross-tree read is rejected");
        for(const auto& item:st.items)if(runtime_expression(item.value))fail("TREE_UNSUPPORTED","Subqueries are not supported in TREE mode");
        for(const auto& item:st.order)if(runtime_expression(item.value))fail("TREE_UNSUPPORTED","Subqueries are not supported in TREE mode");
    }else{
        if(st.table!=topo.table)fail("TREE_UNSUPPORTED","UPDATE must target the bound tree table");
        for(const auto& a:st.assignments)if(a.first==topo.key||a.first==topo.parent_column||runtime_expression(a.second))fail("TREE_UNSUPPORTED","Primary key/parent changes and subqueries are forbidden in frozen tree topology");
    }
    if(!st.where||st.where->kind!=Expr::Binary||st.where->text!="=")fail("TREE_UNSUPPORTED","WHERE primary_key = integer_literal is required");
    auto a=st.where->args[0],b=st.where->args[1];if(a->kind!=Expr::Column&&b->kind==Expr::Column)std::swap(a,b);
    if(a->kind!=Expr::Column||(a->text!=topo.key&&a->text!=alias+"."+topo.key&&a->text!=topo.table+"."+topo.key))fail("TREE_UNSUPPORTED","Predicate must use the frozen INTEGER primary key");
    auto node=tree_integer(b);s.tree.access(node,st.kind=="UPDATE");s.current_locking["node"]=node;s.current_locking["tree"]=s.tree.diagnostics();
}
std::optional<json> run_tree(Session& s,const Statement& st){
    if(st.kind.rfind("TREE_",0)!=0)return {};
    if(s.active_options.protocol!=TransactionProtocol::Tree)fail("TREE_PROTOCOL_REQUIRED","Use BEGIN PROTOCOL TREE first");
    if(!s.explicit_tx)fail("TREE_EXPLICIT_TRANSACTION","TREE requires explicit BEGIN/COMMIT or ROLLBACK");
    auto result=blank_result();
    if(st.kind=="TREE_OPEN"){
        if(s.tree.topology)fail("TREE_ALREADY_OPEN","A transaction binds exactly one static tree");
        auto t=table(s,st.table,false);if(t.value("organization",json{{"method","HEAP"}}).value("method",std::string("HEAP"))!="HEAP"||t.contains("cluster"))fail("TREE_UNSUPPORTED","TREE requires an unclustered HEAP table");
        if(t.value("row_count",std::uint64_t(0))>100000)fail("TREE_TOPOLOGY_LIMIT","TREE topology exceeds100000 logical nodes");
        const auto features=storage.schema_features(s.tx);for(auto feature=features.begin();feature!=features.end();++feature){const auto& f=feature.value();if(feature.key()==st.table&&(f.contains("materialized_view")||f.contains("materialized_aux")))fail("TREE_UNSUPPORTED","Materialized relations cannot form TREE topology");if(f.contains("materialized_view"))for(const auto& dep:f["materialized_view"].value("dependencies",json::array()))if(dep==st.table)fail("TREE_UNSUPPORTED","TREE table cannot have materialized-view dependencies");}
        std::string key;int pk=-1,parent=-1;for(std::size_t i=0;i<t["columns"].size();++i){auto& c=t["columns"][i];if(c.value("primary",false)){if(pk!=-1||c["type"]!="INTEGER")fail("TREE_TOPOLOGY","Exactly one INTEGER primary-key column is required");pk=int(i);key=c["name"];}if(c["name"]==st.column){if(c["type"]!="INTEGER")fail("TREE_TOPOLOGY","Parent column must be INTEGER");parent=int(i);}}
        if(pk<0||parent<0||pk==parent)fail("TREE_TOPOLOGY","Require distinct INTEGER primary-key and parent columns");
        std::lock_guard<std::mutex> guard(tree_topology_mutex);std::size_t active_nodes=0;for(auto i=tree_topologies.begin();i!=tree_topologies.end();)if(auto alive=i->second.lock()){active_nodes+=alive->parents.size();++i;}else i=tree_topologies.erase(i);auto shared=tree_topologies[st.table].lock();
        if(shared){if(shared->key!=key||shared->parent_column!=st.column)fail("TREE_TOPOLOGY_BUSY","Active TREE epoch already uses another parent definition");}
        else{if(active_nodes+t.value("row_count",std::uint64_t(0))>200000)fail("TREE_TOPOLOGY_LIMIT","Aggregate active TREE topologies exceed200000 nodes");std::map<std::int64_t,std::optional<std::int64_t>> edges;ScanSpec scan;scan.lazy_blobs=true;auto cursor=storage.scan(s.tx,st.table,scan);StoredRow stored;while(cursor->next(stored)){const auto& row=stored.values;auto id=row[pk].get<std::int64_t>();if(edges.size()>=100000)fail("TREE_TOPOLOGY_LIMIT","TREE scan exceeds100000 nodes");if(!edges.emplace(id,row[parent].is_null()?std::optional<std::int64_t>{}:std::optional<std::int64_t>(row[parent].get<std::int64_t>())).second)fail("TREE_TOPOLOGY","Duplicate primary node");}if(edges.size()!=t.at("row_count").get<std::size_t>())fail("TREE_TOPOLOGY","Topology scan differs from visible descriptor row count");shared=std::make_shared<LockTreeTopology>(st.table,key,st.column,std::move(edges));tree_topologies[st.table]=shared;}
        s.tree.topology=shared;
    }else if(st.kind=="TREE_LOCK"){
        auto node=tree_integer(st.where);s.tree.before_lock(node);locks.acquire(s.tx->id,s.tree.topology->resource(node),true);s.tree.granted(node);
        if(!s.current_accesses.empty())storage.refresh_read_view(s.tx,s.current_accesses);
    }else if(st.kind=="TREE_UNLOCK"){
        auto node=tree_integer(st.where);s.tree.before_unlock(node);locks.release_one_tree_node(s.tx->id,s.tree.topology->resource(node));s.tree.unlocked(node);
    }
    result["tree"]=s.tree.diagnostics();result["plan"]={{"node",st.kind},{"tree",result["tree"]}};s.current_locking["tree"]=result["tree"];return result;
}
