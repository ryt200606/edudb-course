json Storage::clusters(const std::shared_ptr<Transaction>& tx){if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);return impl_->cluster_catalog(tx).clusters;}
json Storage::schema_features(const std::shared_ptr<Transaction>& tx){
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);auto catalog=impl_->cluster_catalog(tx);
    if(catalog.extended&&!catalog.features_complete)throw DbError("CATALOG_UPGRADE_REQUIRED","Feature discovery requires a complete catalog directory; use catalog-X DDL upgrade");
    return catalog.features;
}
std::string Storage::cluster_lock_resource(const std::shared_ptr<Transaction>& tx,const std::string& table){if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);auto& s=*impl_;auto coordinator=s.protocols.lock();auto catalog=s.cluster_catalog(tx);
    if(!catalog.tables.contains(table))throw DbError("TABLE_NOT_FOUND","Table not found: "+table);
    if(!catalog.extended)return {};
    if(catalog.routes_complete)return catalog.routes.contains(table)?"cluster:"+std::to_string(catalog.routes.at(table).get<PageId>()):std::string();
    if(s.aries_mode)throw DbError("CATALOG_UPGRADE_REQUIRED","Legacy clustered catalog needs catalog-X DDL upgrade before ARIES DML");
    auto oid=catalog.tables.at(table).get<PageId>();json d;if(tx->state->dirty_descriptors.count(oid))d=tx->state->descriptors.at(oid);else d=parse_json(s.read_blob(tx,oid,TYPE_DESCRIPTOR));return s.clustered(d)?"cluster:"+std::to_string(s.cluster_oid(d)):std::string();}
json Storage::create_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name){impl_->require_writable();
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;s.require(tx);Impl::BackupWriteOperation operation(*impl_,tx);if(name.empty()||s.cluster_call([&]{return record::utf8_characters(name);})>256)throw DbError("CLUSTER_NAME","Cluster name must contain1..256 UTF8 characters");auto catalog=s.cluster_catalog(tx);if(catalog.clusters.contains(name))throw DbError("CLUSTER_EXISTS","Cluster already exists");auto oid=s.allocate(tx,TYPE_DESCRIPTOR);json descriptor={{"descriptor_kind","cluster"},{"cluster_oid",oid},{"cluster_name",name},{"physical_version",1},{"total_rows",0},{"members",json::object()},{"groups",json::object()}};s.save_desc(tx,oid,descriptor);catalog.extended=true;catalog.clusters[name]=oid;s.save_cluster_catalog(tx,catalog);return {{"status","PASS"},{"name",name},{"oid",oid}};
}
json Storage::attach_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name,const std::string& table,const std::vector<std::string>& columns){impl_->require_writable();
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;s.require(tx);Impl::BackupWriteOperation operation(*impl_,tx);try{auto definition=read_table(tx,table,true,false);if(definition.contains("cluster"))throw DbError("CLUSTER_MEMBER","Table already belongs to a cluster; detach it first");definition["cluster"]={{"name",name},{"columns",columns}};s.cluster_validate_table(tx,definition);write_table(tx,table,definition);auto actual=read_table(tx,table,false,false);return {{"status","PASS"},{"table",table},{"cluster",actual.at("cluster")},{"row_count",actual.at("row_count")}};}catch(...){tx->state->aborted=true;throw;}
}
json Storage::detach_cluster(const std::shared_ptr<Transaction>& tx,const std::string& table){impl_->require_writable();
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;s.require(tx);Impl::BackupWriteOperation operation(*impl_,tx);try{auto definition=read_table(tx,table,true,false);if(!definition.contains("cluster"))throw DbError("CLUSTER_MEMBER","Table is not clustered");definition.erase("cluster");write_table(tx,table,definition);return {{"status","PASS"},{"table",table},{"organization","HEAP"},{"row_count",definition.at("row_count")}};}catch(...){tx->state->aborted=true;throw;}
}
json Storage::inspect_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name,std::size_t maximum){
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;s.require(tx);auto oid=s.find_cluster(tx,name);auto& d=s.load_cluster_descriptor(tx,oid);
    cluster::Pager pager;pager.read=[&](PageId id){return s.heap_page(tx,id,oid);};pager.write=[](PageId,const Page&){throw DbError("READ_ONLY","Read-only cluster inspection");};pager.allocate=[]()->PageId{throw DbError("READ_ONLY","Read-only cluster inspection");};pager.locate=[](PageId,std::uint64_t)->std::optional<cluster::Rid>{return {};};pager.map=[](const cluster::Rid&){throw DbError("READ_ONLY","Read-only cluster inspection");};pager.unmap=[](PageId,std::uint64_t){throw DbError("READ_ONLY","Read-only cluster inspection");};
    return s.cluster_call([&]{cluster::Heap heap(std::move(pager),d,oid);auto result=heap.inspect(maximum);result["implementation_scope"]="EduDB transaction-visible shared HEAP pages";result["method"]="MULTITABLE_CLUSTER";result["name"]=name;result["physical_version"]=s.ch10_version(d,"physical_version");result["total_rows"]=d.at("total_rows");return result;});
}
void Storage::drop_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name){impl_->require_writable();
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;s.require(tx);Impl::BackupWriteOperation operation(*impl_,tx);auto oid=s.find_cluster(tx,name);auto& d=s.load_cluster_descriptor(tx,oid);if(!d.at("members").empty())throw DbError("CLUSTER_NOT_EMPTY","Detach/drop all member tables first");if(d.at("total_rows")!=0)throw DbError("CORRUPT_CLUSTER","Empty-member cluster has nonzero row count");
    std::set<PageId> pages;for(const auto& group:d.at("groups"))for(const auto& id:group.at("pages"))pages.insert(id.get<PageId>());for(auto id:pages){auto page=s.heap_page(tx,id,oid);for(std::size_t i=0;i<get16(page.data()+32);++i)if(slot_length(page,i))throw DbError("CORRUPT_CLUSTER","Empty-member cluster still has live records");s.retire(tx,id);}PageId id=oid;while(id){auto page=s.read_page(tx,id);auto next=get64(page.data()+24);s.retire(tx,id);id=next;}tx->state->descriptors.erase(oid);tx->state->dirty_descriptors.erase(oid);auto catalog=s.cluster_catalog(tx);catalog.clusters.erase(name);s.save_cluster_catalog(tx,catalog);
}
