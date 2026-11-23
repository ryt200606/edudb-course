// Included in Storage::Impl. All state lives in the existing transaction
// descriptor/private-page maps, so page history and savepoints select one CSN.
struct ClusterCatalog {json tables=json::object(),clusters=json::object();bool extended=false;json routes=json::object();bool routes_complete=false;json features=json::object();bool features_complete=false;};
ClusterCatalog cluster_catalog(const std::shared_ptr<Transaction>& tx){
    auto raw=parse_json(read_blob(tx,1,TYPE_CATALOG));if(!raw.is_object())throw DbError("CORRUPT_PAGE","Catalog is not an object");
    if(raw.value("kind",json())=="EduDBCatalog"){
        if(raw.value("version",0)!=2||!raw.contains("tables")||!raw["tables"].is_object()||!raw.contains("clusters")||!raw["clusters"].is_object())throw DbError("CORRUPT_PAGE","Invalid catalog_v2 document");
        auto routes=raw.value("cluster_routes",json::object());
        if(!routes.is_object())throw DbError("CORRUPT_CLUSTER","Cluster lock routes must be an object");
        const bool complete=raw.value("cluster_routes_complete",false);
        std::unordered_set<PageId> cluster_ids;
        for(const auto& oid:raw["clusters"]){if(!oid.is_number_unsigned())throw DbError("CORRUPT_CLUSTER","Invalid cluster OID");cluster_ids.insert(oid.get<PageId>());}
        for(auto route=routes.begin();route!=routes.end();++route){
            if(!raw["tables"].contains(route.key())||!route.value().is_number_unsigned())throw DbError("CORRUPT_CLUSTER","Invalid cluster lock route");
            if(!cluster_ids.count(route.value().get<PageId>()))throw DbError("CORRUPT_CLUSTER","Cluster lock route references an absent cluster");
        }
        auto features=raw.value("table_features",json::object());if(!features.is_object())throw DbError("CORRUPT_CATALOG","Table feature directory must be an object");
        for(auto item=features.begin();item!=features.end();++item)if(!raw["tables"].contains(item.key())||!item.value().is_object())throw DbError("CORRUPT_CATALOG","Table feature directory references an invalid relation");
        return {raw["tables"],raw["clusters"],true,std::move(routes),complete,std::move(features),raw.value("table_features_complete",false)};
    }
    return {raw,json::object(),false,json::object(),false,json::object(),false};
}
json catalog_table_features(const json& metadata){
    json result=json::object();
    if(metadata.contains("materialized_view")){
        auto definition=metadata.at("materialized_view");if(!definition.is_object())throw DbError("INVALID_METADATA","Materialized definition must be an object");
        // Delta eligibility/aux state can change at REFRESH; immutable SQL,
        // dependencies and policy determine discovery under the catalog lock.
        definition.erase("delta");result["materialized_view"]=std::move(definition);
    }
    if(metadata.contains("materialized_aux"))result["materialized_aux"]=metadata.at("materialized_aux");
    return result;
}
void complete_table_features(const std::shared_ptr<Transaction>& tx,ClusterCatalog& document){
    if(!document.extended||document.features_complete)return;
    document.features=json::object();
    for(auto table=document.tables.begin();table!=document.tables.end();++table){
        auto oid=table.value().get<PageId>();json descriptor;
        if(tx->state->dirty_descriptors.count(oid))descriptor=tx->state->descriptors.at(oid);else descriptor=parse_json(read_blob(tx,oid,TYPE_DESCRIPTOR));
        auto features=catalog_table_features(descriptor.at("meta"));if(!features.empty())document.features[table.key()]=std::move(features);
    }
    document.features_complete=true;
}
void complete_cluster_routes(const std::shared_ptr<Transaction>& tx,ClusterCatalog& document){
    if(!document.extended||document.routes_complete)return;
    // A legacy catalog is upgraded only while performing catalog-X DDL. DML
    // never needs another transaction's mutable/dirty descriptor to find a lock.
    document.routes=json::object();
    for(auto table=document.tables.begin();table!=document.tables.end();++table){
        auto oid=table.value().get<PageId>();json descriptor;
        if(tx->state->dirty_descriptors.count(oid))descriptor=tx->state->descriptors.at(oid);
        else descriptor=parse_json(read_blob(tx,oid,TYPE_DESCRIPTOR));
        if(clustered(descriptor))document.routes[table.key()]=cluster_oid(descriptor);
    }
    document.routes_complete=true;
}
void save_cluster_catalog(const std::shared_ptr<Transaction>& tx,const ClusterCatalog& document){
    auto normalized=document;complete_cluster_routes(tx,normalized);complete_table_features(tx,normalized);
    auto value=normalized.extended?json{{"kind","EduDBCatalog"},{"version",2},{"tables",normalized.tables},{"clusters",normalized.clusters},{"cluster_routes",normalized.routes},{"cluster_routes_complete",true},{"table_features",normalized.features},{"table_features_complete",true}}:normalized.tables;
    write_blob(tx,1,TYPE_CATALOG,value.dump());
}
void save_catalog_tables(const std::shared_ptr<Transaction>& tx,const json& tables){auto document=cluster_catalog(tx);document.tables=tables;for(auto i=document.routes.begin();i!=document.routes.end();)if(!tables.contains(i.key()))i=document.routes.erase(i);else ++i;for(auto i=document.features.begin();i!=document.features.end();)if(!tables.contains(i.key()))i=document.features.erase(i);else ++i;save_cluster_catalog(tx,document);}
void set_cluster_route(const std::shared_ptr<Transaction>& tx,const std::string& table,const json& metadata){
    auto document=cluster_catalog(tx);auto features=catalog_table_features(metadata);
    if(!document.extended&&features.empty())return;
    document.extended=true;const bool was_complete=document.routes_complete&&document.features_complete;complete_cluster_routes(tx,document);complete_table_features(tx,document);
    bool changed=!was_complete;
    if(metadata.contains("cluster")){
        auto oid=metadata.at("cluster").at("oid");
        if(!document.routes.contains(table)||document.routes.at(table)!=oid){document.routes[table]=oid;changed=true;}
    }else{
        if(document.routes.erase(table))changed=true;
    }
    if(features.empty()){if(document.features.erase(table))changed=true;}
    else if(!document.features.contains(table)||document.features.at(table)!=features){document.features[table]=std::move(features);changed=true;}
    if(changed)save_cluster_catalog(tx,document);
}
bool clustered(const json& d)const{return d.at("meta").contains("cluster");}
PageId cluster_oid(const json& d)const{const auto& info=d.at("meta").at("cluster");if(!info.is_object()||!info.contains("oid")||!info["oid"].is_number_unsigned())throw DbError("CORRUPT_CLUSTER","Cluster reference has no valid OID");return info["oid"].get<PageId>();}
json& load_cluster_descriptor(const std::shared_ptr<Transaction>& tx,PageId oid){
    auto found=tx->state->descriptors.find(oid);if(found==tx->state->descriptors.end()){auto value=parse_json(read_blob(tx,oid,TYPE_DESCRIPTOR));found=tx->state->descriptors.emplace(oid,std::move(value)).first;}
    auto& d=found->second;if(d.value("descriptor_kind",std::string())!="cluster"||d.value("cluster_oid",PageId(0))!=oid)throw DbError("CORRUPT_CLUSTER","Cluster descriptor kind/OID mismatch");return d;
}
json& cluster_member_descriptor(const std::shared_ptr<Transaction>& tx,PageId oid,PageId current,json* local){
    if(oid==current&&local)return *local;
    auto found=tx->state->descriptors.find(oid);if(found==tx->state->descriptors.end()){auto value=parse_json(read_blob(tx,oid,TYPE_DESCRIPTOR));found=tx->state->descriptors.emplace(oid,std::move(value)).first;}
    if(!found->second.contains("meta")||!found->second.contains("row_map"))throw DbError("CORRUPT_CLUSTER","Cluster member descriptor is not a table");
    return found->second;
}
PageId find_cluster(const std::shared_ptr<Transaction>& tx,const std::string& name){auto catalog=cluster_catalog(tx);if(!catalog.clusters.contains(name))throw DbError("CLUSTER_NOT_FOUND","Cluster not found: "+name);auto oid=catalog.clusters.at(name).get<PageId>();load_cluster_descriptor(tx,oid);return oid;}
template<class F>auto cluster_call(F&& action)->decltype(action()){
    try{return action();}catch(const cluster::Error& e){throw DbError(e.code,e.what());}catch(const record::Error& e){throw DbError(e.code,e.what());}
}
std::shared_ptr<cluster::Heap> cluster_heap(const std::shared_ptr<Transaction>& tx,PageId current,json& local){
    auto oid=cluster_oid(local);auto& descriptor=load_cluster_descriptor(tx,oid);cluster::Pager page_io;
    page_io.read=[this,tx,oid](PageId id){return heap_page(tx,id,oid);};
    page_io.write=[this,tx](PageId id,const Page& p){private_page(tx,id,p);};
    page_io.allocate=[this,tx,oid]{return allocate(tx,TYPE_HEAP,oid);};
    auto lookup=[this,tx,current,p=&local](PageId table)->json&{return cluster_member_descriptor(tx,table,current,p);};
    page_io.locate=[this,tx,lookup](PageId table,std::uint64_t lid)->std::optional<cluster::Rid>{auto& d=lookup(table);try{auto location=map_locator(tx,table,d,lid);auto rid=decode_rid(location.at("rid"),lid);return cluster::Rid{rid.page_id,rid.slot_id,rid.generation,table,lid};}catch(const DbError& e){if(e.code=="STALE_ROW_ID")return {};throw;}};
    page_io.map=[this,tx,lookup](const cluster::Rid& rid){auto& d=lookup(rid.table);bool exists=false;try{map_locator(tx,rid.table,d,rid.lid);exists=true;}catch(const DbError& e){if(e.code!="STALE_ROW_ID")throw;}set_locator(tx,rid.table,d,rid.lid,{{"rid",json::array({rid.page,rid.slot,rid.generation})}},exists);};
    page_io.unmap=[this,tx,lookup](PageId table,std::uint64_t lid){auto& d=lookup(table);access::Index map(pager(tx,table),d["row_map"]);if(!map.erase(json::array({lid}),lid))throw DbError("CORRUPT_INDEX","Cluster row-map entry missing during erase");};
    return cluster_call([&]{return std::make_shared<cluster::Heap>(std::move(page_io),descriptor,oid);});
}
std::vector<unsigned> cluster_columns(const json& metadata)const{
    const auto& info=metadata.at("cluster");if(!info.is_object()||!info.contains("columns")||!info["columns"].is_array()||info["columns"].empty())throw DbError("CLUSTER_KEY","Cluster member needs a nonempty key column list");
    std::vector<unsigned> result;std::set<unsigned> seen;for(const auto& name:info["columns"]){if(!name.is_string())throw DbError("CLUSTER_KEY","Cluster column names must be strings");bool found=false;for(std::size_t i=0;i<metadata.at("columns").size();++i)if(metadata["columns"][i].at("name")==name){if(!seen.insert(static_cast<unsigned>(i)).second)throw DbError("CLUSTER_KEY","Duplicate cluster key column");result.push_back(static_cast<unsigned>(i));found=true;break;}if(!found)throw DbError("CLUSTER_KEY","Unknown cluster key column");}return result;
}
json resolve_cluster(const std::shared_ptr<Transaction>& tx,const json& metadata){
    if(!metadata.contains("cluster"))return nullptr;
    const auto& requested=metadata.at("cluster");if(!requested.is_object()||!requested.contains("name")||!requested["name"].is_string())throw DbError("CLUSTER_KEY","Cluster reference needs a name");auto name=requested.at("name").get<std::string>();auto oid=find_cluster(tx,name);
    if(requested.contains("oid")&&requested["oid"]!=oid)throw DbError("CLUSTER_KEY","Cluster name/OID reference differs");
    auto schema=cluster_call([&]{return record::schema(metadata);});auto columns=cluster_columns(metadata);json types=json::array();for(auto index:columns){auto type=schema.columns[index].type;if(type=="BLOB")throw DbError("CLUSTER_KEY","BLOB cannot be a clustering key");types.push_back(type);}
    auto organization=metadata.value("organization",json{{"method","HEAP"}});auto method=organization.is_string()?organization.get<std::string>():organization.value("method",std::string("HEAP"));if(method!="HEAP")throw DbError("INVALID_ORGANIZATION","A multi-table cluster member requires HEAP organization");
    auto& descriptor=load_cluster_descriptor(tx,oid);if(descriptor.contains("key_types")&&descriptor.at("key_types")!=types)throw DbError("CLUSTER_KEY","Cluster key arity/types differ from existing members");auto layout=cluster_call([&]{return record::layout(schema);});if(schema.format==record::Format::Fixed&&layout.at("fixed_record_bytes").get<std::size_t>()>record::maximum_record_bytes-8)throw DbError("ROW_TOO_LARGE","Fixed record plus8-byte tableOID cannot fit one shared heap slot");
    return {{"name",name},{"oid",oid},{"columns",requested.at("columns")}};
}
void cluster_validate_table(const std::shared_ptr<Transaction>& tx,const json& table){if(table.contains("cluster"))resolve_cluster(tx,table);}
void cluster_changed(const std::shared_ptr<Transaction>& tx,PageId oid,std::int64_t delta=0){auto& descriptor=load_cluster_descriptor(tx,oid);auto rows=descriptor.value("total_rows",std::uint64_t(0));if(delta<0){auto amount=static_cast<std::uint64_t>(-delta);if(rows<amount)throw DbError("CORRUPT_CLUSTER","Shared row count underflow");rows-=amount;}else{if(static_cast<std::uint64_t>(delta)>std::numeric_limits<std::uint64_t>::max()-rows)throw DbError("DATABASE_TOO_LARGE","Shared row count overflow");rows+=static_cast<std::uint64_t>(delta);}descriptor["total_rows"]=rows;tx->state->ch10_layout_touched.erase(oid);ch10_bump(descriptor,"physical_version");save_desc(tx,oid,descriptor);}
void cluster_attach_descriptor(const std::shared_ptr<Transaction>& tx,PageId owner,json& d){
    if(!clustered(d))return;
    auto heap=cluster_heap(tx,owner,d);json definition={{"columns",d.at("meta").at("columns")},{"record_format",d.at("meta").value("record_format",json("JSON"))},{"record_options",d.at("meta").value("record_options",json::object())}};
    cluster_call([&]{heap->attach(owner,definition,cluster_columns(d.at("meta")));});cluster_changed(tx,cluster_oid(d));
}
void cluster_remove_member(const std::shared_ptr<Transaction>& tx,PageId owner,json& d){
    if(!clustered(d))return;
    auto heap=cluster_heap(tx,owner,d);auto rows=cluster_call([&]{return heap->scan(owner);});for(const auto& row:rows)cluster_call([&]{heap->erase(owner,row.id.lid);});cluster_call([&]{heap->detach_empty(owner);});if(load_cluster_descriptor(tx,cluster_oid(d)).at("members").empty())load_cluster_descriptor(tx,cluster_oid(d)).erase("key_types");cluster_changed(tx,cluster_oid(d),-static_cast<std::int64_t>(rows.size()));
}
RowId cluster_insert(const std::shared_ptr<Transaction>& tx,PageId owner,json& d,const json& row,std::uint64_t lid){auto heap=cluster_heap(tx,owner,d);auto rid=cluster_call([&]{return heap->insert(owner,lid,row);});cluster_changed(tx,cluster_oid(d),1);return {rid.page,rid.slot,rid.generation,rid.lid};}
RowId cluster_update(const std::shared_ptr<Transaction>& tx,PageId owner,json& d,const json& row,std::uint64_t lid){auto heap=cluster_heap(tx,owner,d);auto rid=cluster_call([&]{return heap->update(owner,lid,row);});cluster_changed(tx,cluster_oid(d));return {rid.page,rid.slot,rid.generation,rid.lid};}
void cluster_erase(const std::shared_ptr<Transaction>& tx,PageId owner,json& d,std::uint64_t lid){auto heap=cluster_heap(tx,owner,d);cluster_call([&]{heap->erase(owner,lid);});cluster_changed(tx,cluster_oid(d),-1);}
json cluster_stored(const std::shared_ptr<Transaction>& tx,PageId owner,json& d,std::uint64_t lid,RowId* physical){auto heap=cluster_heap(tx,owner,d);auto row=cluster_call([&]{return heap->get(owner,lid);});if(physical)*physical={row.id.page,row.id.slot,row.id.generation,lid};return row.values;}
json cluster_stats_versions(const std::shared_ptr<Transaction>& tx,const json& d){auto result=ch10_versions(d);if(clustered(d))result["cluster_physical_version"]=ch10_version(load_cluster_descriptor(tx,cluster_oid(d)),"physical_version");return result;}
json cluster_statistics(const std::shared_ptr<Transaction>& tx,const json& d){auto result=ch10_statistics(d);if(result.is_null()||!clustered(d))return result;auto captured=d.value("statistics_versions",json::object());auto actual=ch10_version(load_cluster_descriptor(tx,cluster_oid(d)),"physical_version");if(!captured.contains("cluster_physical_version")||captured["cluster_physical_version"]!=actual){result["stale"]=true;result["stale_reasons"].push_back("cluster_physical_version");}return result;}
json cluster_organization_stats(const std::shared_ptr<Transaction>& tx,const json& d){auto oid=cluster_oid(d);auto& c=load_cluster_descriptor(tx,oid);std::size_t pages=0;for(const auto& group:c.at("groups"))pages+=group.at("pages").size();auto total=c.at("total_rows").get<std::uint64_t>(),own=d.at("row_count").get<std::uint64_t>();if(total<own)throw DbError("CORRUPT_CLUSTER","Shared row count is smaller than member count");return {{"method","MULTITABLE_CLUSTER"},{"cluster_oid",oid},{"pages",pages},{"leaf_pages",pages},{"height",pages?1:0},{"entries",own},{"shared_cluster_rows",total},{"target_table_rows",own},{"other_table_rows",total-own},{"member_tables",c.at("members").size()},{"exact",true},{"sharing","all cluster heap pages; target table rows filtered by tableOID"},{"cluster_physical_version",ch10_version(c,"physical_version")}};}
json cluster_record_layout(const json& d){auto result=heap_record_layout(d.at("meta"));if(!clustered(d))return result;result["table_oid_prefix_bytes"]=8;result["record_header_offset"]=8;if(result.contains("fixed_record_bytes")&&!result["fixed_record_bytes"].is_null()){result["member_codec_bytes"]=result["fixed_record_bytes"];result["fixed_record_bytes"]=result["fixed_record_bytes"].get<std::size_t>()+8;}if(result.contains("fields"))for(auto& field:result["fields"])field["offset"]=field["offset"].get<std::size_t>()+8;return result;}
