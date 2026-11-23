// Storage metadata measured in the caller's transaction-visible page view.
// Included in Storage::Impl; this never consults a global latest-page cache.
static std::uint64_t ch10_version(const json& descriptor,const char* field){
    return descriptor.value(field,std::uint64_t(std::string(field)=="data_version"?0:1));
}
static void ch10_bump(json& descriptor,const char* field){
    auto value=ch10_version(descriptor,field);
    if(value==std::numeric_limits<std::uint64_t>::max())
        throw DbError("DATABASE_TOO_LARGE","Table metadata version exhausted");
    descriptor[field]=value+1;
}
static json ch10_versions(const json& descriptor){
    return {{"data_version",ch10_version(descriptor,"data_version")},
            {"schema_version",ch10_version(descriptor,"schema_version")},
            {"physical_version",ch10_version(descriptor,"physical_version")}};
}
void ch10_touch(const std::shared_ptr<Transaction>& tx,PageId owner){
    if(owner){tx->state->ch10_layout_touched.insert(owner);tx->state->ch10_physical_cache.erase(owner);}
}
json ch10_index_measure(const std::shared_ptr<Transaction>& tx,PageId owner,
                       json descriptor,bool data_pages){
    auto page_io=pager(tx,owner,data_pages);
    std::set<PageId> visited;
    json kinds=json::object();std::uint64_t tree_leaves=0,leaves=0;
    constexpr std::size_t maximum=2000000;
    auto walk=[&](PageId first){
        std::vector<PageId> pending{first};
        while(!pending.empty()){
            auto id=pending.back();pending.pop_back();if(!id||visited.count(id))continue;
            if(visited.size()>=maximum)throw DbError("INDEX_RESOURCE","Exact metadata traversal exceeds two million pages");
            visited.insert(id);auto page=page_io.read(id);auto kind=page.value("kind",std::string("unknown"));
            kinds[kind]=kinds.value(kind,std::uint64_t(0))+1;
            if(page.value("leaf",false)){++tree_leaves;++leaves;}
            else if(kind=="hash_bucket"||kind=="hash_probe"||kind=="bitmap_posting")++leaves;
            if(page.contains("children"))for(const auto& child:page.at("children"))pending.push_back(child.get<PageId>());
            if(page.value("next",PageId(0)))pending.push_back(page.at("next").get<PageId>());
        }
    };
    std::function<void(const json&)> visit=[&](const json& d){
        walk(d.value("root",PageId(0)));walk(d.value("head",PageId(0)));
        if(d.contains("directory_pages"))for(const auto& id:d.at("directory_pages")){
            walk(id.get<PageId>());auto directory_page=page_io.read(id.get<PageId>());
            for(const auto& bucket:directory_page.at("values"))walk(bucket.get<PageId>());
        }
        for(const char* nested:{"ordered_directory","bitmap_dictionary","bitmap_masks","posting_dictionary"})
            if(d.contains(nested))visit(d.at(nested));
        for(const char* dictionary:{"posting_dictionary","bitmap_dictionary","bitmap_masks"})if(d.contains(dictionary)){
            auto copy=d.at(dictionary);access::Index index(page_io,copy);auto next=index.cursor();access::Entry entry;
            while(next(entry))walk(entry.payload.at(std::string(dictionary)=="bitmap_masks"?"page":"head").get<PageId>());
        }
    };
    visit(descriptor);
    const auto method=descriptor.value("method",std::string("BPLUS"));
    return {{"method",method},{"pages",visited.size()},{"leaf_pages",leaves},
            {"tree_leaf_pages",tree_leaves},{"height",descriptor.value("height",std::uint64_t(0))},
            {"entries",descriptor.value("count",std::uint64_t(0))},{"page_kinds",kinds},
            {"exact",true},{"leaf_scope","tree leaves plus terminal bucket/posting pages; nested dictionaries included"}};
}
json ch10_physical(const std::shared_ptr<Transaction>& tx,PageId owner,const json& d){
    auto signature=cluster_stats_versions(tx,d).dump();auto found=tx->state->ch10_physical_cache.find(owner);
    if(found!=tx->state->ch10_physical_cache.end()&&found->second.at("signature")==signature){
        auto result=found->second.at("result");result["cache_hit"]=true;
        result["metadata_io"]={{"heap_page_reads",0},{"index_page_reads",0},{"total_page_reads",0}};return result;
    }
    const auto heap_before=tx->state->heap_reads,index_before=tx->state->index_reads;
    json organization;
    if(clustered(d))organization=cluster_organization_stats(tx,d);
    else if(org_method(d)=="HEAP")organization={{"method","HEAP"},{"pages",d.at("heap").size()},
        {"leaf_pages",d.at("heap").size()},{"height",d.at("heap").empty()?0:1},{"entries",d.at("row_count")},{"exact",true}};
    else organization=ch10_index_measure(tx,owner,d.at("organization_index"),true);
    json indexes=json::array();
    for(const auto& ix:d.at("index_roots")){
        auto measured=ix.value("organization_alias",false)?organization:ch10_index_measure(tx,owner,ix,false);
        measured["name"]=ix.at("name");measured["organization_alias"]=ix.value("organization_alias",false);
        measured["entries"]=d.at("row_count");indexes.push_back(std::move(measured));
    }
    auto row_map=ch10_index_measure(tx,owner,d.at("row_map"),false);
    json result={{"exact",true},{"measurement_scope","transaction-visible exact traversal"},
        {"table_pages",organization.at("pages")},{"organization",organization},{"indexes",indexes},{"row_map",row_map},
        {"versions",cluster_stats_versions(tx,d)},{"cache_hit",false},
        {"metadata_io_scope","access-page visits during this metadata traversal; excludes catalog/descriptor reads"}};
    tx->state->ch10_physical_cache[owner]={{"signature",signature},{"result",result}};
    auto heap=tx->state->heap_reads-heap_before,index=tx->state->index_reads-index_before;
    result["metadata_io"]={{"heap_page_reads",heap},{"index_page_reads",index},{"total_page_reads",heap+index}};return result;
}
json ch10_statistics(const json& descriptor){
    if(!descriptor.contains("statistics"))return nullptr;
    auto result=descriptor.at("statistics");json reasons=json::array();
    auto captured=descriptor.value("statistics_versions",json::object());
    for(const char* field:{"data_version","schema_version","physical_version"})
        if(!captured.contains(field)||captured.at(field)!=ch10_version(descriptor,field))reasons.push_back(field);
    result["stale"]=!reasons.empty();result["stale_reasons"]=std::move(reasons);return result;
}
void cleanup_query_runs(){
    // Called only during startup, after the database directory lock is held.
    const std::string prefix="edudb-query-";
    for(const auto& item:fs::directory_iterator(directory)){
        const auto name=item.path().filename().u8string();if(name.rfind(prefix,0)!=0)continue;
        bool owned=false;
        try{
            if(item.is_symlink()||!item.is_regular_file()||name.size()<prefix.size()+64+1+1+4||name.substr(name.size()-4)!=".tmp")throw std::runtime_error("Unrecognized name/type");
            const auto nonce=name.substr(prefix.size(),64);const auto tail=name.substr(prefix.size()+65,name.size()-4-prefix.size()-65);
            if(name[prefix.size()+64]!='-'||nonce.find_first_not_of("0123456789abcdef")!=std::string::npos||tail.empty()||tail.find_first_not_of("0123456789")!=std::string::npos)throw std::runtime_error("Unrecognized run name");
            auto run=std::stoull(tail);if(!run||std::to_string(run)!=tail)throw std::runtime_error("Invalid run identity");
            {NativeFile file(item.path(),false);auto bytes=file.size();if(bytes<80)throw std::runtime_error("Incomplete identity header");
                std::array<unsigned char,80> h{};file.read(0,h.data(),h.size());auto crc=get32(h.data()+76);put32(h.data()+76,0);
                auto maximum=get32(h.data()+64),sealed=get32(h.data()+68);
                if(std::memcmp(h.data(),"EDUQSP1",7)||h[7]||std::memcmp(h.data()+8,db_uuid.data(),32)||get64(h.data()+40)!=run||maximum<16||maximum>64u*1024u*1024u||sealed>1||get32(h.data()+72)||crc!=crc32(h.data(),h.size()))throw std::runtime_error("Invalid run header");
                if(sealed){
                    if(get64(h.data()+56)!=bytes||bytes>1024ull*1024ull*1024ull)throw std::runtime_error("Invalid or excessive sealed length");
                    std::uint64_t offset=80,rows=0;
                    while(offset<bytes){
                        if(bytes-offset<24)throw std::runtime_error("Truncated frame");
                        std::array<unsigned char,24> frame{};file.read(offset,frame.data(),frame.size());auto frame_crc=get32(frame.data()+20);put32(frame.data()+20,0);auto length=get32(frame.data()+4);
                        if(get32(frame.data())!=0x51524631||!length||length>maximum||get64(frame.data()+8)!=rows||frame_crc!=crc32(frame.data(),frame.size())||length>bytes-offset-24)throw std::runtime_error("Invalid frame");
                        std::vector<unsigned char> payload(length);file.read(offset+24,payload.data(),payload.size());if(get32(frame.data()+16)!=crc32(payload.data(),payload.size()))throw std::runtime_error("Invalid payload CRC");
                        offset+=24+length;++rows;
                    }
                    if(rows!=get64(h.data()+48))throw std::runtime_error("Wrong sealed row count");
                }
                owned=true;
            }
            if(owned){std::error_code error;if(fs::remove(item.path(),error)&&!error)++stale_query_files_removed;else ++unrecognized_query_files_retained;}
        }catch(...){++unrecognized_query_files_retained;}
    }
}
