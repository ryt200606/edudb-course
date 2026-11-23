// Included inside Storage::Impl. BufferPool owns the buffer mutex. Loader
// and flusher never acquire the transaction coordinator: WAL -> coordinator
// -> buffer is the only supported order. Guards release buffer metadata only.
void setup_buffer(){
    buffer=std::make_unique<buffer::BufferPool>(frame_limit,
        [this](PageId id){Page p{};try{
            if(data->size()<(id+1)*PAGE_SIZE)throw DbError("CORRUPT_PAGE","Nonexistent page");
            data->read(id*PAGE_SIZE,p.data(),p.size());
            if(!valid_page(p,id))throw DbError("CORRUPT_PAGE","Page checksum mismatch: "+std::to_string(id));
            if(get64(p.data()+16)>durable_lsn.load())throw DbError("CORRUPT_WAL","Data page is newer than the last durable commit");
            return p;
        }catch(...){failed=true;registry_cv.notify_all();throw;}},
        [this](PageId id,const Page& p){try{
            if(get64(p.data()+8)!=id||get64(p.data()+16)>durable_lsn.load())throw DbError("WAL_ORDER","Data page precedes durable WAL or has wrong page id");
            data->write(id*PAGE_SIZE,p.data(),p.size());++writes;crash_at("after_page_flush");
        }catch(...){failed=true;registry_cv.notify_all();throw;}});
}
Page committed_page(PageId id){
    if(aries_mode)return aries_manager->read(0,id);
    try{auto guard=buffer->fetch(id);return guard.image();}
    catch(const buffer::Error& e){throw DbError(e.code,e.what());}
}
void prefetch_heap(const std::shared_ptr<Transaction>& tx,const std::vector<PageId>& heap,std::size_t current){
    if(aries_mode)return;
    const auto window=buffer->statistics().config.read_ahead_pages;if(!window)return;
    std::vector<PageId> candidates;
    for(auto i=current+1;i<heap.size()&&candidates.size()<window;++i)
        if(!tx->state->pages.count(heap[i])&&!tx->state->new_pages.count(heap[i]))candidates.push_back(heap[i]);
    try{buffer->prefetch(candidates);}catch(const buffer::Error& e){throw DbError(e.code,e.what());}
}
json configure_buffer(const json& request){
    if(aries_mode)return aries_buffer_configuration(request);
    // Replay/promotion may replace the pool outside any registered transaction.
    std::lock_guard<std::mutex> lifecycle(wal_mutex);
    if(!request.is_object())throw DbError("INVALID_CONFIG","Buffer configuration must be an object");
    auto config=buffer->statistics().config;
    for(auto i=request.begin();i!=request.end();++i)if(i.key()!="policy"&&i.key()!="read_ahead_pages"&&i.key()!="wait_ms")throw DbError("INVALID_CONFIG","Unknown buffer configuration field: "+i.key());
    if(request.contains("policy")){
        if(!request["policy"].is_string())throw DbError("INVALID_CONFIG","Buffer policy must be a string");
        const auto name=request["policy"].get<std::string>();
        if(name=="LRU")config.policy=buffer::Policy::Lru;else if(name=="MRU")config.policy=buffer::Policy::Mru;
        else if(name=="CLOCK")config.policy=buffer::Policy::Clock;else if(name=="TOSS_IMMEDIATE")config.policy=buffer::Policy::TossImmediate;
        else throw DbError("INVALID_CONFIG","Buffer policy must be LRU/MRU/CLOCK/TOSS_IMMEDIATE");
    }
    for(const char* name:{"read_ahead_pages","wait_ms"})if(request.contains(name)){
        const auto& value=request[name];if((!value.is_number_unsigned()&&!value.is_number_integer())||value.get<std::int64_t>()<0||value.get<std::uint64_t>()>(std::string(name)=="wait_ms"?5000u:16u))throw DbError("INVALID_CONFIG",std::string(name)+" is out of range");
        if(std::string(name)=="wait_ms")config.wait_ms=value.get<unsigned>();else config.read_ahead_pages=value.get<unsigned>();
    }
    try{buffer->configure(config);}catch(const buffer::Error& e){throw DbError(e.code,e.what());}
    return {{"policy",buffer::policy_name(config.policy)},{"read_ahead_pages",config.read_ahead_pages},{"wait_ms",config.wait_ms},{"prefetch_io_mode","synchronous read-ahead; no hardware timing claim"}};
}
