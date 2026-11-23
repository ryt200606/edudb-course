// Include once inside Storage::Impl; include again at namespace scope with
// EDUDB_STORAGE_PROTOCOL_PUBLIC defined for the public Storage adapters.
#ifndef EDUDB_STORAGE_PROTOCOL_PUBLIC
void protocol_configure() {
    if(protocol_configured)return;
    auto limit=[](const char* name,std::size_t fallback) {
        const char* raw=std::getenv(name);if(!raw)return fallback;
        std::string s(raw);if(s.empty()||s.find_first_not_of("0123456789")!=std::string::npos)throw DbError("ARGUMENT",std::string(name)+" must be decimal bytes");
        unsigned long long n=0;try{n=std::stoull(s);}catch(...){throw DbError("ARGUMENT",std::string(name)+" is out of range");}
        if(n<PAGE_SIZE||n>2ull*1024*1024*1024)throw DbError("ARGUMENT",std::string(name)+" must be 8192..2147483648");
        return static_cast<std::size_t>(n);
    };
    if(const char* raw=std::getenv("EDUDB_MVTO_METADATA_SLOTS")){std::string value(raw);if(value.empty()||value.find_first_not_of("0123456789")!=std::string::npos)throw DbError("ARGUMENT","EDUDB_MVTO_METADATA_SLOTS must be decimal1..1048576");std::uint64_t n=0;try{n=std::stoull(value);}catch(...){throw DbError("ARGUMENT","MVTO metadata slot limit out of range");}if(!n||n>1048576)throw DbError("ARGUMENT","MVTO metadata slot limit must be1..1048576");mvto.set_limit(static_cast<std::size_t>(n));}
    protocol_history_limit=limit("EDUDB_HISTORY_LIMIT_BYTES",protocol_history_limit);
    protocol_savepoint_limit=limit("EDUDB_SAVEPOINT_LIMIT_BYTES",protocol_savepoint_limit);
    protocol_configured=true;
}
void protocol_gc() {
    auto guard=protocols.lock();
    if(!protocols.has_snapshot_readers()) {
        protocol_history.clear();protocol_page_csn.clear();protocol_page_birth.clear();protocol_history_bytes=0;mvto.reset();return;
    }
    const auto oldest=protocols.oldest_snapshot_csn();
    for(auto it=protocol_history.begin();it!=protocol_history.end();) {
        auto& chain=it->second;
        if(protocol_page_csn[it->first]<=oldest) {
            protocol_history_bytes-=chain.size()*PAGE_SIZE;it=protocol_history.erase(it);continue;
        }
        while(chain.size()>1&&chain[1].csn<=oldest){chain.pop_front();protocol_history_bytes-=PAGE_SIZE;}
        ++it;
    }
    std::map<PageId,std::set<std::uint64_t>> retained;for(const auto& chain:protocol_history)for(const auto& v:chain.second)retained[chain.first].insert(get64(v.image.data()+16));
    mvto.collect(retained);
}
void protocol_initialize(const std::shared_ptr<Transaction>& tx,const TransactionOptions& options) {
    if(aries_mode&&!logical_mode&&options.record_locks)throw DbError("ARIES_RECORD_UNSUPPORTED","ARIES_PAGE full-page UNDO requires coarse TABLE locks");
    if(aries_mode&&options.protocol!=TransactionProtocol::Strict2PL)throw DbError("ARIES_PROTOCOL","ARIES_PAGE requires STRICT_2PL; full-page undo is incompatible with MVCC/OCC/TS");
    auto guard=protocols.lock();protocol_configure();tx->state->protocol=protocols.begin(tx->id,options);if(!logical_mode)version_graph.begin(tx->id,tx->state->protocol->start_csn);
}
Page protocol_read_page(const std::shared_ptr<Transaction>& tx,PageId id) {
    if(logical_mode){auto found=tx->state->pages.find(id);if(found!=tx->state->pages.end())return found->second;return aries_manager->read(tx->state->logical_kind=="physical"?tx->id:tx->state->logical_inner,id);}
    if(aries_mode){auto guard=protocols.lock();try{auto selected=aries_manager->read(tx->id,id);if(!tx->state->aries_pages.count(id))version_graph.read(tx->id,id,get64(selected.data()+16),get16(selected.data()+6));return selected;}catch(...){aries_note_failure();throw;}}
    auto overlay=tx->state->pages.find(id);if(overlay!=tx->state->pages.end())return overlay->second;
    auto guard=protocols.lock();auto ctx=tx->state->protocol;
    if(!ctx)throw DbError("NO_TRANSACTION","Missing transaction protocol context");
    Page selected{};
    if(ctx->options.protocol==TransactionProtocol::Mvto){
        auto current=committed_page(id);auto latest=get64(current.data()+16);mvto.ensure_current(id,latest);
        std::vector<std::uint64_t> available{latest};auto history=protocol_history.find(id);
        if(history!=protocol_history.end())for(const auto& v:history->second)available.push_back(get64(v.image.data()+16));
        auto chosen=mvto.select(id,available,ctx->start_ts);if(!chosen)throw DbError("PAGE_NOT_VISIBLE","No committed page WTS is <= MVTO transaction timestamp");
        if(*chosen==latest)selected=std::move(current);else{bool found=false;if(history!=protocol_history.end())for(const auto& v:history->second)if(get64(v.image.data()+16)==*chosen){selected=v.image;found=true;break;}if(!found)throw DbError("MVTO_VERSION_MISSING","Chosen MVTO page image is unavailable");}
        mvto.read(id,*chosen,ctx->start_ts);
    }else if((ctx->options.protocol==TransactionProtocol::Strict2PL&&!ctx->options.record_locks)||
       (ctx->options.protocol==TransactionProtocol::Mv2pl&&!ctx->options.read_only))selected=committed_page(id);
    else {
        const auto current=protocol_page_csn.count(id)?protocol_page_csn.at(id):0;
        const auto birth=protocol_page_birth.count(id)?protocol_page_birth.at(id):0;
        if(birth>ctx->read_csn)throw DbError("PAGE_NOT_VISIBLE","Page did not exist in the transaction read view");
        if(current<=ctx->read_csn)selected=committed_page(id);
        else {
            auto chain=protocol_history.find(id);bool found=false;
            if(chain!=protocol_history.end())for(auto i=chain->second.rbegin();i!=chain->second.rend();++i)if(i->csn<=ctx->read_csn){selected=i->image;found=true;break;}
            if(!found)throw DbError("SNAPSHOT_TOO_OLD","Required committed page version is unavailable");
        }
    }
    protocols.note_page_read(ctx,id,get64(selected.data()+16));version_graph.read(tx->id,id,get64(selected.data()+16),get16(selected.data()+6));return selected;
}
void protocol_record_allocation(const std::shared_ptr<Transaction>& tx,PageId id) {
    tx->state->protocol_allocated_pages.insert(id);
}
void protocol_forget_allocation(const std::shared_ptr<Transaction>& tx,PageId id) {
    tx->state->protocol_allocated_pages.erase(id);tx->state->protocol_write_base_lsn.erase(id);
}
void protocol_record_private_write(const std::shared_ptr<Transaction>& tx,PageId id) {
    auto ctx=tx->state->protocol;
    if(!ctx)throw DbError("NO_TRANSACTION","Missing transaction protocol context");
    if(ctx->options.read_only)throw DbError("READ_ONLY_TRANSACTION","A read-only transaction cannot modify pages");
    if(tx->state->protocol_allocated_pages.count(id)||tx->state->protocol_write_base_lsn.count(id))return;
    // Capture the selected page version BEFORE replacing it with a private page.
    auto page=protocol_read_page(tx,id);tx->state->protocol_write_base_lsn[id]=get64(page.data()+16);
}
ProtocolCommit protocol_prepare_commit(const std::shared_ptr<Transaction>& tx) {
    if(logical_mode){auto guard=protocols.lock();std::vector<TransactionControl::PageCheck> checks;for(const auto& p:tx->state->aries_pages){auto image=aries_manager->read(tx->state->logical_kind=="physical"?tx->id:0,p.first);auto lsn=get64(image.data()+16);checks.push_back({p.first,lsn,lsn,false,true});}return {protocols.prepare_commit(tx->state->protocol,checks),{},{}};}
    if(aries_mode){
        // Strict2PL plus Manager page ownership guarantees these current images
        // belong to this writer. They are not old private-base snapshots.
        auto outer=protocols.lock();std::vector<TransactionControl::PageCheck> owned;std::vector<PageVersionGraph::Write> graph_writes;
        for(const auto& p:tx->state->aries_pages){auto page=aries_manager->read(tx->id,p.first);auto lsn=get64(page.data()+16);owned.push_back({p.first,lsn,lsn,tx->state->new_pages.count(p.first)!=0,true});
            auto base=tx->state->protocol_write_base_lsn.find(p.first);graph_writes.push_back({p.first,base==tx->state->protocol_write_base_lsn.end()?0:base->second,lsn,p.second});}
        return {protocols.prepare_commit(tx->state->protocol,owned),{},std::move(graph_writes)};
    }
    auto outer=protocols.lock();protocol_gc();std::vector<TransactionControl::PageCheck> checks;
    std::vector<std::pair<PageId,Page>> previous;
    std::set<PageId> written;std::vector<PageVersionGraph::Write> graph_writes;
    for(const auto& p:tx->state->pages) {
        written.insert(p.first);const bool fresh=tx->state->protocol_allocated_pages.count(p.first)!=0;
        if(fresh){checks.push_back({p.first,0,0,true,true});graph_writes.push_back({p.first,0,0,get16(p.second.data()+6)});continue;}
        auto base=tx->state->protocol_write_base_lsn.find(p.first);
        if(base==tx->state->protocol_write_base_lsn.end())throw DbError("INTERNAL_ERROR","Private page has no captured write base");
        auto current=committed_page(p.first);
        if(tx->state->protocol->options.protocol==TransactionProtocol::Mvto){try{mvto.validate_write(p.first,base->second,get64(current.data()+16),tx->state->protocol->start_ts);}catch(const DbError& ex){protocols.event(tx->state->protocol,"mvto_write_rejected","page:"+std::to_string(p.first),{{"code",ex.code},{"base_lsn",base->second},{"current_lsn",get64(current.data()+16)},{"timestamp",tx->state->protocol->start_ts}});throw;}}
        checks.push_back({p.first,base->second,get64(current.data()+16),false,true});graph_writes.push_back({p.first,get64(current.data()+16),0,get16(p.second.data()+6)});
        if(protocols.has_snapshot_readers(tx->id))previous.emplace_back(p.first,std::move(current));
    }
    if(tx->state->protocol->options.protocol==TransactionProtocol::Occ)
        for(const auto& r:tx->state->protocol->read_pages)if(!written.count(r.first)&&!tx->state->protocol_allocated_pages.count(r.first)) {
            auto current=committed_page(r.first);checks.push_back({r.first,r.second,get64(current.data()+16),false,false});
        }
    if(previous.size()>(protocol_history_limit-protocol_history_bytes)/PAGE_SIZE)
        throw DbError("VERSION_HISTORY_LIMIT","Committed-page history budget exceeded; finish old snapshots or roll back");
    auto permit=protocols.prepare_commit(tx->state->protocol,checks);
    if(tx->state->protocol->options.protocol==TransactionProtocol::Mvto){std::vector<PageId> pages;for(const auto& p:tx->state->pages)pages.push_back(p.first);mvto.prepare_publish(pages,!protocols.has_snapshot_readers(tx->id));}
    return {std::move(permit),std::move(previous),std::move(graph_writes)};
}
void protocol_publish_versions(const std::shared_ptr<Transaction>& tx,ProtocolCommit& pending) {
    for(auto& write:pending.graph_writes)write.new_lsn=get64(tx->state->pages.at(write.page).data()+16);
    if(!pending.permit.guard.owns_lock())throw DbError("INTERNAL_ERROR","Page publication requires the protocol permit");
    for(auto& p:pending.previous_pages) {
        auto& chain=protocol_history[p.first];const auto old=protocol_page_csn.count(p.first)?protocol_page_csn.at(p.first):0;
        if(chain.empty()||chain.back().csn!=old){chain.push_back({old,std::move(p.second)});protocol_history_bytes+=PAGE_SIZE;}
    }
    for(const auto& p:tx->state->pages) {
        if(tx->state->protocol->options.protocol==TransactionProtocol::Mvto)mvto.publish(p.first,get64(p.second.data()+16),tx->state->protocol->start_ts);
        protocol_page_csn[p.first]=pending.permit.csn;
        if(tx->state->protocol_allocated_pages.count(p.first))protocol_page_birth[p.first]=pending.permit.csn;
    }
}
void protocol_complete_commit(ProtocolCommit& pending) {
    if(!logical_mode&&!pending.graph_writes.empty())version_graph.publish(pending.permit.context->id,pending.permit.csn,pending.graph_writes);
    protocols.complete_commit(pending.permit,durable_lsn.load());
}
void protocol_finish(const std::shared_ptr<Transaction>& tx) {
    if(!logical_mode)version_graph.finish(tx->id,tx->state->protocol&&tx->state->protocol->committed?"COMMITTED":failed.load()?"UNKNOWN":"ABORTED");
    protocols.finish(tx->state->protocol,failed.load()?"abandon_on_recovery_required":"rollback");
    tx->state->protocol_savepoints.clear();tx->state->protocol_savepoint_bytes=0;
    tx->state->protocol_write_base_lsn.clear();tx->state->protocol_allocated_pages.clear();protocol_gc();
}
TransactionControl::StatementView protocol_refresh(const std::shared_ptr<Transaction>& tx,const std::vector<TransactionAccess>& accesses) {
    auto guard=protocols.lock();auto old=tx->state->protocol->read_csn;
    auto view=protocols.before_statement(tx->state->protocol,accesses);
    if(old!=view.read_csn) {
        // A private write must not be combined with an intervening committed
        // version of the same page, even in a per-statement timestamp/RC view.
        for(const auto& p:tx->state->protocol_write_base_lsn)if(tx->state->pages.count(p.first)) {
            auto now=committed_page(p.first);
            if(get64(now.data()+16)!=p.second)throw DbError("SERIALIZATION_FAILURE","Private write base changed before a read-view refresh");
        }
        for(auto i=tx->state->descriptors.begin();i!=tx->state->descriptors.end();) {
            if(!tx->state->dirty_descriptors.count(i->first))i=tx->state->descriptors.erase(i);else ++i;
        }
        for(auto i=tx->state->descriptor_names.begin();i!=tx->state->descriptor_names.end();) {
            if(!tx->state->descriptors.count(i->second))i=tx->state->descriptor_names.erase(i);else ++i;
        }
        tx->state->ch10_physical_cache.clear();
        protocols.event(tx->state->protocol,"read_view_refreshed","",{{"previous_csn",old},{"current_csn",view.read_csn}});
        protocol_gc();
    }
    return view;
}
std::size_t protocol_snapshot_bytes(const Transaction::State& state) {
    std::size_t n=state.pages.size()*PAGE_SIZE+state.changefeed.snapshot_bytes()+state.aries_pages.size()*(sizeof(PageId)+sizeof(std::uint16_t));
    for(const auto& d:state.descriptors)n+=d.second.dump().size();
    for(const auto& d:state.descriptor_names)n+=d.first.size()+sizeof(PageId);
    n+=state.new_pages.size()*sizeof(PageId)+state.dirty_descriptors.size()*sizeof(PageId);
    for(const auto& m:state.moved_rows)n+=m.second.size()*2*sizeof(PageId);
    n+=state.protocol_write_base_lsn.size()*2*sizeof(PageId)+state.protocol_allocated_pages.size()*sizeof(PageId);
    n+=state.ch10_layout_touched.size()*sizeof(PageId);
    for(const auto& table:state.protocol->write_tables)n+=table.size();
    for(const auto& table:state.protocol->ignored_write_tables)n+=table.size();
    return n;
}
void protocol_savepoint(const std::shared_ptr<Transaction>& tx,const std::string& name) {
    if(name.empty())throw DbError("INVALID_SAVEPOINT","Savepoint name is empty");
    auto& st=*tx->state;std::size_t keep=st.protocol_savepoints.size();
    for(std::size_t n=0;n<st.protocol_savepoints.size();++n)if(st.protocol_savepoints[n].name==name){keep=n;break;}
    std::size_t bytes=protocol_snapshot_bytes(st),retained=0;
    for(std::size_t n=0;n<keep;++n)retained+=st.protocol_savepoints[n].accounted_bytes;
    if(keep>=16||bytes>protocol_savepoint_limit||retained>protocol_savepoint_limit-bytes)
        throw DbError("SAVEPOINT_LIMIT","At most 16 savepoints and the separate saved-image/serialized-metadata byte budget are allowed");
    Transaction::State::ProtocolSavepoint saved;
    saved.name=name;saved.pages=st.pages;saved.new_pages=st.new_pages;saved.descriptors=st.descriptors;
    saved.changefeed=st.changefeed.snapshot();
    saved.descriptor_names=st.descriptor_names;saved.dirty_descriptors=st.dirty_descriptors;saved.moved_rows=st.moved_rows;
    saved.write_base_lsn=st.protocol_write_base_lsn;saved.allocated_pages=st.protocol_allocated_pages;saved.layout_touched=st.ch10_layout_touched;
    saved.read_csn=st.protocol->read_csn;saved.write_tables=st.protocol->write_tables;saved.ignored_write_tables=st.protocol->ignored_write_tables;saved.accounted_bytes=bytes;
    saved.aries_pages=st.aries_pages;
    st.protocol_savepoints.resize(keep);st.protocol_savepoints.push_back(std::move(saved));st.protocol_savepoint_bytes=retained+bytes;
    if(aries_mode)aries_manager->savepoint(tx->id,name);
    protocols.event(st.protocol,"savepoint",name,{{"saved_bytes",st.protocol_savepoint_bytes}});
}
void protocol_rollback_to(const std::shared_ptr<Transaction>& tx,const std::string& name) {
    auto& st=*tx->state;std::size_t at=st.protocol_savepoints.size();
    for(std::size_t n=0;n<st.protocol_savepoints.size();++n)if(st.protocol_savepoints[n].name==name){at=n;break;}
    if(at==st.protocol_savepoints.size())throw DbError("SAVEPOINT_NOT_FOUND","Unknown savepoint: "+name);
    const auto& saved=st.protocol_savepoints[at];
    if(aries_mode){auto guard=protocols.lock();try{aries_manager->rollback_to(tx->id,name);aries_alias_restored(tx,saved.aries_pages);st.wal_bytes=aries_manager->transaction_wal_bytes(tx->id);}catch(...){aries_note_failure();throw;}st.aries_pages=saved.aries_pages;}
    st.pages=saved.pages;st.rebuild_private_page_types();st.new_pages=saved.new_pages;st.descriptors=saved.descriptors;st.descriptor_names=saved.descriptor_names;
    st.changefeed.restore(saved.changefeed);
    st.dirty_descriptors=saved.dirty_descriptors;st.moved_rows=saved.moved_rows;st.protocol_write_base_lsn=saved.write_base_lsn;
    st.protocol_allocated_pages=saved.allocated_pages;st.ch10_layout_touched=saved.layout_touched;st.ch10_physical_cache.clear();
    const bool moving=st.protocol->options.record_locks||st.protocol->options.read_committed||st.protocol->options.protocol==TransactionProtocol::Timestamp||st.protocol->options.protocol==TransactionProtocol::Thomas;
    if(!moving)st.protocol->read_csn=saved.read_csn;
    else if(st.protocol->read_csn!=saved.read_csn) {
        for(auto i=st.descriptors.begin();i!=st.descriptors.end();) {
            if(!st.dirty_descriptors.count(i->first))i=st.descriptors.erase(i);else ++i;
        }
        for(auto i=st.descriptor_names.begin();i!=st.descriptor_names.end();) {
            if(!st.descriptors.count(i->second))i=st.descriptor_names.erase(i);else ++i;
        }
    }
    st.protocol->write_tables=saved.write_tables;st.protocol->ignored_write_tables=saved.ignored_write_tables;
    st.protocol_savepoints.resize(at+1);st.protocol_savepoint_bytes=0;
    for(const auto& p:st.protocol_savepoints)st.protocol_savepoint_bytes+=p.accounted_bytes;
    st.aborted=false;protocols.event(st.protocol,"rollback_to",name,{{"private_pages",st.pages.size()}});
    // Global page identifiers, timestamp high-water marks and already-held
    // transaction locks are intentionally never rewound or released here.
}
void protocol_release_savepoint(const std::shared_ptr<Transaction>& tx,const std::string& name) {
    auto& st=*tx->state;std::size_t at=st.protocol_savepoints.size();
    for(std::size_t n=0;n<st.protocol_savepoints.size();++n)if(st.protocol_savepoints[n].name==name){at=n;break;}
    if(at==st.protocol_savepoints.size())throw DbError("SAVEPOINT_NOT_FOUND","Unknown savepoint: "+name);
    if(aries_mode)aries_manager->release_savepoint(tx->id,name);
    st.protocol_savepoints.resize(at);st.protocol_savepoint_bytes=0;
    for(const auto& p:st.protocol_savepoints)st.protocol_savepoint_bytes+=p.accounted_bytes;
    protocols.event(st.protocol,"release_savepoint",name);
}
json protocol_diagnostics() {
    auto guard=protocols.lock();auto out=protocols.diagnostics();std::size_t saved=0;
    // Avoid acquiring registry while holding this mutex: per-transaction saved
    // bytes are exposed by transaction_stats; the global field reports history.
    (void)saved;out["history_bytes"]=protocol_history_bytes;out["history_limit_bytes"]=protocol_history_limit;
    out["history_scope"]="retained 8192-byte committed page images, excludes allocator/map overhead and process RSS";out["page_version_graph"]=logical_mode?json{{"disabled",true},{"scope","Physical mini-transactions contain outer-uncommitted logical operations; committed-page-CSN graph is not applicable"},{"is_serializability_proof",false}}:version_graph.diagnostics();out["mvto"]=mvto.diagnostics();
    out["savepoint_limit_bytes_per_transaction"]=protocol_savepoint_limit;
    out["savepoint_scope"]="separate hard budget for copied page images and serialized descriptor bytes, not process RSS";
    if(logical_mode){out["read_csn_scope"]="coordinator observation chronology only; SQL reads use current physical authority under key/range or coarse locks, not physical-CSN snapshots";out["snapshot_conflicts"]="not applicable; logical mode has no MVCC/physical-page snapshot";out["history_scope"]="committed-page image history is not used by logical mode";out.erase("savepoint_limit_bytes_per_transaction");out["savepoint_scope"]="bounded logical-operation position and name metadata; no copied page images";out["logical_undo"]=logical_journal->diagnostics();}
    return out;
}
#else
std::shared_ptr<Transaction> Storage::begin(const TransactionOptions& options) {
    auto& s=*impl_;auto effective=options;if(s.legacy_read_only)effective.read_only=true;auto tx=std::make_shared<Transaction>();
    {std::unique_lock<std::mutex> lock(s.registry_mutex);
     s.registry_cv.wait(lock,[&]{return !s.checkpoint_requested||s.failed.load();});
     if(s.failed)throw DbError("RECOVERY_REQUIRED","Storage requires recovery");
     tx->id=s.next_tx.fetch_add(1);tx->state=std::make_shared<Transaction::State>();tx->state->owner=&s;s.active[tx->id]=tx->state;}
    // registry is released before coordinator is acquired. A checkpoint sees
    // this registration and waits; no reverse registry/coordinator lock cycle.
    bool physical_registered=false;
    try{s.protocol_initialize(tx,effective);if(s.aries_mode){s.aries_manager->begin(tx->id,effective.read_only);physical_registered=true;}if(s.logical_mode)s.logical_journal->begin(tx->id);}catch(...){if(s.logical_mode&&physical_registered){try{s.aries_manager->abort(tx->id);}catch(...){s.aries_note_failure();}}s.finish(tx);throw;}
    return tx;
}
TransactionControl::StatementView Storage::refresh_read_view(const std::shared_ptr<Transaction>& tx,const std::vector<TransactionAccess>& accesses) {
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);if(std::any_of(accesses.begin(),accesses.end(),[](const TransactionAccess& access){return access.write;}))impl_->backup_write_guard();if(impl_->logical_mode){std::lock_guard<std::recursive_mutex> structure(impl_->logical_mutex);if(tx->state->logical_kind!="physical")impl_->logical_clear(tx);auto permit=impl_->protocols.lock();return impl_->protocols.before_statement(tx->state->protocol,accesses);}return impl_->protocol_refresh(tx,accesses);
}
void Storage::savepoint(const std::shared_ptr<Transaction>& tx,const std::string& name) {
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);if(impl_->logical_mode&&tx->state->logical_kind!="physical"){impl_->logical_journal->savepoint(tx->id,name);return;}impl_->protocol_savepoint(tx,name);
}
void Storage::rollback_to(const std::shared_ptr<Transaction>& tx,const std::string& name) {
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;
    if(tx->state->owner!=&s||!tx->state->active)throw DbError("NO_TRANSACTION","Inactive transaction/wrong storage owner");
    if(s.failed)throw DbError("RECOVERY_REQUIRED","Storage fail-stop; full rollback/restart required");
    if(s.logical_mode&&tx->state->logical_kind!="physical"){std::lock_guard<std::recursive_mutex> structure(s.logical_mutex);auto operations=s.logical_journal->after_savepoint(tx->id,name);try{for(const auto& op:operations)s.logical_inverse(tx,op);s.logical_journal->keep_savepoint(tx->id,name);tx->state->aborted=false;return;}catch(...){s.logical_fail();throw;}}
    s.protocol_rollback_to(tx,name);
}
void Storage::release_savepoint(const std::shared_ptr<Transaction>& tx,const std::string& name) {
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);if(impl_->logical_mode&&tx->state->logical_kind!="physical"){impl_->logical_journal->release_savepoint(tx->id,name);return;}impl_->protocol_release_savepoint(tx,name);
}
#endif
