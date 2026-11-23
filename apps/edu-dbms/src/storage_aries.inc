// Included inside Storage::Impl. This is the sole ARIES page authority.
// Lock order: Engine LM commit permit -> State -> Storage WAL -> coordinator
// -> ARIES authority-buffer mutex -> DataDevice. Manager never calls outward
// into State, coordinator, lock manager or the legacy buffer.
void aries_setup(){
    aries::PageCodec codec;
    codec.empty=[](PageId id){auto p=empty_page(id,TYPE_RETIRED);seal(p,0);return p;};
    codec.valid=[](const Page& p,PageId id){return valid_page(p,id);};
    codec.lsn=[](const Page& p){return get64(p.data()+16);};
    codec.stamp=[](Page& p,std::uint64_t lsn){seal(p,lsn);};
    aries_options.io_fault=[this](const char* phase){if(std::strcmp(phase,"after_commit_force")==0)inject("aries_after_commit_force_once");};
    aries_manager=std::make_unique<aries::Manager>(directory.u8string(),*data,db_uuid,codec,aries_options);
    // Recovery may extend data for committed/aborted formerly-unflushed pages.
    next_page=std::max(next_page.load(),(data->size()+PAGE_SIZE-1)/PAGE_SIZE);
    auto d=aries_manager->diagnostics();next_tx=std::max(next_tx.load(),d.at("max_tx_id").get<std::uint64_t>()+1);
    durable_lsn=d.at("flushed_lsn").get<std::uint64_t>();checkpoint_lsn=d.at("checkpoint_end").get<std::uint64_t>();
}
void aries_note_failure(){
    if(aries_manager&&aries_manager->diagnostics().value("recovery_required",false)){failed=true;registry_cv.notify_all();}
}
void aries_alias_restored(const std::shared_ptr<Transaction>& tx,const std::map<PageId,std::uint16_t>& still_private,bool transaction_active=true){
    // A CLR changes physical pageLSN while restoring the old committed contents.
    // Preserve that version's CSN/creator instead of inventing a CSN0 version.
    for(const auto& base:tx->state->protocol_write_base_lsn)if(!still_private.count(base.first)){
        try{auto page=aries_manager->read(transaction_active?tx->id:0,base.first);version_graph.restore_alias(base.first,base.second,get64(page.data()+16),get16(page.data()+6));}
        catch(...){aries_note_failure();failed=true;throw;}
    }
}
void aries_abort(const std::shared_ptr<Transaction>& tx){
    auto guard=protocols.lock();aries_manager->abort(tx->id);aries_alias_restored(tx,{},false);
}
void aries_write_page(const std::shared_ptr<Transaction>& tx,PageId id,Page p){
    if(tx->state->protocol->options.read_only)throw DbError("READ_ONLY_TRANSACTION","ARIES read-only transaction cannot modify a page");
    const auto type=get16(p.data()+6);
    if(type>=tx->state->private_page_types.size())throw DbError("INVALID_PAGE","ARIES page has invalid type");
    protocol_record_private_write(tx,id);
    // Storage callers pass modified copies whose old CRC is no longer valid.
    // Manager stamps the actual UPDATE LSN and reseals after it logs the image.
    seal(p,0);
    try{aries_manager->update(tx->id,id,p);tx->state->wal_bytes=aries_manager->transaction_wal_bytes(tx->id);}
    catch(...){aries_note_failure();tx->state->aborted=true;throw;}
    tx->state->aries_pages[id]=type;
    // No duplicate full-page image is retained in State.pages or BufferPool.
}
void aries_commit(const std::shared_ptr<Transaction>& tx){
    std::lock_guard<std::mutex> lock(wal_mutex);
    if(failed){finish(tx);throw DbError("RECOVERY_REQUIRED","Earlier ARIES I/O failure requires recovery");}
    bool manager_commit_returned=false;
    try{
        // Only Strict2PL can reach here. Manager pages already contain this
        // transaction's UPDATE LSNs; old private-base comparison is inapplicable.
        auto pending=protocol_prepare_commit(tx);
        auto result=aries_manager->commit(tx->id);manager_commit_returned=true;
        inject("aries_after_manager_commit_once");
        tx->state->wal_bytes=result.at("wal_bytes").get<std::uint64_t>();
        tx->state->aries_last_dirty=result.at("dirty_pages").get<std::uint64_t>();
        durable_lsn=aries_manager->diagnostics().at("flushed_lsn").get<std::uint64_t>();
        protocol_complete_commit(pending);++commits;finish(tx);
    }catch(...){
        // A returned Manager COMMIT is irrevocable even if later accounting,
        // diagnostics or protocol completion throws. Restart resolves outcome.
        if(manager_commit_returned){failed=true;registry_cv.notify_all();}
        aries_note_failure();
        // If no COMMIT was forced and Manager remains healthy, compensate now.
        // A failed persistence boundary is UNKNOWN; fail-stop and restart decide.
        if(!failed){try{aries_abort(tx);}catch(...){aries_note_failure();}}
        finish(tx);throw;
    }
}
json aries_checkpoint(){
    if(failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");
    try{
        auto result=aries_manager->checkpoint_fuzzy();
        checkpoint_lsn=result.at("end_lsn").get<std::uint64_t>();
        durable_lsn=aries_manager->diagnostics().at("flushed_lsn").get<std::uint64_t>();
        // This manifest records allocation/mode metadata. The separately synced
        // ARIES master (not this file's checkpoint_lsn) selects analysis restart.
        atomic_manifest(directory,control(generation,checkpoint_lsn));return result;
    }catch(...){aries_note_failure();throw;}
}
json aries_buffer_configuration(const json& request){
    if(!request.is_object())throw DbError("INVALID_CONFIG","ARIES buffer configuration must be an object");
    for(auto it=request.begin();it!=request.end();++it){
        if(it.key()=="policy"&&it.value()=="LRU")continue;
        if((it.key()=="read_ahead_pages"||it.key()=="wait_ms")&&it.value().is_number_integer()&&it.value().get<std::int64_t>()==0)continue;
        throw DbError("ARIES_BUFFER_POLICY","ARIES authority buffer currently supports only LRU, read_ahead_pages=0 and wait_ms=0");
    }
    return {{"policy","LRU"},{"read_ahead_pages",0},{"wait_ms",0},{"authority","ARIES manager; legacy buffer is not constructed"}};
}
json aries_transaction_stats(const std::shared_ptr<Transaction>& tx){
    auto& state=*tx->state;
    auto dirty=state.active?aries_manager->transaction_metrics(tx->id).at("dirty_pages").get<std::uint64_t>():state.aries_last_dirty;
    return {{"scope","transaction"},{"recovery_mode","ARIES_PAGE"},{"active",state.active},
            {"private_pages",0},{"private_dirty_pages",0},{"private_bytes",0},
            {"dirty_pages",dirty},
            {"pending_logical_pages",state.active?state.aries_pages.size():state.last_pages},
            {"dirty_pages_scope","This transaction's actual unflushed authority-buffer pages; post-commit value is sampled at COMMIT return"},
            {"wal_bytes",state.wal_bytes},{"heap_page_reads",state.heap_reads},{"index_page_reads",state.index_reads},
            {"row_fetches",state.rows_fetched},{"cover_hits",state.cover_hits},{"bitmap_words",state.bitmap_words},
            {"savepoint_bytes",state.protocol_savepoint_bytes},{"savepoint_count",state.protocol_savepoints.size()},
            {"protocol","STRICT_2PL"},{"private_image_budget_applicable",false}};
}
json aries_diagnostics(){
    auto out=aries_manager->diagnostics();std::size_t count;
    out["recovery_required"]=failed.load()||out.value("recovery_required",false);
    {std::lock_guard<std::mutex> lock(registry_mutex);count=active.size();}
    out["recovery_mode"]=logical_mode?"ARIES_LOGICAL":"ARIES_PAGE";if(logical_mode){out["physical_authority_mode"]=out.at("mode");out["mode"]="ARIES_LOGICAL";out["mode_scope"]="outer logical user transactions plus ARIES_PAGE physical system mini-transactions";out["logical"]=logical_journal->diagnostics();out["limits"]="Explicit bounded ARIES_LOGICAL experiment;1024 pages/128 rows per table/1KiB row/3 active operations/two inverse attempts; not unbounded recovery";}out["format_version"]=legacy_read_only?3:4;out["page_format"]=3;out["read_only"]=legacy_read_only;out["legacy_source"]=legacy_snapshot?legacy_snapshot->source().u8string():std::string();out["recovery_scope"]=legacy_read_only?"owned snapshot copy only; source locked and unchanged":"database directory";out["active_transactions"]=count;
    out["next_page_id"]=next_page.load();out["buffer_pool_frames"]=frame_limit;
    out["legacy_buffer_constructed"]=bool(buffer);out["buffer_policy"]="LRU";
    out["authority_frames"]=out.at("buffer");
    out["buffer"]={{"frames",frame_limit},{"resident",out.at("authority_frames").size()},{"dirty",out.at("dirty_page_table").size()},{"policy","LRU"},{"authority","ARIES manager"}};
    out["buffer_read_ahead_pages"]=0;out["buffer_wait_ms"]=0;
    out["buffer_authority"]="ARIES manager only";out["commits"]=commits.load();out["rollbacks"]=rollbacks.load();
    out["transactions"]=protocol_diagnostics();out["io"]=io_snapshot(directory);
    out["wal"]={{"mode",logical_mode?"ARIES_LOGICAL_PHYSICAL_MINI":"ARIES_PAGE"},{"bytes",out.at("wal_bytes")},{"last_lsn",out.at("last_lsn")},{"durable_lsn",out.at("flushed_lsn")},{"checkpoint_begin",out.at("checkpoint_begin")},{"checkpoint_end",out.at("checkpoint_end")},{"forces",out.at("wal_forces")}};
    if(logical_mode)out["wal"]["logical"]=logical_journal->diagnostics();
    out["media"]=data_layout;out["retired_pages"]=retired.load();
    out["stale_sort_files_removed"]=stale_sort_files_removed;out["stale_query_files_removed"]=stale_query_files_removed;
    return backup_diagnostics(out);
}
