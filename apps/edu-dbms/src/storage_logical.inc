// ARIES_LOGICAL experiment only. State mutex -> short structure mutex ->
// Journal/physical authority. Neither journal nor Manager calls back here.
void logical_clear(const std::shared_ptr<Transaction>& tx){auto& st=*tx->state;st.pages.clear();st.private_page_types.fill(0);st.new_pages.clear();st.descriptors.clear();st.descriptor_names.clear();st.dirty_descriptors.clear();st.ch10_layout_touched.clear();st.ch10_physical_cache.clear();st.moved_rows.clear();st.protocol_allocated_pages.clear();st.protocol_write_base_lsn.clear();}
void logical_fail(){failed=true;registry_cv.notify_all();}
void logical_crash(const char* phase){
    const char* pause=std::getenv("EDUDB_LOGICAL_PAUSE_AT");
    if(pause&&std::strcmp(pause,phase)==0){const char* ready=std::getenv("EDUDB_LOGICAL_PHASE_READY");if(!ready)throw DbError("ARGUMENT","Test pause requires an owned ready-file path");std::ofstream out(fs::u8path(ready),std::ios::binary);out<<json{{"phase",phase},{"status","PAUSED_FOR_OWNED_EXTERNAL_KILL"}}.dump();out.close();if(!out)throw DbError("IO_ERROR","Cannot publish test-only logical phase marker");for(unsigned i=0;i<2000;++i)std::this_thread::sleep_for(std::chrono::milliseconds(10));std::_Exit(87);}
    const char* value=std::getenv("EDUDB_LOGICAL_CRASH_AT");if(value&&std::strcmp(value,phase)==0)std::_Exit(86);
}
int logical_pk(const json& meta){
    if(meta.contains("cluster")||meta.contains("materialized_aux")||meta.value("organization",json{{"method","HEAP"}}).value("method",std::string("HEAP"))!="HEAP")throw DbError("LOGICAL_UNSUPPORTED","Logical mode requires independent HEAP tables");
    const auto& columns=meta.at("columns");if(columns.empty()||columns.size()>8)throw DbError("LOGICAL_SCHEMA_LIMIT","Logical mode requires1..8 scalar columns");int pk=-1;
    for(std::size_t i=0;i<columns.size();++i){const auto& c=columns[i];auto type=c.at("type").get<std::string>();if((type!="INTEGER"&&type!="REAL"&&type!="BOOLEAN"&&type!="VARCHAR")||c.value("unique",false))throw DbError("LOGICAL_UNSUPPORTED","Only INTEGER/REAL/BOOLEAN/VARCHAR and primary-key uniqueness are available");if(c.value("primary",false)){if(pk>=0||c.at("type")!="INTEGER")throw DbError("LOGICAL_UNSUPPORTED","Logical primary key must be one INTEGER column");pk=static_cast<int>(i);}}
    for(const auto& ix:meta.at("indexes")){
        const auto& names=ix.at("columns");if(names.size()!=1)throw DbError("LOGICAL_UNSUPPORTED","Composite indexes are unavailable in logical mode");
        if(ix.value("primary",false)){int candidate=-1;for(std::size_t i=0;i<columns.size();++i)if(columns[i].at("name")==names[0])candidate=static_cast<int>(i);if(candidate<0||columns[candidate].at("type")!="INTEGER"||(pk>=0&&pk!=candidate))throw DbError("LOGICAL_UNSUPPORTED","Logical mode requires one INTEGER primary key");pk=candidate;}
    }
    if(pk<0)throw DbError("LOGICAL_UNSUPPORTED","Logical mode requires an INTEGER primary key");
    if(meta.at("indexes").size()>2)throw DbError("LOGICAL_SCHEMA_LIMIT","At most primary plus one same-key BPLUS index");
    for(const auto& ix:meta.at("indexes")){auto method=ix.value("method",std::string("BPLUS"));auto options=ix.value("options",json::object());if(method!="BPLUS"||ix.at("columns")[0]!=columns[pk].at("name")||options.value("bstar",false)||options.value("dup_layout",std::string("entries"))!="entries"||ix.value("include",json::array()).size())throw DbError("LOGICAL_UNSUPPORTED","Only simple same-primary-key BPLUS indexes are available");}
    if(meta.dump().size()>4096)throw DbError("LOGICAL_SCHEMA_LIMIT","Logical table metadata exceeds4KiB");return pk;
}
int logical_validate(const std::shared_ptr<Transaction>& tx,const std::string& name,const json* row=nullptr){auto pair=descriptor(tx,name);auto& d=pair.second;int pk=logical_pk(d.at("meta"));if(d.dump().size()>6144||d.at("row_count").get<std::uint64_t>()>logical::Limits::table_rows)throw DbError("LOGICAL_SCHEMA_LIMIT","Logical descriptor/row bound exceeded");for(auto& ix:d["index_roots"]){if(ix.value("height",std::uint64_t(1))>16)throw DbError("LOGICAL_TREE_LIMIT","Logical BPLUS height limit16");}if(d["row_map"].value("height",std::uint64_t(1))>16)throw DbError("LOGICAL_TREE_LIMIT","Logical row-map height limit16");if(row){if(!row->is_array()||row->size()!=d["meta"]["columns"].size()||row->dump().size()>logical::Limits::row_bytes||!(*row)[pk].is_number_integer())throw DbError("LOGICAL_ROW_LIMIT","Logical rows require an integer key and at most1024 JSON bytes");try{record::encode(record::schema(d.at("meta")),1,*row);}catch(const record::Error& e){throw DbError(e.code,e.what());}}return pk;}
std::uint64_t logical_deleted_reservation(const std::string& name){std::uint64_t n=0;for(auto id:logical_journal->loser_transactions())for(const auto& op:logical_journal->pending(id))if(op.description.at("table")==name&&!op.description.at("before").is_null()&&op.description.at("after").is_null())++n;return n;}
void logical_allocation_check(const std::shared_ptr<Transaction>& tx){
    auto& st=*tx->state;auto reserved=logical_journal?logical_journal->remaining_pages():0;reserved+=logical_provisional_pages;
    if(st.logical_compensating){if(reserved<logical::Limits::new_pages_per_attempt)throw DbError("LOGICAL_PAGE_LIMIT","Compensation has no held allocation allowance");reserved-=logical::Limits::new_pages_per_attempt;}
    if(reserved>logical::Limits::file_pages||next_page.load()>=logical::Limits::file_pages-reserved)throw DbError("LOGICAL_PAGE_LIMIT","Physical allocation would consume reserved logical UNDO pages");
    if(st.logical_staging&&next_page.load()-st.logical_allocation_start>=logical::Limits::new_pages_per_attempt)throw DbError("LOGICAL_PAGE_LIMIT","One operation exceeds64 new pages");
}
void logical_stage_page(const std::shared_ptr<Transaction>& tx,PageId id,Page p){auto& st=*tx->state;if(!st.logical_staging)throw DbError("LOGICAL_INTERNAL","No logical page workspace");if(id>=logical::Limits::file_pages||(!st.pages.count(id)&&st.pages.size()>=logical::Limits::file_pages))throw DbError("LOGICAL_PAGE_LIMIT","Logical operation workset exceeds1024 unique pages");seal(p,0);st.pages[id]=std::move(p);}
void logical_prepare_ddl(const std::shared_ptr<Transaction>& tx){
    if(!logical_mode)return;std::lock_guard<std::recursive_mutex> structure(logical_mutex);auto& st=*tx->state;if(st.protocol->options.read_only)throw DbError("READ_ONLY_TRANSACTION","Logical read-only transaction cannot perform physical DDL");if(st.logical_kind=="logical")throw DbError("LOGICAL_MIXED_DDL","Logical DML and physical DDL cannot share a transaction");st.logical_kind="physical";
}
void logical_validate_workspace(const std::shared_ptr<Transaction>& tx){if(tx->state->pages.size()>logical::Limits::file_pages)throw DbError("LOGICAL_PAGE_LIMIT","Unique page workset bound");for(const auto& d:tx->state->descriptors)if(d.second.dump().size()>6144)throw DbError("LOGICAL_SCHEMA_LIMIT","Descriptor expansion would invalidate inverse bound");}
std::uint64_t logical_publish(const std::shared_ptr<Transaction>& tx,std::uint64_t mini,bool compensation){
    IoOwnerScope io_owner(directory);auto backup_gate=compensation?std::optional<recovery::FencePermit>{}:backup_commit_gate(true);
    auto& st=*tx->state;st.logical_staging=false;st.logical_inner=mini;bool started=false,committed=false;
    try{
        if(compensation)aries_manager->begin_compensation(mini,logical::Limits::attempt_credit,logical::Limits::update_bytes);else aries_manager->begin(mini);
        started=true;std::vector<PageId> ids;for(const auto& p:st.pages)ids.push_back(p.first);std::sort(ids.begin(),ids.end());
        for(auto id:ids){auto p=st.pages.at(id);seal(p,0);aries_manager->update(mini,id,p);st.aries_pages[id]=get16(p.data()+6);logical_crash(compensation?"during_inverse_update":"during_forward_update");}
        auto result=aries_manager->commit(mini);committed=true;st.wal_bytes+=result.at("wal_bytes").get<std::uint64_t>();durable_lsn=aries_manager->diagnostics().at("flushed_lsn").get<std::uint64_t>();st.logical_inner=0;
        if(!ids.empty())logical_crash(compensation?"after_inverse_mini_commit":"after_forward_mini_commit");
        auto used=result.at("wal_bytes").get<std::uint64_t>();if(used>logical::Limits::update_bytes+160)throw DbError("LOGICAL_INTERNAL","Physical mini-transaction exceeded final-page WAL proof");return used;
    }catch(...){
        st.logical_inner=0;aries_note_failure();if(committed)logical_fail();
        if(started&&!failed){try{aries_manager->abort(mini);}catch(...){logical_fail();}}
        throw;
    }
}
RowId logical_mutate(const std::shared_ptr<Transaction>& tx,const std::string& name,const RowId& requested,const json& after,const std::string& kind){
    IoOwnerScope io_owner(directory);std::lock_guard<std::recursive_mutex> structure(logical_mutex);auto& st=*tx->state;if(st.protocol->options.read_only)throw DbError("READ_ONLY_TRANSACTION","Logical read-only transaction cannot modify a row");if(st.logical_kind=="physical")throw DbError("LOGICAL_MIXED_DDL","Physical DDL transaction cannot perform logical DML");st.logical_kind="logical";logical_clear(tx);int pk=logical_validate(tx,name,kind=="DELETE"?nullptr:&after);auto pair=descriptor(tx,name);json before=nullptr;RowId actual=requested;
    if(kind!="INSERT"){before=hydrate(tx,pair.first,stored_at(tx,pair.first,pair.second,requested.logical_id,&actual));if(kind=="UPDATE"&&before[pk]!=after[pk])throw DbError("LOGICAL_PRIMARY_UPDATE","Primary key UPDATE is unavailable in logical mode");if(kind=="UPDATE"&&before==after)return actual;}
    if(kind=="INSERT"&&pair.second.at("row_count").get<std::uint64_t>()+logical_deleted_reservation(name)>=logical::Limits::table_rows)throw DbError("LOGICAL_ROW_LIMIT","128-row capacity includes outstanding DELETE inverse slots");
    logical_journal->admit(tx->id,logical::Limits::file_pages,*aries_manager);logical_provisional_pages=logical::Limits::operation_pages;
    const auto start=next_page.load();st.logical_allocation_start=start;st.logical_staging=true;bool recorded=false;
    try{
        if(kind=="INSERT")actual=v3_insert(tx,name,after);else if(kind=="UPDATE")actual=v3_update(tx,name,RowId{0,0,1,actual.logical_id},after);else v3_erase(tx,name,RowId{0,0,1,actual.logical_id});
        logical_validate_workspace(tx);flush_descriptors(tx);logical_validate_workspace(tx);auto mini=next_tx.fetch_add(1);auto key=(kind=="INSERT"?after:before).at(pk);
        auto operation=logical_journal->start_operation(tx->id,mini,{{"table",name},{"key",key},{"lid",actual.logical_id},{"before",before},{"after",kind=="DELETE"?json(nullptr):after}},start,next_page.load());recorded=true;logical_provisional_pages=0;
        logical_publish(tx,mini,false);logical_journal->complete_operation(operation);logical_clear(tx);logical_journal->synchronize_reservations(*aries_manager);return actual;
    }catch(...){
        st.logical_staging=false;logical_provisional_pages=0;if(!recorded)next_page=start;
        if(logical_journal->failed())logical_fail();aries_note_failure();logical_clear(tx);st.aborted=true;
        if(!failed){try{logical_journal->synchronize_reservations(*aries_manager);}catch(...){logical_fail();}}
        throw;
    }
}
std::optional<StoredRow> logical_lookup(const std::shared_ptr<Transaction>& tx,const std::string& name,const json& key){
    auto pair=descriptor(tx,name);auto& d=pair.second;int pk=logical_validate(tx,name);json* selected=nullptr;for(auto& ix:d["index_roots"])if(ix.value("unique",false)&&ix.at("column_numbers")==json::array({pk})){selected=&ix;break;}if(!selected)throw DbError("LOGICAL_INTERNAL","Primary BPLUS lookup is missing");access::Index index(pager(tx,pair.first),*selected);auto matches=index.find(json::array({key}));if(matches.empty())return {};if(matches.size()!=1)throw DbError("CORRUPT_INDEX","Logical primary key duplicates");RowId rid;auto row=hydrate(tx,pair.first,stored_at(tx,pair.first,d,matches[0].id,&rid));return StoredRow{rid,row};
}
void logical_inverse(const std::shared_ptr<Transaction>& tx,const logical::Operation& operation){
    if(!operation.attempts.empty()&&aries_manager->transaction_committed(operation.attempts.back().tx)){logical_journal->compensated(operation.id);logical_journal->synchronize_reservations(*aries_manager);return;}
    if(operation.attempts.size()>=logical::Limits::recovery_attempts)throw DbError("LOGICAL_RECOVERY_RETRY_LIMIT","Two inverse attempts exhausted; database remains fail-closed");
    auto& st=*tx->state;logical_clear(tx);const auto& description=operation.description;auto name=description.at("table").get<std::string>();auto current=logical_lookup(tx,name,description.at("key"));auto lid=description.at("lid").get<std::uint64_t>();const auto& before=description.at("before");const auto& after=description.at("after");try{if(!before.is_null())logical_validate(tx,name,&before);if(!after.is_null())logical_validate(tx,name,&after);}catch(const DbError& e){throw DbError("CORRUPT_LOGICAL_WAL",std::string("Logical undo row violates the stable schema: ")+e.what());}
    const auto start=next_page.load();st.logical_allocation_start=start;st.logical_staging=true;st.logical_compensating=true;bool recorded=false;
    try{
        if(before.is_null()){if(current){if(current->id.logical_id!=lid||current->values!=after)throw DbError("LOGICAL_UNDO_CONFLICT","INSERT inverse key/LID/after state changed");v3_erase(tx,name,RowId{0,0,1,lid});}}
        else if(!current){if(!after.is_null())throw DbError("LOGICAL_UNDO_CONFLICT","UPDATE inverse lost its stable key");v3_insert(tx,name,before,lid);}
        else {if(current->id.logical_id!=lid)throw DbError("LOGICAL_UNDO_CONFLICT","Inverse key belongs to a different LID");if(current->values!=before){if(after.is_null()||current->values!=after)throw DbError("LOGICAL_UNDO_CONFLICT","Inverse does not match before/after");v3_update(tx,name,RowId{0,0,1,lid},before);}}
        logical_validate_workspace(tx);flush_descriptors(tx);logical_validate_workspace(tx);auto mini=next_tx.fetch_add(1);logical_journal->start_undo(operation.id,mini,start,next_page.load());recorded=true;logical_publish(tx,mini,true);logical_journal->compensated(operation.id);st.logical_compensating=false;logical_clear(tx);logical_journal->synchronize_reservations(*aries_manager);
    }catch(...){st.logical_staging=false;st.logical_compensating=false;if(!recorded)next_page=start;logical_clear(tx);logical_fail();throw;}
}
void logical_abort(const std::shared_ptr<Transaction>& tx){
    IoOwnerScope io_owner(directory);
    std::lock_guard<std::recursive_mutex> structure(logical_mutex);auto& st=*tx->state;
    try{
        logical_clear(tx);
        if(st.logical_kind=="physical"){aries_manager->abort(tx->id);logical_journal->finish_abort(tx->id);return;}
        logical_journal->abort_begin(tx->id);for(const auto& op:logical_journal->pending(tx->id))logical_inverse(tx,op);logical_journal->finish_abort(tx->id);logical_journal->synchronize_reservations(*aries_manager);aries_manager->abort(tx->id);
    }catch(...){logical_fail();throw;}
}
void logical_commit(const std::shared_ptr<Transaction>& tx){
    IoOwnerScope io_owner(directory);
    std::lock_guard<std::recursive_mutex> structure(logical_mutex);
    if(tx->state->logical_kind=="physical"){
        bool physical_decision=false;
        try{flush_descriptors(tx);auto backup_gate=backup_commit_gate(true);aries_commit(tx);physical_decision=true;logical_journal->commit(tx->id);return;}
        catch(...){
            // Physical DDL COMMIT is irrevocable even if journal/accounting
            // fails afterwards. Never append ABORT for an accepted decision.
            if(physical_decision||!tx->state->active||failed){logical_fail();throw;}
            try{logical_abort(tx);}catch(...){logical_fail();}finish(tx);throw;
        }
    }
    auto backup_gate=backup_commit_gate(!tx->state->aries_pages.empty()||(tx->state->protocol&&!tx->state->protocol->write_tables.empty()));
    std::lock_guard<std::mutex> wal_lock(wal_mutex);bool decision=false;
    try{auto permit=protocol_prepare_commit(tx);aries_manager->commit(tx->id);logical_journal->commit(tx->id);decision=true;logical_journal->synchronize_reservations(*aries_manager);protocol_complete_commit(permit);++commits;finish(tx);}
    catch(...){if(decision||logical_journal->failed())logical_fail();aries_note_failure();if(!failed){try{logical_abort(tx);}catch(...){logical_fail();}}finish(tx);throw;}
}
void logical_setup(){
    if(!logical_journal)throw DbError("LOGICAL_INTERNAL","Journal analysis must precede physical recovery");next_tx=std::max(next_tx.load(),logical_journal->max_id()+1);next_page=std::max(next_page.load(),logical_journal->allocation_high_water());if(next_page.load()>logical::Limits::file_pages)throw DbError("LOGICAL_PAGE_LIMIT","Logical physical high-water exceeds1024-page limit");logical_journal->synchronize_reservations(*aries_manager);
    // Manager construction/physical recovery is complete. These callbacks use
    // assigned authorities and run outside both Manager and Journal mutexes.
    std::lock_guard<std::recursive_mutex> structure(logical_mutex);
    for(auto id:logical_journal->loser_transactions()){
        auto tx=std::make_shared<Transaction>();tx->id=id;tx->state=std::make_shared<Transaction::State>();tx->state->owner=this;tx->state->logical_kind="logical";TransactionOptions options;options.record_locks=true;protocol_initialize(tx,options);
        try{logical_journal->abort_begin(id);for(const auto& op:logical_journal->pending(id))logical_inverse(tx,op);logical_journal->finish_abort(id);logical_journal->synchronize_reservations(*aries_manager);protocol_finish(tx);tx->state->active=false;}catch(...){logical_fail();throw;}
    }
    // Logical recovery produced new committed mini-transactions AFTER the
    // physical constructor's redo/undo phase. Flush their final pages and END
    // before admitting sessions so the next recovery is byte-idempotent.
    aries_manager->recovery_barrier();durable_lsn=aries_manager->diagnostics().at("flushed_lsn").get<std::uint64_t>();
}
std::unique_ptr<RowCursor> logical_scan(Storage& store,const std::shared_ptr<Transaction>& tx,const std::string& name,const ScanSpec& spec){
    std::lock_guard<std::recursive_mutex> structure(logical_mutex);auto& st=*tx->state;if(st.logical_kind!="physical"&&!st.logical_staging)logical_clear(tx);st.logical_scanning=true;
    auto values=std::make_shared<std::vector<StoredRow>>();
    try{auto input=store.scan(tx,name,spec);StoredRow row;while(input->next(row)){if(values->size()>=logical::Limits::table_rows)throw DbError("LOGICAL_ROW_LIMIT","Logical scan row bound exceeded");values->push_back(std::move(row));}st.logical_scanning=false;}catch(...){st.logical_scanning=false;throw;}
    auto at=std::make_shared<std::size_t>(0);return std::make_unique<FunctionCursor>([values,at](StoredRow& row){if(*at>=values->size())return false;row=values->at((*at)++);return true;});
}
json logical_stats(const std::shared_ptr<Transaction>& tx){auto& st=*tx->state;auto points=logical_journal->savepoint_stats(tx->id);return {{"scope","transaction"},{"recovery_mode","ARIES_LOGICAL"},{"active",st.active},{"private_pages",st.pages.size()},{"private_bytes",st.pages.size()*PAGE_SIZE},{"private_dirty_pages",st.pages.size()},{"dirty_pages",st.aries_pages.size()},{"dirty_pages_scope","unique pages touched by this outer transaction's committed physical mini-transactions; may overlap other users, not current dirty-frame ownership"},{"wal_bytes",st.wal_bytes},{"wal_bytes_scope","this outer transaction's completed physical mini-transaction records; logical journal is separately exposed"},{"heap_page_reads",st.heap_reads},{"index_page_reads",st.index_reads},{"row_fetches",st.rows_fetched},{"cover_hits",st.cover_hits},{"bitmap_words",st.bitmap_words},{"savepoint_bytes",points.at("bytes")},{"savepoint_count",points.at("count")},{"savepoint_scope",points.at("scope")},{"protocol","STRICT_2PL"},{"read_view","key/range locks plus a bounded statement materialization under the structure mutex; not MVCC/physical-CSN snapshot"}};}
