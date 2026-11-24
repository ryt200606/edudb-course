// Member functions of Storage::Impl. The on-disk unit is still one transaction.
std::uint64_t append_private_transaction(const std::shared_ptr<Transaction>& tx,
                                         const std::vector<PageId>& ids) {
    append_record(1,next_lsn++,tx,{});
    std::uint32_t digest=0;
    for(auto id:ids) {
        auto& image=tx->state->pages.at(id);
        const auto lsn=next_lsn++;
        seal(image,lsn);
        digest=digest_page(digest,image);
        std::vector<unsigned char> payload(PAGE_SIZE+8);
        put64(payload.data(),id);
        std::copy(image.begin(),image.end(),payload.begin()+8);
        append_record(2,lsn,tx,payload);
    }
    std::vector<unsigned char> end(8);
    put32(end.data(),static_cast<std::uint32_t>(ids.size()));
    put32(end.data()+4,digest);
    const auto lsn=next_lsn++;
    append_record(3,lsn,tx,end);
    return lsn;
}

// Each submitting thread retains its transaction mutex until its ticket is
// returned. The condition variable hands exclusive use of its pages and I/O
// scope to this leader and synchronizes their return to the caller.
void write_group(GroupWriter** batch,std::size_t count) {
    auto fence=backup_commit_gate(true);
    std::lock_guard<std::mutex> wal_guard(wal_mutex);
    if(failed)throw DbError("RECOVERY_REQUIRED","Earlier writer failed before batch admission");
    auto protocol_guard=protocols.lock();
    std::size_t at=0;
    while(at<count) {
        struct Prepared {
            GroupWriter* ticket;
            ProtocolCommit permit;
            std::vector<PageId> ids;
        };
        std::vector<Prepared> prepared;
        std::set<PageId> covered;
        const bool history_needed=protocols.has_snapshot_readers();
        while(at<count) {
            auto* ticket=batch[at];
            auto& tx=ticket->tx;
            std::vector<PageId> ids;
            bool overlap=false;
            for(const auto& page:tx->state->pages) {
                ids.push_back(page.first);
                if(covered.count(page.first))overlap=true;
            }
            // Publish earlier writers before validating a conflicting base.
            // With snapshot readers, also reserve version history one tx at a time.
            if(!prepared.empty() && (overlap||history_needed))break;
            ++at;
            IoRequestBinding request(ticket->request);
            try {
                auto permit=protocol_prepare_commit(tx);
                if(prepared.size()>std::numeric_limits<std::uint64_t>::max()-permit.permit.csn)
                    throw DbError("TRANSACTION_LIMIT","Commit sequence exhausted");
                permit.permit.csn+=prepared.size();
                std::sort(ids.begin(),ids.end());
                covered.insert(ids.begin(),ids.end());
                prepared.push_back({ticket,std::move(permit),std::move(ids)});
            } catch(const DbError&) {
                ticket->error=std::current_exception();
                finish(tx);
            }
        }
        if(prepared.empty())continue;
        std::optional<buffer::BufferPool::Publication> publication;
        try { publication.emplace(buffer->reserve_publication(5000)); }
        catch(const buffer::Error& e){throw DbError(e.code,e.what());}
        try {
            std::uint64_t last=0;
            for(auto& item:prepared) {
                IoRequestBinding request(item.ticket->request);
                last=append_private_transaction(item.ticket->tx,item.ids);
            }
            {
                IoRequestBinding issuer(prepared.front().ticket->request);
                crash_at("wal_before_sync");
                inject("wal_sync_once");
                wal->sync();
                durable_lsn=last;
                ++wal_forces;
                ++group_syncs;
                group_transactions+=prepared.size();
                group_peak=std::max<std::uint64_t>(group_peak,prepared.size());
                crash_at("after_wal_sync");
            }
            for(auto& item:prepared) {
                IoRequestBinding request(item.ticket->request);
                auto& tx=item.ticket->tx;
                protocol_publish_versions(tx,item.permit);
                for(auto id:item.ids)publication->publish(id,tx->state->pages.at(id));
                protocol_complete_commit(item.permit);
                ++commits;
                finish(tx);
            }
            publication->finish();
            if(const char* fault_point=std::getenv("EDUDB_CRASH_AT"))
                if(std::strcmp(fault_point,"after_page_flush")==0)buffer->flush_one();
        } catch(...) {
            failed=true;
            throw;
        }
    }
}

void group_commit(const std::shared_ptr<Transaction>& tx) {
    GroupWriter mine{tx,IoRequestBinding::current(),nullptr,false};
    std::unique_lock<std::mutex> lock(group_mutex);
    group_changed.wait(lock,[&]{return group_queue.size()<group_size;});
    group_queue.push_back(&mine);
    group_changed.notify_all();
    group_changed.wait(lock,[&]{return mine.done ||
        (!group_running && !group_queue.empty() && group_queue.front()==&mine);});
    if(!mine.done) {
        group_running=true;
        group_changed.wait_for(lock,std::chrono::microseconds(group_wait_us),
                              [&]{return group_queue.size()>=group_size;});
        std::array<GroupWriter*,16> batch{};
        auto count=group_queue.size();
        for(std::size_t i=0;i<count;++i){batch[i]=group_queue.front();group_queue.pop_front();}
        lock.unlock();
        group_changed.notify_all();
        try { write_group(batch.data(),count); }
        catch(...) {
            const auto error=std::current_exception();
            for(std::size_t i=0;i<count;++i) {
                auto* ticket=batch[i];
                if(ticket->tx->state->active){
                    ticket->error=error;
                    finish(ticket->tx);
                }
            }
        }
        lock.lock();
        for(std::size_t i=0;i<count;++i)batch[i]->done=true;
        group_running=false;
        group_changed.notify_all();
    }
    if(mine.error)std::rethrow_exception(mine.error);
}
