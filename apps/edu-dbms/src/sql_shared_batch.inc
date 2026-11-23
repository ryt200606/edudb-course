// Public API owns its transaction; no recursion through Engine::execute and no
// possibility of rolling back a transaction that belonged to the caller.
json Engine::execute_shared_batch(const std::string& id,const std::vector<std::string>& queries){
    IoScope io_scope("shared_select_batch");const auto started=Clock::now();
    std::shared_ptr<Impl::Session> session;std::unique_lock<std::mutex> guard;
    bool owned=false;std::size_t at=0;std::string phase="validation";json result=blank_result();
    json shared_metrics={{"base_scan_opens",0},{"base_rows",0},{"base_scan_io",json::object()},{"spool_reader_opens",0},{"spool_rows_read",0},{"scope","one explicit same-table read-only transaction; materialized CRC spool reuse, not simultaneous pipelined sharing"}};
    try{
        session=impl_->get(id);guard=std::unique_lock<std::mutex>(session->mutex);
        if(session->closed)fail("SESSION_NOT_FOUND","Session has been closed");
        if(session->tx)fail("TRANSACTION_ACTIVE","Shared batch requires an idle session and owns its read-only transaction");
        if(session->aborted)fail("TRANSACTION_ABORTED","Execute ROLLBACK before shared batch");
        if(queries.empty()||queries.size()>16)fail("BATCH_LIMIT","Shared batch requires1..16 queries");
        std::size_t bytes=0,nodes=0;std::vector<Statement> statements;std::string table;
        for(at=0;at<queries.size();++at){
            if(queries[at].size()>256u*1024u-bytes)fail("BATCH_LIMIT","Shared batch SQL text exceeds256 KiB total");
            bytes+=queries[at].size();
            auto st=Parser(queries[at]).parse();
            if(st.input_ast_nodes>250000-nodes)fail("BATCH_LIMIT","Shared batch exceeds250000 cumulative input AST nodes");
            nodes+=st.input_ast_nodes;
            if(st.kind!="SELECT"||st.explain||!st.set_operation.empty()||st.sources.size()!=1||!st.join_hint.empty()||st.on||st.query_definition)
                fail("BATCH_QUERY_UNSUPPORTED","Shared batch accepts ordinary single-table SELECT only; no compound, join, EXPLAIN or subqueries");
            if(!st.sources[0].index_hint.empty())fail("BATCH_QUERY_UNSUPPORTED","Explicit index hints are incompatible with shared base scanning");
            if(st.read_tables.size()!=1||st.read_tables[0]!=st.sources[0].table)fail("BATCH_QUERY_UNSUPPORTED","Shared batch cannot read a nested query");
            auto check=[&](const E& e){if(subquery_expression(e))fail("BATCH_QUERY_UNSUPPORTED","Shared batch subqueries are unsupported");};
            check(st.where);check(st.having);for(const auto& i:st.items)check(i.value);for(const auto& e:st.group)check(e);for(const auto& o:st.order)check(o.value);
            if(table.empty())table=st.sources[0].table;else if(table!=st.sources[0].table)fail("BATCH_QUERY_UNSUPPORTED","Every shared batch SELECT must use the same table");
            statements.push_back(std::move(st));
        }
        auto config=session->next_transaction;config["record_locks"]=session->record_locks;config["read_only"]=true;
        auto options=impl_->transaction_options(config);
        if(options.protocol!=TransactionProtocol::Strict2PL||options.record_locks)fail("BATCH_TRANSACTION_UNSUPPORTED","Shared batch requires STRICT_2PL with TABLE locking");
        if(impl_->storage.diagnostics().value("recovery_mode",std::string())=="ARIES_LOGICAL")fail("BATCH_TRANSACTION_UNSUPPORTED","Shared batch is not enabled for ARIES_LOGICAL recovery");
        impl_->start_transaction(*session,{{"read_only",true}});owned=true;session->explicit_tx=false;
        QueryBudget budget(impl_->query_limits);budget.cancellation=impl_->locks.cancellation_token(session->tx->id);
        QueryBudgetScope budget_scope(budget);PageProgressScope page_scope(query_page_progress(budget));
        impl_->before_statement(*session,statements[0]);impl_->acquire(*session,"catalog",false);impl_->acquire(*session,"table:"+table,false);
        auto metadata=impl_->table(*session,table,false);
        if(metadata.contains("materialized_view")||metadata.contains("materialized_aux"))fail("BATCH_QUERY_UNSUPPORTED","Shared batch source must be a base table");
        SharedScanState shared;shared.table=table;shared.tx=session->tx->id;SharedScanScope scope(shared);
        phase="preflight";
        for(at=0;at<statements.size();++at){auto st=statements[at];st.explain=true;st.analyze=false;impl_->before_statement(*session,st);impl_->select(*session,st,&budget);}
        auto workspace=impl_->storage.query_workspace();QuerySpillStore spool(workspace.first,workspace.second,std::min<std::size_t>(512u*1024u*1024u,impl_->query_limits.materialized_bytes));shared.store=&spool;
        phase="base_scan";at=0;
        if(shared.needs_run){
            auto before=impl_->storage.transaction_stats(session->tx);auto writer=spool.create();ScanSpec spec;spec.mode="TABLE";spec.lazy_blobs=true;
            auto cursor=impl_->storage.scan(session->tx,table,spec);shared_metrics["base_scan_opens"]=1;StoredRow row;
            while(cursor->next(row)){budget.materialize(row.values);writer->append(row.values);shared_metrics["base_rows"]=shared_metrics["base_rows"].get<std::uint64_t>()+1;}
            cursor.reset();shared.run=writer->finish();writer.reset();auto after=impl_->storage.transaction_stats(session->tx);
            for(const char* key:{"heap_page_reads","index_page_reads","row_fetches","cover_hits","bitmap_words"})shared_metrics["base_scan_io"][key]=after.value(key,std::uint64_t(0))-before.value(key,std::uint64_t(0));
        }
        shared.preflight=false;phase="query";result["results"]=json::array();
        for(at=0;at<statements.size();++at){
            shared.reader_opens=shared.spool_rows_read=shared.matched_rows=0;
            impl_->before_statement(*session,statements[at]);auto r=impl_->select(*session,statements[at],&budget);
            r["shared_scan"]={{"query_index",at},{"source","CRC temporary row spool"},{"spool_reader_opens",shared.reader_opens},{"spool_rows_read",shared.spool_rows_read},{"candidate_rows",shared.matched_rows}};
            r["plan"]["shared_scan"]=r["shared_scan"];r["locking"]=session->current_locking;
            shared_metrics["spool_reader_opens"]=shared_metrics["spool_reader_opens"].get<std::uint64_t>()+shared.reader_opens;
            shared_metrics["spool_rows_read"]=shared_metrics["spool_rows_read"].get<std::uint64_t>()+shared.spool_rows_read;
            result["results"].push_back(std::move(r));budget.check_time();
        }
        if(shared.run.id){spool.erase(shared.run);}
        shared_metrics["temporary_io"]=spool.diagnostics();shared_metrics["query_budget"]=budget.snapshot();
        phase="commit";impl_->commit(*session);owned=false;result["transaction_state"]="idle";
        result["transaction_options"]={{"protocol","STRICT_2PL"},{"read_only",true},{"lock_granularity","TABLE"}};
        result["plan"]={{"node","ExplicitSharedScanBatch"},{"query_count",queries.size()},{"input_sql_bytes",bytes},{"input_ast_nodes",nodes}};
    }catch(const DbError& error){
        if(owned&&session){try{impl_->rollback(*session);}catch(...){}owned=false;}
        result=blank_result(session?impl_->state(*session):"idle");result["ok"]=false;result["error"]={{"code",error.code},{"message",error.what()},{"position",error.position},{"query_index",at},{"phase",phase}};
    }catch(const std::exception& error){
        if(owned&&session){try{impl_->rollback(*session);}catch(...){}owned=false;}
        result=blank_result(session?impl_->state(*session):"idle");result["ok"]=false;result["error"]={{"code","INTERNAL_ERROR"},{"message",error.what()},{"position",-1},{"query_index",at},{"phase",phase}};
    }
    if(session){session->touched=Clock::now();}
    result["metrics"]["shared_scan"]=std::move(shared_metrics);
    result["metrics"]["elapsed_ms"]=std::chrono::duration<double,std::milli>(Clock::now()-started).count();result["metrics"]["io"]=io_scope.snapshot();return result;
}
