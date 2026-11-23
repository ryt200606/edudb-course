Budget budget(limits);validate_control(control);
if(control.value("recovery_role",std::string())!="STANDBY")fail("STANDBY_ROLE","Physical replay requires the STANDBY role");
const auto identity=control.at("uuid").get<std::string>();const auto timeline=control.at("timeline").get<std::string>();uuid_check(timeline);
auto marker=fs::absolute(fs::u8path(archive_manifest)).lexically_normal();auto manifest=load_json(marker);
if(manifest.value("format",std::string())!="EduDBWalArchive1"||manifest.value("state",std::string())!="COMPLETE"||manifest.at("uuid")!=identity||manifest.at("timeline")!=timeline)fail("ARCHIVE_IDENTITY","Incoming WAL database/timeline differs from the standby");
auto filename=manifest.at("file").get<std::string>();
if(filename.rfind("wal-archive-",0)!=0||filename.size()<16||fs::u8path(filename).filename().u8string()!=filename||filename.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-.")!=std::string::npos||filename.substr(filename.size()-4)!=".log")fail("ARCHIVE_MANIFEST","Unsafe incoming archive filename");
auto incoming_path=marker.parent_path()/filename;
if(file_proof(incoming_path,budget,limits.wal_bytes)!=manifest.at("proof"))fail("ARCHIVE_CHECKSUM","Incoming WAL proof differs");
SourceFile source(incoming_path);auto incoming=parse_log(source,identity,budget);auto local=parse_log(active_wal,identity,budget);
if(local.end!=current_end_lsn||local.generation!=control.at("wal_generation").get<std::uint64_t>()||local.base!=control.at("checkpoint_lsn").get<std::uint64_t>())fail("STANDBY_STATE","Local WAL/control/end LSN disagree");
if(incoming.generation!=number(manifest,"generation",1,std::numeric_limits<std::uint64_t>::max())||incoming.base!=number(manifest,"base_lsn",0,std::numeric_limits<std::uint64_t>::max())||incoming.end!=number(manifest,"end_lsn",0,std::numeric_limits<std::uint64_t>::max()))fail("ARCHIVE_MANIFEST","Incoming WAL header differs from manifest");
const auto source_generation=control.value("standby_source_generation",std::uint64_t(0));
const auto source_base=control.value("standby_source_base_lsn",std::uint64_t(0));
const auto recorded_replay=control.value("standby_replayed_lsn",local.base);
if(incoming.generation==source_generation){if(incoming.base!=source_base)fail("ARCHIVE_FORK","Same source generation has a different base");}
else if(source_generation!=std::numeric_limits<std::uint64_t>::max()&&incoming.generation==source_generation+1){
    if(incoming.base!=current_end_lsn&&incoming.base!=recorded_replay)fail("ARCHIVE_GAP","Next source generation does not start at a verified replay boundary");
}else if(incoming.generation>source_generation||incoming.end>current_end_lsn)fail("ARCHIVE_GAP","Missing source generation or future data on an older generation");
std::map<std::uint64_t,const TransactionImage*> local_commits;for(const auto& transaction:local.transactions)local_commits[transaction.end]=&transaction;
std::vector<const TransactionImage*> append;
auto next=current_end_lsn;auto max_tx=std::uint64_t(0);std::uint64_t new_bytes=0;
for(const auto& transaction:incoming.transactions){
    if(transaction.end<=local.base)continue;
    if(transaction.end<=current_end_lsn){auto found=local_commits.find(transaction.end);if(found==local_commits.end()||found->second->begin!=transaction.begin||found->second->tx!=transaction.tx||found->second->records!=transaction.records)fail("ARCHIVE_FORK","Overlapping incoming committed bytes differ from locally forced history");continue;}
    if(next==std::numeric_limits<std::uint64_t>::max()||transaction.begin!=next+1)fail("ARCHIVE_GAP","Incoming transaction sequence does not continue the standby");
    if(new_bytes>limits.wal_bytes||transaction.records.size()>limits.wal_bytes-new_bytes)fail("ARCHIVE_LIMIT","Replay append exceeds budget");
    new_bytes+=transaction.records.size();append.push_back(&transaction);next=transaction.end;max_tx=std::max(max_tx,transaction.tx);
}
if(local.bytes.size()>limits.wal_bytes||new_bytes>limits.wal_bytes-local.bytes.size())fail("ARCHIVE_LIMIT","Accumulated local standby WAL exceeds budget");
validate_data(data,control,current_end_lsn,budget);
if(limits.validate_only)return {{"status","VALIDATED"},{"received_lsn",incoming.end},{"current_end_lsn",current_end_lsn},{"planned_replayed_lsn",next},{"planned_wal_bytes",new_bytes},{"source_generation",incoming.generation}};
budget.phase("standby_before_mutation");
auto offset=active_wal.size();for(const auto* transaction:append){budget.tick();active_wal.write(offset,transaction->records.data(),transaction->records.size());offset+=transaction->records.size();}
if(!append.empty())active_wal.sync();
budget.phase("standby_after_wal_force");
std::uint64_t pages=0,max_page=data.size()/PAGE;
// Repeat physical redo for overlapping records too. This is necessary if a
// previous receive forced local WAL but crashed before its data replay.
for(const auto& transaction:incoming.transactions)if(transaction.end>local.base&&transaction.end<=next)for(std::size_t at=0;at<transaction.records.size();){budget.tick();auto* header=transaction.records.data()+at;auto type=get32(header+8),size=get32(header+4);if(type==2){auto* body=header+48;auto id=get64(body);auto* image=body+8;bool current=false;if(data.size()>=(id+1)*PAGE){std::vector<unsigned char> existing(PAGE);data.read(id*PAGE,existing.data(),existing.size());current=page_valid(existing,id)&&get64(existing.data()+16)>=get64(image+16);}if(!current){data.write(id*PAGE,image,PAGE);++pages;}max_page=std::max(max_page,id+1);}at+=48+size;}
if(pages)data.sync();
budget.phase("standby_after_replay_data");
auto updated=control;updated.erase("checksum");
if(incoming.generation>=source_generation){updated["standby_source_generation"]=incoming.generation;updated["standby_source_base_lsn"]=incoming.base;}
updated["standby_replayed_lsn"]=next;updated["next_page_id"]=std::max(updated.at("next_page_id").get<std::uint64_t>(),max_page);
if(max_tx==std::numeric_limits<std::uint64_t>::max())fail("STANDBY_STATE","Transaction identifier exhausted");
updated["next_tx_id"]=std::max(updated.at("next_tx_id").get<std::uint64_t>(),max_tx+1);
return {{"status","REPLAYED"},{"received_lsn",incoming.end},{"forced_lsn",next},{"replayed_lsn",next},{"appended_transactions",append.size()},{"appended_wal_bytes",new_bytes},{"data_pages_written",pages},{"control",updated},{"scope","same-machine physical redo; no active readers; caller must persist control and invalidate caches"}};
