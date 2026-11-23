// Function body for restore_backup. Inputs stay open read-only only while read.
Budget budget(limits);
auto source=absolute_path(fs::u8path(directory));
auto base=checked_backup(source,budget);
const auto identity=base.at("uuid").get<std::string>();
const auto timeline=base.at("timeline").get<std::string>();
const auto start=number(base,"end_lsn",0,std::numeric_limits<std::uint64_t>::max());
const auto base_generation=number(base,"source_wal_generation",1,std::numeric_limits<std::uint64_t>::max());
const auto base_wal_start=number(base,"source_wal_base_lsn",0,start);
if(archives.size()>4096)fail("ARCHIVE_LIMIT","At most4096 archive manifests are allowed");
std::map<std::uint64_t,Log> generations;
std::set<std::string> seen_paths;
std::uint64_t materialized=0;
for(const auto& name:archives){
    budget.tick();auto marker=absolute_path(fs::u8path(name));
    auto canonical=canonical_path(marker).generic_u8string();
#ifdef _WIN32
    for(auto& c:canonical)if(c>='A'&&c<='Z')c=static_cast<char>(c-'A'+'a');
#endif
    if(!seen_paths.insert(canonical).second)continue;
    auto m=load_json(marker);
    if(m.value("format",std::string())!="EduDBWalArchive1"||m.value("state",std::string())!="COMPLETE"||m.at("uuid")!=identity||m.at("timeline")!=timeline)fail("ARCHIVE_IDENTITY","Archive is incomplete or belongs to another database/timeline");
    auto filename=m.at("file").get<std::string>();
    if(filename.empty()||fs::u8path(filename).filename().u8string()!=filename||filename.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-.")!=std::string::npos||filename.rfind("wal-archive-",0)!=0||filename.substr(filename.size()-4)!=".log")fail("ARCHIVE_MANIFEST","Unsafe archive filename");
    const auto file=marker.parent_path()/filename;
    auto proof=file_proof(file,budget,limits.wal_bytes);
    if(proof!=m.at("proof"))fail("ARCHIVE_CHECKSUM","Archive file checksum/length mismatch");
    const auto bytes=proof.at("bytes").get<std::uint64_t>();
    if(materialized>limits.wal_bytes||bytes>limits.wal_bytes-materialized)fail("ARCHIVE_LIMIT","Total materialized input archives exceed the operation WAL budget");
    materialized+=bytes;
    SourceFile input(file);auto log=parse_log(input,identity,budget);
    if(log.base!=number(m,"base_lsn",0,std::numeric_limits<std::uint64_t>::max())||log.end!=number(m,"end_lsn",0,std::numeric_limits<std::uint64_t>::max())||log.generation!=number(m,"generation",1,std::numeric_limits<std::uint64_t>::max()))fail("ARCHIVE_MANIFEST","Archive header and manifest boundaries disagree");
    auto previous=generations.find(log.generation);
    if(previous!=generations.end()){
        const auto& a=previous->second;
        if(a.base!=log.base||!std::equal(a.bytes.begin(),a.bytes.begin()+std::min(a.bytes.size(),log.bytes.size()),log.bytes.begin()))fail("ARCHIVE_FORK","Snapshots of the same generation have different bytes");
        if(log.bytes.size()>a.bytes.size())previous->second=std::move(log);
    }else generations.emplace(log.generation,std::move(log));
}

// The base already represents source_generation through start. A later
// generation can start at start only if it is exactly source_generation+1.
// Empty generations still form part of the explicit archive chain.
bool have_chain=false;
std::uint64_t previous_generation=base_generation,previous_end=start;
for(const auto& item:generations){
    const auto& log=item.second;
    if(item.first<base_generation){if(log.end>start)fail("ARCHIVE_FORK","An older generation exceeds the base's committed history");continue;}
    if(!have_chain&&item.first==base_generation){
        if(log.base!=base_wal_start||log.end<start)fail("ARCHIVE_GAP","Source generation does not cover the base boundary");
        bool boundary=start==log.base;for(const auto& t:log.transactions)boundary=boundary||t.end==start;
        if(!boundary)fail("ARCHIVE_GAP","Base end is not a boundary in its source generation");
    }else if(previous_generation==std::numeric_limits<std::uint64_t>::max()||item.first!=previous_generation+1||log.base!=previous_end){
        fail("ARCHIVE_GAP","Missing/overlapping generation or predecessor LSN");
    }
    have_chain=true;previous_generation=item.first;previous_end=log.end;
}

std::map<std::uint64_t,TransactionImage> selected;
std::uint64_t retained=0;
for(auto& item:generations)for(auto& transaction:item.second.transactions)if(transaction.end>start){
    if(transaction.begin<=start)fail("ARCHIVE_GAP","Transaction straddles physical backup boundary");
    auto old=selected.find(transaction.end);
    if(old!=selected.end()){
        if(old->second.begin!=transaction.begin||old->second.tx!=transaction.tx||old->second.records!=transaction.records)fail("ARCHIVE_FORK","Different transactions occupy the same COMMIT LSN");
    }else{
        if(retained>limits.wal_bytes||transaction.records.size()>limits.wal_bytes-retained)fail("ARCHIVE_LIMIT","Selected WAL exceeds materialization budget");
        retained+=transaction.records.size();selected.emplace(transaction.end,std::move(transaction));
    }
}
if(!target)target=selected.empty()?start:selected.rbegin()->first;
if(target<start)fail("RECOVERY_TARGET","Target precedes base backup end LSN");
if(target!=start&&!selected.count(target))fail("RECOVERY_TARGET","Target is not a complete archived COMMIT boundary");
auto header=wal_header(identity,1,start);
std::vector<unsigned char> combined(header.begin(),header.end());
auto expected=start;auto max_tx=std::uint64_t(0);
for(const auto& item:selected){const auto& t=item.second;if(t.end>target)break;
    if(expected==std::numeric_limits<std::uint64_t>::max()||t.begin!=expected+1)fail("ARCHIVE_GAP","Missing or overlapping transaction history");
    if(combined.size()>limits.wal_bytes||t.records.size()>limits.wal_bytes-combined.size())fail("ARCHIVE_LIMIT","Restored WAL exceeds budget");
    combined.insert(combined.end(),t.records.begin(),t.records.end());expected=t.end;max_tx=std::max(max_tx,t.tx);
}
if(expected!=target)fail("ARCHIVE_GAP","Archive coverage does not reach target");
auto dest=canonical_path(absolute_path(fs::u8path(destination)));
auto path_key=[](const fs::path& path){auto key=canonical_path(path).generic_u8string();
#ifdef _WIN32
    for(auto& c:key)if(c>='A'&&c<='Z')c=static_cast<char>(c-'A'+'a');
#endif
    return key+"/";};
auto a=path_key(source),b=path_key(dest);
if(a.rfind(b,0)==0||b.rfind(a,0)==0)fail("BACKUP_PATH","Restore source and target must be separate trees");
for(const auto& name:archives){auto folder=path_key(absolute_path(fs::u8path(name)).parent_path());if(folder.rfind(b,0)==0||b.rfind(folder,0)==0)fail("BACKUP_PATH","Restore target must be separate from immutable archive trees");}
new_directory(dest);
write_json_new(dest/"restore-incomplete.json",{{"state","INCOMPLETE"},{"uuid",identity}});
SourceFile data(source/"data.pages");copy_bytes(data,dest/"data.pages",budget,limits.data_bytes);
if(file_proof(dest/"data.pages",budget,limits.data_bytes)!=base["files"]["data.pages"])fail("BACKUP_CHECKSUM","Backup source changed before copying");
budget.phase("restore_after_data_copy");
auto control=load_json(source/"control.json");
control["recovery_role"]="RESTORED_READ_ONLY";control["restore_target_lsn"]=target;
control["instance_id"]=new_identity();control["fence_epoch"]=0;
control["standby_source_generation"]=base_generation;control["standby_source_base_lsn"]=base_wal_start;control["standby_replayed_lsn"]=target;
for(const auto& item:generations)if(item.first>=base_generation&&item.second.base<=target){control["standby_source_generation"]=item.first;control["standby_source_base_lsn"]=item.second.base;}
if(max_tx==std::numeric_limits<std::uint64_t>::max())fail("RECOVERY_TARGET","Transaction ID range exhausted");
control["next_tx_id"]=std::max(control.at("next_tx_id").get<std::uint64_t>(),max_tx+1);
write_json_new(dest/"control.json",control);
{File file(dest/"wal-1.log",true,true);file.write(0,combined.data(),combined.size());file.sync();}
json files=json::object();for(const auto& name:{"control.json","data.pages","wal-1.log"})files[name]=file_proof(dest/name,budget,file_budget(name,limits));
json result={{"state","READY_FOR_CORE_RECOVERY"},{"uuid",identity},{"instance_id",control["instance_id"]},{"source_timeline",timeline},{"base_end_lsn",start},{"target_commit_lsn",target},{"transactions",std::count_if(selected.begin(),selected.end(),[&](const auto& t){return t.first<=target;})},{"files_before_core_recovery",files},{"core_recovery","not yet run; database startup must replay and verify"}};
budget.phase("restore_before_complete");write_json_new(dest/"restore-complete.json",result);budget.phase("restore_after_complete");return result;
