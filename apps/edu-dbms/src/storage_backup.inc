// Include inside Impl, and at namespace scope with PUBLIC macro for adapters.
#ifndef EDUDB_STORAGE_BACKUP_PUBLIC
void backup_configure(const json& source,const StorageOptions& options){
    backup_role=source.value("recovery_role",std::string("PRIMARY"));
    if(backup_role!="PRIMARY"&&backup_role!="RESTORED_READ_ONLY"&&backup_role!="STANDBY")throw DbError("RECOVERY_ROLE","Unknown persistent recovery role");
    backup_instance=source.value("instance_id",uuid());backup_timeline=source.value("timeline",db_uuid);backup_epoch=source.value("fence_epoch",std::uint64_t(0));
    auto identity=[](const std::string& text){return text.size()==32&&text.find_first_not_of("0123456789abcdef")==std::string::npos;};
    if(!identity(backup_instance)||!identity(backup_timeline))throw DbError("RECOVERY_IDENTITY","Malformed recovery instance/timeline");
    backup_archive_enabled=source.value("archive_enabled",false);backup_identity_upgrade=!source.contains("instance_id");
    backup_fence_directory=options.fencing_directory;backup_phase_hook=options.recovery_phase_hook;
    for(const auto* key:{"standby_source_generation","standby_source_base_lsn","standby_replayed_lsn","restore_target_lsn","timeline_parent","timeline_fork_lsn"})if(source.contains(key))backup_metadata[key]=source[key];
}
json backup_control(json control){
    std::lock_guard<std::mutex> guard(backup_mutex);
    if(!control.contains("recovery_mode"))control["recovery_mode"]="REDO_PRIVATE";
    control["recovery_role"]=backup_role;control["instance_id"]=backup_instance;control["timeline"]=backup_timeline;control["fence_epoch"]=backup_epoch;control["archive_enabled"]=backup_archive_enabled;
    for(auto it=backup_metadata.begin();it!=backup_metadata.end();++it)control[it.key()]=it.value();return control;
}
recovery::WriterIdentity backup_writer_identity(){std::lock_guard<std::mutex> guard(backup_mutex);return {backup_instance,backup_timeline,backup_epoch};}
std::string backup_current_role(){std::lock_guard<std::mutex> guard(backup_mutex);return backup_role;}
void backup_setup(){
    IoOwnerScope owner(directory);backup_fence=std::make_unique<recovery::FenceDomain>(db_uuid,backup_fence_directory);
    if(backup_current_role()!="PRIMARY")return;
    // Persist a legacy database's newly generated instance identity before it
    // can become the shared writer; a crash cannot lose a claimed identity.
    if(backup_identity_upgrade)atomic_manifest(directory,control(generation,checkpoint_lsn));
    try{auto state=backup_fence->register_primary(backup_writer_identity());{std::lock_guard<std::mutex> guard(backup_mutex);backup_epoch=state.at("epoch");}atomic_manifest(directory,control(generation,checkpoint_lsn));}
    catch(const DbError& error){if(error.code!="FENCED")throw;backup_fenced=true;std::lock_guard<std::mutex> guard(backup_mutex);backup_error=error.what();}
}
void backup_record_fence_error(const DbError& error){{std::lock_guard<std::mutex> guard(backup_mutex);backup_error=error.code+": "+error.what();}if(error.code=="FENCE_BUSY")return;backup_fenced=true;if(error.code!="FENCED"){failed=true;registry_cv.notify_all();}}
void backup_runtime_write_guard(){
    if(failed)throw DbError("RECOVERY_REQUIRED","Storage requires recovery before accepting writes");
    const auto role=backup_current_role();if(role!="PRIMARY")throw DbError(role=="STANDBY"?"STANDBY_READ_ONLY":"RECOVERY_READ_ONLY","User page modifications are forbidden in this recovery role");
    if(backup_fenced)throw DbError("FENCED","This instance is no longer the same-machine writer");
}
void backup_write_guard(){
    backup_runtime_write_guard();IoOwnerScope owner(directory);
    try{auto permit=backup_fence->acquire_writer(backup_writer_identity());(void)permit;}
    catch(const DbError& error){backup_record_fence_error(error);throw;}
}
void backup_private_write_guard(const std::shared_ptr<Transaction>& tx){
    backup_runtime_write_guard();
    if(verified_write_storage==this&&verified_write_state==tx->state.get())return;
    backup_write_guard(); // Unscoped/internal entry points fail closed, never trust a stale proof.
}
std::optional<recovery::FencePermit> backup_commit_gate(bool writes){
    if(!writes)return std::nullopt;const auto role=backup_current_role();if(role!="PRIMARY")throw DbError(role=="STANDBY"?"STANDBY_READ_ONLY":"RECOVERY_READ_ONLY","Recovery role cannot confirm user writes");
    if(backup_fenced)throw DbError("FENCED","This instance is no longer the same-machine writer");IoOwnerScope owner(directory);
    try{return backup_fence->acquire_writer(backup_writer_identity());}catch(const DbError& error){backup_record_fence_error(error);throw;}
}
void backup_ensure_redo(){require_writable();if(control(generation,checkpoint_lsn).value("recovery_mode",std::string("REDO_PRIVATE"))!="REDO_PRIVATE")throw DbError("BACKUP_MODE","Backup/archive/physical standby in this stage supports REDO_PRIVATE only");}
struct BackupMaintenance {
    Impl& storage;bool acquired=false;
    BackupMaintenance(Impl& s,int timeout_ms):storage(s){
        if(timeout_ms<0||timeout_ms>5000)throw DbError("ARGUMENT","Maintenance timeout must be0..5000ms");
        std::unique_lock<std::mutex> lock(s.registry_mutex);if(s.failed)throw DbError("RECOVERY_REQUIRED","Storage requires recovery");if(s.checkpoint_requested)throw DbError("BUSY","Another maintenance operation is active");s.checkpoint_requested=true;
        if(!s.registry_cv.wait_for(lock,std::chrono::milliseconds(timeout_ms),[&]{return s.active.empty()||s.failed.load();})){s.checkpoint_requested=false;lock.unlock();s.registry_cv.notify_all();throw DbError("BUSY","Active transactions did not finish before maintenance deadline");}
        if(s.failed){s.checkpoint_requested=false;lock.unlock();s.registry_cv.notify_all();throw DbError("RECOVERY_REQUIRED","Storage failed during maintenance wait");}acquired=true;
    }
    ~BackupMaintenance(){if(acquired){{std::lock_guard<std::mutex> lock(storage.registry_mutex);storage.checkpoint_requested=false;}storage.registry_cv.notify_all();}}
};
fs::path backup_subdirectory(const char* name){auto path=directory/name;if(media_detail::native_reparse(path)||fs::is_symlink(path)||(fs::exists(path)&&!fs::is_directory(path)))throw DbError("BACKUP_PATH","Reserved backup/archive path must be a real directory without reparse aliases");fs::create_directories(path);return path;}
recovery::Limits backup_limits(){recovery::Limits limits;limits.phase_hook=backup_phase_hook;return limits;}
json backup_snapshot(const std::string& label,int timeout_ms){
    if(label.empty()||label.size()>48||label.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")!=std::string::npos)throw DbError("BACKUP_LABEL","Backup label must contain1..48 ASCII letters,digits,dash,underscore");
    backup_ensure_redo();IoOwnerScope owner(directory);IoScope scope("paused-write physical backup");BackupMaintenance barrier(*this,timeout_ms);
    // No lease is held while waiting for active transactions to commit.
    auto permit=backup_commit_gate(true);std::lock_guard<std::mutex> wal_lock(wal_mutex);buffer->flush_all();data->sync();
    auto target=backup_subdirectory("backups")/label;auto result=recovery::create_backup(*data,control(generation,checkpoint_lsn),durable_lsn.load(),target.u8string(),backup_limits());result["directory"]=fs::absolute(target).u8string();result["io"]=scope.snapshot();return result;
}
json backup_archive(){
    backup_ensure_redo();IoOwnerScope owner(directory);IoScope scope("validated immutable WAL archive");auto permit=backup_commit_gate(true);std::lock_guard<std::mutex> wal_lock(wal_mutex);wal->sync();
    auto target=backup_subdirectory("archives");auto identity=backup_writer_identity();auto result=recovery::archive_wal(*wal,db_uuid,identity.timeline,target.u8string(),backup_limits());
    auto marker="control-archive-"+std::to_string(result.at("generation").get<std::uint64_t>())+"-"+std::to_string(result.at("end_lsn").get<std::uint64_t>())+"-"+identity.timeline+".json";
    result["manifest_path"]=fs::absolute(target/marker).u8string();result["io"]=scope.snapshot();return result;
}
void backup_archive_before_checkpoint(){
    bool enabled;{std::lock_guard<std::mutex> guard(backup_mutex);enabled=backup_archive_enabled;}
    if(!enabled)return;backup_ensure_redo();auto identity=backup_writer_identity();recovery::archive_wal(*wal,db_uuid,identity.timeline,backup_subdirectory("archives").u8string(),backup_limits());
}
json backup_set_archiving(bool enabled,int timeout_ms){
    backup_ensure_redo();IoOwnerScope owner(directory);IoScope scope("configure WAL archiving");BackupMaintenance barrier(*this,timeout_ms);auto permit=backup_commit_gate(true);std::lock_guard<std::mutex> wal_lock(wal_mutex);
    {std::lock_guard<std::mutex> guard(backup_mutex);backup_archive_enabled=enabled;}
    try{atomic_manifest(directory,control(generation,checkpoint_lsn));}catch(...){failed=true;throw;}
    return {{"archive_enabled",enabled},{"io",scope.snapshot()}};
}
json backup_become_standby(int timeout_ms){
    backup_ensure_redo();IoOwnerScope owner(directory);IoScope scope("establish read-only standby role");BackupMaintenance barrier(*this,timeout_ms);std::lock_guard<std::mutex> wal_lock(wal_mutex);
    if(backup_current_role()!="RESTORED_READ_ONLY"||!fs::exists(directory/"restore-complete.json"))throw DbError("STANDBY_SOURCE","Create a separate restored instance before becoming a standby");
    {std::lock_guard<std::mutex> guard(backup_mutex);backup_role="STANDBY";backup_archive_enabled=false;}
    try{atomic_manifest(directory,control(generation,checkpoint_lsn));}catch(...){failed=true;throw;}
    auto out=backup_identity();out["io"]=scope.snapshot();return out;
}
json backup_replay(const std::string& manifest,int timeout_ms,bool validate_only){
    backup_ensure_redo();IoOwnerScope owner(directory);IoScope scope(validate_only?"validate incoming standby WAL":"force and replay standby WAL");BackupMaintenance barrier(*this,timeout_ms);std::lock_guard<std::mutex> wal_lock(wal_mutex);
    bool attempted=false;auto limits=backup_limits();auto hook=limits.phase_hook;limits.phase_hook=[&](const char* point){if(std::string(point)=="standby_before_mutation")attempted=true;if(hook)hook(point);};limits.validate_only=validate_only;
    try{
        if(!validate_only){buffer->flush_all();data->sync();}
        auto result=recovery::replay_archive(*data,*wal,control(generation,checkpoint_lsn),durable_lsn.load(),manifest,limits);
        if(validate_only){result["io"]=scope.snapshot();return result;}
        auto next=result.at("control");{std::lock_guard<std::mutex> guard(backup_mutex);for(const auto* key:{"standby_source_generation","standby_source_base_lsn","standby_replayed_lsn"})backup_metadata[key]=next.at(key);}
        next_page=std::max(next_page.load(),next.at("next_page_id").get<std::uint64_t>());next_tx=std::max(next_tx.load(),next.at("next_tx_id").get<std::uint64_t>());
        durable_lsn=result.at("replayed_lsn").get<std::uint64_t>();next_lsn=durable_lsn+1;atomic_manifest(directory,control(generation,checkpoint_lsn));
        setup_buffer();if(result.at("appended_wal_bytes").get<std::uint64_t>()||result.at("data_pages_written").get<std::uint64_t>()){protocol_gc();version_graph.reset_epoch("physical standby replay; external creators unknown");}recovered+=result.at("data_pages_written").get<std::uint64_t>();writes+=result.at("data_pages_written").get<std::uint64_t>();if(result.at("appended_wal_bytes").get<std::uint64_t>())++wal_forces;
        result["io"]=scope.snapshot();return result;
    }catch(...){if(attempted)failed=true;throw;}
}
json backup_promote(int timeout_ms,const json* ticket=nullptr,const std::string& key={}){
    backup_ensure_redo();IoOwnerScope owner(directory);IoScope scope("fenced standby promotion");BackupMaintenance barrier(*this,timeout_ms);
    if(ticket&&backup_current_role()=="PRIMARY"){
        auto previous=backup_writer_identity();
        auto permit=backup_fence->accept(previous,durable_lsn.load(),*ticket,key,true);
        return {{"role","PRIMARY"},{"writable",true},{"timeline",previous.timeline},{"fence_epoch",previous.epoch},{"idempotent",true}};
    }
    const auto role=backup_current_role();if(role!="STANDBY"&&role!="RESTORED_READ_ONLY")throw DbError("PROMOTION_ROLE","Only a separate read-only recovered/standby instance can be promoted");
    auto previous=backup_writer_identity();
    auto permit=ticket?backup_fence->accept(previous,durable_lsn.load(),*ticket,key):
        backup_fence->promote(previous.instance_id,previous.timeline,durable_lsn.load());
    bool claimed=true;try{
        if(backup_phase_hook)backup_phase_hook("promotion_after_fence_force");std::lock_guard<std::mutex> wal_lock(wal_mutex);buffer->flush_all();data->sync();
        auto gen=generation+1;if(!gen)throw DbError("DATABASE_TOO_LARGE","WAL generation exhausted");while(fs::exists(wal_path(gen))){if(++gen==0)throw DbError("DATABASE_TOO_LARGE","WAL generation exhausted");}
        auto next=std::make_unique<NativeFile>(wal_path(gen),true,true);auto header=wal_header(gen,durable_lsn);next->write(0,header.data(),header.size());next->sync();sync_directory(directory);
        const auto& state=permit.state();{std::lock_guard<std::mutex> guard(backup_mutex);backup_role="PRIMARY";backup_timeline=state.at("timeline");backup_epoch=state.at("epoch");backup_archive_enabled=false;backup_metadata["timeline_parent"]=previous.timeline;backup_metadata["timeline_fork_lsn"]=durable_lsn.load();backup_error.clear();}
        atomic_manifest(directory,control(gen,durable_lsn));if(backup_phase_hook)backup_phase_hook("promotion_after_local_control");wal=std::move(next);generation=gen;checkpoint_lsn=durable_lsn;setup_buffer();backup_fenced=false;
        auto result=backup_identity();result["writable"]=true;result["owner_confirmation"]="current promotion holds the durable ownership permit";result["scope"]=ticket?"signed cross-domain handoff at the surrendered durable LSN":"same-machine shared epoch/owner lease; asynchronous fork may omit unreceived primary commits";result["io"]=scope.snapshot();return result;
    }catch(...){if(claimed)failed=true;throw;}
}
json backup_identity(){
    json result;recovery::WriterIdentity identity;
    {std::lock_guard<std::mutex> guard(backup_mutex);identity={backup_instance,backup_timeline,backup_epoch};result={{"uuid",db_uuid},{"instance_id",backup_instance},{"timeline",backup_timeline},{"fence_epoch",backup_epoch},{"role",backup_role},{"archive_enabled",backup_archive_enabled},{"writable",false},{"fenced",backup_fenced.load()},{"fence_error",backup_error},{"fencing_scope","per-user same-machine shared UUID; commit lease held through durable publish"},{"durable_lsn",durable_lsn.load()},{"standby",backup_metadata}};}
    {std::lock_guard<std::mutex> guard(registry_mutex);result["maintenance_requested"]=checkpoint_requested;}
    if(result.at("role")=="PRIMARY"&&!backup_fenced.load()&&!failed.load()){
        IoOwnerScope owner(directory);try{auto permit=backup_fence->acquire_writer(identity,0);result["writable"]=true;result["owner_confirmation"]="shared owner/epoch/timeline checked";}
        catch(const DbError& error){if(error.code=="FENCE_BUSY")result["owner_confirmation"]="lease busy; writable ownership temporarily unconfirmed";else{backup_record_fence_error(error);result["fenced"]=true;result["fence_error"]=error.code+": "+error.what();}}
    }
    if(result.at("maintenance_requested")==true)result["writable"]=false;
    return result;
}
json backup_diagnostics(json out){out["recovery_identity"]=backup_identity();return out;}
#else
json Storage::backup(const std::string& label,int timeout_ms){return impl_->backup_snapshot(label,timeout_ms);}
json Storage::archive_wal(){return impl_->backup_archive();}
json Storage::set_archiving(bool enabled,int timeout_ms){return impl_->backup_set_archiving(enabled,timeout_ms);}
json Storage::become_standby(int timeout_ms){return impl_->backup_become_standby(timeout_ms);}
json Storage::replay_archive(const std::string& manifest,int timeout_ms,bool validate_only){return impl_->backup_replay(manifest,timeout_ms,validate_only);}
json Storage::promote(int timeout_ms){return impl_->backup_promote(timeout_ms);}
json Storage::handoff_offer(const std::string& key){
    impl_->backup_ensure_redo();Impl::BackupMaintenance barrier(*impl_,5000);
    if(impl_->backup_current_role()!="STANDBY"&&impl_->backup_current_role()!="RESTORED_READ_ONLY")
        throw DbError("HANDOFF_ROLE","Only a separate read-only standby may issue an offer");
    return impl_->backup_fence->offer(impl_->backup_writer_identity(),impl_->durable_lsn.load(),key);
}
json Storage::handoff_surrender(const json& offer,const std::string& key){
    impl_->backup_ensure_redo();Impl::BackupMaintenance barrier(*impl_,5000);
    if(impl_->backup_current_role()!="PRIMARY")throw DbError("HANDOFF_ROLE","Only the source primary may release ownership");
    auto ticket=impl_->backup_fence->surrender(impl_->backup_writer_identity(),impl_->durable_lsn.load(),offer,key);
    impl_->backup_fenced=true;return ticket;
}
json Storage::handoff_accept(const json& ticket,const std::string& key){return impl_->backup_promote(5000,&ticket,key);}
json Storage::recovery_identity(){return impl_->backup_identity();}
#endif
