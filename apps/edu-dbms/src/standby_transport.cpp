#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0A00
#endif
#ifndef WINVER
#define WINVER 0x0A00
#endif
#include "httplib.h"
#include "standby_transport.hpp"
#include "input_limits.hpp"
#include "media_native.hpp"
#include <atomic>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <tuple>
#ifdef _WIN32
#include <wincrypt.h>
#endif
namespace edudb::recovery {
namespace {
using namespace media_detail;
#include "recovery_native.inc"
void identity_ok(const std::string& id){if(id.size()!=32||id.find_first_not_of("0123456789abcdef")!=std::string::npos)fail("RECEIVER_ARGUMENT","Receiver identity/token must be32 lowercase hexadecimal digits");}
json load_state(const fs::path& path){ReadOnlyFile f(path);if(f.size()<2||f.size()>65536)fail("RECEIVER_STATE","Receiver state size invalid");std::string bytes(f.size(),'\0');f.read(0,reinterpret_cast<unsigned char*>(bytes.data()),bytes.size());try{auto j=input::parse_json(bytes);auto checksum=j.at("checksum").get<std::uint32_t>();j.erase("checksum");if(crc(j.dump())!=checksum)fail("RECEIVER_STATE","Receiver state checksum invalid");return j;}catch(const DbError&){throw;}catch(...){fail("RECEIVER_STATE","Malformed receiver state");}}
void persist(const fs::path& path,json state){state["checksum"]=crc(state.dump());auto bytes=state.dump();auto temp=path;temp+=".tmp-"+new_identity();bool owned=false;try{{File f(temp,true,true);owned=true;f.write(0,reinterpret_cast<const unsigned char*>(bytes.data()),bytes.size());f.sync();}replace_atomic(temp,path,true);owned=false;}catch(...){if(owned){try{native_remove(temp);}catch(...){}}throw;}}
fs::path marker_name(const fs::path& dir,const json& m){return dir/("control-archive-"+std::to_string(m.at("generation").get<std::uint64_t>())+"-"+std::to_string(m.at("end_lsn").get<std::uint64_t>())+"-"+m.at("timeline").get<std::string>()+".json");}
bool cleanup_stage(const fs::path& path,const std::string& identity){try{if(native_reparse(path)||!native_directory(path))return false;auto owner=load_state(path/"control-stage-owner.json");if(owner.at("format")!="EduDBReceiveStage1"||owner.at("uuid")!=identity||path.filename().u8string()!="receive-stage-"+owner.at("id").get<std::string>())return false;std::vector<fs::path> files;for(const auto& item:native_entries(path,16)){auto name=item.filename().u8string();if(native_reparse(item)||!native_regular(item))return false;if(name=="control-stage-owner.json")continue;bool known=name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-.")==std::string::npos&&(name.rfind("wal-archive-",0)==0||name.rfind("control-archive-",0)==0||name.rfind("control-stage-owner.json.tmp-",0)==0);if(!known)return false;files.push_back(item);}for(const auto& file:files)if(!native_remove(file))return false;if(!native_remove(path/"control-stage-owner.json"))return false;return native_remove(path);}catch(...){return false;}}
class MemoryInput final:public DataDevice {
    const unsigned char* bytes_;std::uint64_t size_;
public:MemoryInput(const unsigned char* bytes,std::uint64_t size):bytes_(bytes),size_(size){}
    std::uint64_t size()override{return size_;}
    void read(std::uint64_t at,unsigned char* p,std::size_t n)override{if(at>size_||n>size_-at)fail("RECEIVER_PROTOCOL","Memory frame is truncated");std::copy_n(bytes_+at,n,p);}
    void write(std::uint64_t,const unsigned char*,std::size_t)override{fail("READ_ONLY","Received input is immutable");}
    void sync()override{fail("READ_ONLY","Received input is immutable");}
};
}
struct StandbyReceiver::Impl {
    ReceiverOptions options;Replay apply;fs::path inbox,state_path;
    httplib::Server server;std::thread thread;unsigned bound_port=0;
    mutable std::mutex status_mutex;std::mutex receive_mutex;
    json state;std::string last_error;bool started=false;
    Impl(ReceiverOptions o,Replay callback):options(std::move(o)),apply(std::move(callback)),inbox(absolute_path(fs::u8path(options.inbox_directory))),state_path(inbox/"control-receiver.json"){
        identity_ok(options.database_uuid);identity_ok(options.timeline);identity_ok(options.token);
        if(!apply||options.port>65535||options.inbox_directory.empty()||options.limits.wal_bytes<64||options.limits.wal_bytes>512ull*1024*1024)fail("RECEIVER_ARGUMENT","Invalid receiver configuration");
        fs::create_directories(native_path(inbox));if(native_reparse(inbox))fail("RECEIVER_PATH","Inbox cannot be a symlink");
        state={{"format","EduDBStandbyReceiver1"},{"uuid",options.database_uuid},{"timeline",options.timeline},{"received_lsn",options.base_lsn},{"forced_lsn",options.base_lsn},{"replayed_lsn",options.base_lsn}};
        if(native_exists(state_path)){state=load_state(state_path);if(state.at("format")!="EduDBStandbyReceiver1"||state.at("uuid")!=options.database_uuid||state.at("timeline")!=options.timeline)fail("RECEIVER_STATE","Receiver state belongs to another source");if(state.at("replayed_lsn").get<std::uint64_t>()>state.at("forced_lsn").get<std::uint64_t>())fail("RECEIVER_STATE","Replay is ahead of forced receive state");}
        state["received_lsn"]=state["forced_lsn"]; // unforced progress is not recovered.
        server.new_task_queue=[](){return new httplib::ThreadPool(2);};
        server.set_keep_alive_max_count(1);server.set_read_timeout(5,0);server.set_write_timeout(5,0);
        server.set_payload_max_length(static_cast<std::size_t>(options.limits.wal_bytes+4*1024*1024+8));
        server.Get("/status",[this](const httplib::Request& req,httplib::Response& res){if(req.get_header_value("X-EduDB-Token")!=options.token){res.status=403;res.set_content("{}","application/json");return;}res.set_content(status().dump(),"application/json");});
        server.Post("/receive",[this](const httplib::Request& req,httplib::Response& res){
            if(req.get_header_value("X-EduDB-Token")!=options.token){res.status=403;res.set_content("{}","application/json");return;}
            std::lock_guard<std::mutex> serialized(receive_mutex);std::optional<IoOwnerScope> owner;if(!options.owner_directory.empty())owner.emplace(fs::u8path(options.owner_directory));IoScope scope("standby loopback receive/force/replay");
            try{auto result=receive(req.body);result["io"]=scope.snapshot();res.set_content(result.dump(),"application/json");}
            catch(const DbError& e){{std::lock_guard<std::mutex> lock(status_mutex);last_error=e.code+": "+e.what();}res.status=409;res.set_content(json{{"ok",false},{"error",{{"code",e.code},{"message",e.what()}}},{"state",status()},{"io",scope.snapshot()}}.dump(),"application/json");}
            catch(const std::exception& e){{std::lock_guard<std::mutex> lock(status_mutex);last_error=e.what();}res.status=500;res.set_content(json{{"ok",false},{"error",e.what()},{"state",status()}}.dump(),"application/json");}
        });
    }
    void phase(const char* name){if(options.phase_hook)options.phase_hook(name);}
    json status()const{std::lock_guard<std::mutex> lock(status_mutex);auto out=state;out["port"]=bound_port;out["listening"]=server.is_running();out["last_error"]=last_error;out["replication_mode"]="asynchronous physical WAL shipping over loopback; primary commit does not wait for receiver";return out;}
    void forced(const json& m){std::lock_guard<std::mutex> lock(status_mutex);state["received_lsn"]=std::max(state.at("received_lsn").get<std::uint64_t>(),m.at("end_lsn").get<std::uint64_t>());state["forced_lsn"]=std::max(state.at("forced_lsn").get<std::uint64_t>(),m.at("end_lsn").get<std::uint64_t>());persist(state_path,state);}
    json replay(const fs::path& marker,const json& m){auto result=apply(marker.u8string(),false);auto replayed=result.at("replayed_lsn").get<std::uint64_t>();std::lock_guard<std::mutex> lock(status_mutex);if(replayed>state.at("forced_lsn").get<std::uint64_t>()||replayed<options.base_lsn)fail("RECEIVER_STATE","Callback replayed LSN is outside forced coverage");state["replayed_lsn"]=std::max(replayed,state.at("replayed_lsn").get<std::uint64_t>());state["last_archive"]=m.at("file");last_error.clear();persist(state_path,state);return result;}
    json receive(const std::string& body){
        if(body.size()<8)fail("RECEIVER_PROTOCOL","Missing envelope length");
        auto metadata_size=get64(reinterpret_cast<const unsigned char*>(body.data()));
        if(metadata_size<2||metadata_size>4*1024*1024||metadata_size>body.size()-8)fail("RECEIVER_PROTOCOL","Invalid envelope metadata length");
        json metadata;try{metadata=input::parse_json(body.substr(8,static_cast<std::size_t>(metadata_size)));}catch(...){fail("RECEIVER_PROTOCOL","Invalid envelope JSON");}
        if(!metadata.is_object())fail("RECEIVER_PROTOCOL","Envelope metadata must be an object");
        for(const auto* key:{"format","state","uuid","timeline","file"})if(!metadata.contains(key)||!metadata.at(key).is_string())fail("RECEIVER_PROTOCOL",std::string("Envelope metadata requires string ")+key);
        for(const auto* key:{"generation","base_lsn","end_lsn"})if(!metadata.contains(key)||!(metadata.at(key).is_number_unsigned()||(metadata.at(key).is_number_integer()&&metadata.at(key).get<std::int64_t>()>=0)))fail("RECEIVER_PROTOCOL",std::string("Envelope metadata requires unsigned ")+key);
        if(metadata.value("format",std::string())!="EduDBWalArchive1"||metadata.value("state",std::string())!="COMPLETE"||metadata.at("uuid")!=options.database_uuid||metadata.at("timeline")!=options.timeline)fail("ARCHIVE_IDENTITY","Receiver database/timeline differs from shipment");
        const auto bytes=body.size()-8-metadata_size;if(bytes<64||bytes>options.limits.wal_bytes)fail("RECEIVER_LIMIT","Envelope WAL length outside budget");
        MemoryInput input(reinterpret_cast<const unsigned char*>(body.data())+8+metadata_size,bytes);
        auto inspected=inspect_wal(input,options.database_uuid,options.limits);
        if(inspected.at("end_lsn")!=metadata.at("end_lsn")||inspected.at("generation")!=metadata.at("generation")||inspected.at("base_lsn")!=metadata.at("base_lsn"))fail("ARCHIVE_MANIFEST","Envelope metadata and WAL differ");
        {std::lock_guard<std::mutex> lock(status_mutex);state["received_lsn"]=std::max(state.at("received_lsn").get<std::uint64_t>(),metadata.at("end_lsn").get<std::uint64_t>());}
        const auto existing_marker=marker_name(inbox,metadata);
        if(native_exists(existing_marker)){
            auto canonical=archive_wal(input,options.database_uuid,options.timeline,inbox.u8string(),options.limits);
            if(canonical!=metadata)fail("ARCHIVE_MANIFEST","Duplicate envelope differs from validated canonical manifest");
            auto validated=apply(existing_marker.u8string(),true);if(validated.value("status",std::string())!="VALIDATED")fail("RECEIVER_STATE","Duplicate replay validation did not succeed");
            forced(canonical);phase("receiver_after_force");auto replayed=replay(existing_marker,canonical);phase("receiver_after_replay");auto result=status();result["ok"]=true;result["replay"]=replayed;result["acknowledgement"]="previously forced immutable WAL verified and replay callback completed";return result;
        }
        // Reserve peak staging+publication space before writing an envelope.
        check_archive_capacity(inbox.u8string(),options.database_uuid,2*(bytes+metadata_size)+4096,6,options.limits);
        std::size_t stages=0;for(const auto& item:native_entries(inbox))if(item.filename().u8string().rfind("receive-stage-",0)==0)++stages;if(stages>=4)fail("RECEIVER_LIMIT","Retained incoming stages reached the bounded count4");
        auto id=new_identity();auto stage=inbox/("receive-stage-"+id);if(!fs::create_directory(native_path(stage)))fail("RECEIVER_PATH","Cannot create unique receive stage");
        persist(stage/"control-stage-owner.json",{{"format","EduDBReceiveStage1"},{"uuid",options.database_uuid},{"id",id}});
        struct StageGuard{fs::path path;std::string uuid;~StageGuard(){cleanup_stage(path,uuid);}} guard{stage,options.database_uuid};
        auto limits=options.limits;auto previous=limits.phase_hook;limits.phase_hook=[&](const char* at){if(previous)previous(at);if(std::string(at)=="copy_before_sync")phase("receiver_before_force");};
        auto canonical=archive_wal(input,options.database_uuid,options.timeline,stage.u8string(),limits);
        if(canonical!=metadata)fail("ARCHIVE_MANIFEST","Envelope is not the canonical validated archive manifest");
        auto staged_marker=marker_name(stage,canonical);auto validated=apply(staged_marker.u8string(),true);if(validated.value("status",std::string())!="VALIDATED")fail("RECEIVER_STATE","Replay validator did not confirm a side-effect-free check");
        // Admission to forced history happens only after validating lineage and
        // overlap against the live, read-only standby. Staging is not resumed.
        ReadOnlyFile staged_wal(stage/canonical.at("file").get<std::string>());
        canonical=archive_wal(staged_wal,options.database_uuid,options.timeline,inbox.u8string(),options.limits);
        forced(canonical);phase("receiver_after_force");
        auto marker=marker_name(inbox,canonical);auto replayed=replay(marker,canonical);phase("receiver_after_replay");
        auto result=status();result["ok"]=true;result["replay"]=replayed;result["acknowledgement"]="received WAL forced and replay callback completed";return result;
    }
    void resume(){
        for(const auto& item:native_entries(inbox))if(item.filename().u8string().rfind("receive-stage-",0)==0)cleanup_stage(item,options.database_uuid);
        struct Pending{std::uint64_t generation,end;fs::path marker;json metadata;};std::vector<Pending> pending;
        for(const auto& item:native_entries(inbox)){auto name=item.filename().u8string();if(name.rfind("control-archive-",0)!=0||item.extension()!=L".json")continue;auto m=inspect_archive(item.u8string(),options.limits);if(m.at("uuid")!=options.database_uuid||m.at("timeline")!=options.timeline)fail("RECEIVER_STATE","Foreign complete archive in inbox");pending.push_back({m.at("generation"),m.at("end_lsn"),item,m});}
        std::sort(pending.begin(),pending.end(),[](const Pending& a,const Pending& b){return std::tie(a.generation,a.end)<std::tie(b.generation,b.end);});
        // Complete root markers were admitted only after lineage validation.
        // Reconstruct the forced watermark before replaying an older duplicate;
        // the core may already have recovered a later locally forced segment.
        for(const auto& item:pending)forced(item.metadata);
        for(const auto& item:pending)replay(item.marker,item.metadata);
    }
};
StandbyReceiver::StandbyReceiver(ReceiverOptions options,Replay callback):impl_(std::make_unique<Impl>(std::move(options),std::move(callback))){}
StandbyReceiver::~StandbyReceiver(){stop();}
void StandbyReceiver::start(){auto& p=*impl_;if(p.started)throw DbError("RECEIVER_STATE","Receiver already started");std::optional<IoOwnerScope> owner;if(!p.options.owner_directory.empty())owner.emplace(fs::u8path(p.options.owner_directory));p.resume();int port=p.options.port? (p.server.bind_to_port("127.0.0.1",static_cast<int>(p.options.port))?static_cast<int>(p.options.port):-1):p.server.bind_to_any_port("127.0.0.1");if(port<=0)throw DbError("RECEIVER_BIND","Cannot bind loopback receiver");p.bound_port=static_cast<unsigned>(port);p.started=true;p.thread=std::thread([&p]{p.server.listen_after_bind();});auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);while(!p.server.is_running()){if(std::chrono::steady_clock::now()>=deadline){stop();throw DbError("RECEIVER_BIND","Loopback receiver did not enter listening state");}std::this_thread::sleep_for(std::chrono::milliseconds(1));}}
void StandbyReceiver::stop(){if(!impl_||!impl_->started)return;impl_->server.stop();if(impl_->thread.joinable())impl_->thread.join();impl_->started=false;}
unsigned StandbyReceiver::port()const{return impl_->bound_port;}
json StandbyReceiver::status()const{return impl_->status();}
json ship_archive(const std::string& archive_manifest,unsigned port,const std::string& token,Limits limits){
    identity_ok(token);if(!port||port>65535)throw DbError("RECEIVER_ARGUMENT","Loopback port invalid");auto marker=absolute_path(fs::u8path(archive_manifest));auto metadata=inspect_archive(marker.u8string(),limits);auto text=metadata.dump();ReadOnlyFile file(marker.parent_path()/metadata.at("file").get<std::string>());auto size=file.size();if(size>limits.wal_bytes||text.size()>4*1024*1024)throw DbError("RECEIVER_LIMIT","Shipment exceeds budget");std::string body(8+text.size()+static_cast<std::size_t>(size),'\0');put64(reinterpret_cast<unsigned char*>(body.data()),text.size());std::copy(text.begin(),text.end(),body.begin()+8);file.read(0,reinterpret_cast<unsigned char*>(body.data())+8+text.size(),static_cast<std::size_t>(size));
    httplib::Client client("127.0.0.1",static_cast<int>(port));client.set_connection_timeout(2,0);client.set_read_timeout(5,0);client.set_write_timeout(5,0);client.set_max_timeout(static_cast<time_t>(limits.timeout_ms));
    auto response=client.Post("/receive",httplib::Headers{{"X-EduDB-Token",token}},body,"application/octet-stream");if(!response)throw DbError("REPLICATION_OUTCOME_UNKNOWN","Loopback response unavailable; SQL is never retried automatically");json result;try{result=input::parse_json(response->body);if(!result.is_object()||!result.contains("ok")||!result.at("ok").is_boolean())throw std::runtime_error("Invalid receiver response shape");}catch(...){throw DbError("REPLICATION_OUTCOME_UNKNOWN","Receiver response malformed; inspect persisted forced/replayed state");}if(response->status!=200||!result.value("ok",false))throw DbError("REPLICATION_REJECTED",result.dump());return result;
}
}
