#include "fencing.hpp"
#include "portable_sha256.hpp"
#include "managed_block_device.hpp"
#include "media_native.hpp"
#include <mutex>
#include <random>
#ifndef _WIN32
#include <pwd.h>
#endif
#include <iomanip>
#include <sstream>
#include <thread>
#ifdef _WIN32
#include <wincrypt.h>
#endif
namespace edudb::recovery {
namespace {
using namespace media_detail;
#include "recovery_native.inc"
void identity_ok(const std::string& value){if(value.size()!=32||value.find_first_not_of("0123456789abcdef")!=std::string::npos)fail("FENCE_IDENTITY","Fencing identity must be32 lowercase hexadecimal digits");}
void no_reparse(const fs::path& p){if(native_reparse(p))fail("FENCE_PATH","Fence path must not contain symlinks");
#ifdef _WIN32
    auto attributes=GetFileAttributesW(native_path(p).c_str());if(attributes==INVALID_FILE_ATTRIBUTES||(attributes&FILE_ATTRIBUTE_REPARSE_POINT))fail("FENCE_PATH","Fence path is missing or a reparse point");
#endif
}
fs::path default_directory(){
#ifdef _WIN32
    auto size=GetEnvironmentVariableW(L"LOCALAPPDATA",nullptr,0);if(!size||size>32768)fail("FENCE_PATH","LocalAppData unavailable");std::wstring value(size,L'\0');auto actual=GetEnvironmentVariableW(L"LOCALAPPDATA",value.data(),size);if(!actual||actual>=size)fail("FENCE_PATH","LocalAppData changed");value.resize(actual);return fs::path(value)/"EduDBFence";
#else
    // Use the account directory from the OS. Changing HOME or cwd must never
    // give the same writer a second fencing domain.
    struct passwd account{}, *found=nullptr;
    std::vector<char> scratch(16384);
    if(getpwuid_r(geteuid(),&account,scratch.data(),scratch.size(),&found)!=0 ||
       !found || !account.pw_dir || account.pw_dir[0]!='/')
        fail("FENCE_PATH","Cannot determine a stable local account directory");
    return fs::path(account.pw_dir)/".local"/"state"/"EduDBFence";
#endif
}
json load_state(const fs::path& path,const std::string& uuid){ReadOnlyFile file(path,true);auto bytes=file.size();if(bytes<2||bytes>65536)fail("FENCE_CORRUPT","Fence state has invalid length");std::string text(bytes,'\0');file.read(0,reinterpret_cast<unsigned char*>(text.data()),text.size());try{auto state=json::parse(text);auto sum=state.at("checksum").get<std::uint32_t>();state.erase("checksum");if(crc(state.dump())!=sum||state.at("format")!="EduDBFence1"||state.at("uuid")!=uuid||!state.at("epoch").is_number_unsigned()||!state.at("epoch").get<std::uint64_t>())fail("FENCE_CORRUPT","Fence identity/checksum/epoch invalid");identity_ok(state.at("owner_instance_id").get<std::string>());identity_ok(state.at("timeline").get<std::string>());return state;}catch(const DbError&){throw;}catch(...){fail("FENCE_CORRUPT","Malformed shared fence state");}}
void persist(const fs::path& path,json state){state["checksum"]=crc(state.dump());auto bytes=state.dump();auto temp=path;temp+=".tmp-"+new_identity();bool owned=false;try{{File file(temp,true,true);owned=true;file.write(0,reinterpret_cast<const unsigned char*>(bytes.data()),bytes.size());file.sync();}replace_atomic(temp,path,true);owned=false;}catch(...){if(owned){try{native_remove(temp);}catch(...){}}throw;}}
}
struct FencePermit::Impl {std::unique_ptr<media_detail::File> lease;json state;};
struct FenceDomain::Impl {
    std::string uuid;fs::path root,lock_path,state_path;
    Impl(const std::string& database,const std::string& dir):uuid(database),root(absolute_path(dir.empty()?default_directory():fs::u8path(dir))){
        identity_ok(uuid);fs::create_directories(native_path(root));no_reparse(root);
        lock_path=root/("control-fence-"+uuid+".lock");state_path=root/("control-fence-"+uuid+".json");
        if(native_exists(lock_path))no_reparse(lock_path);
        if(native_exists(state_path))no_reparse(state_path);
    }
    std::unique_ptr<File> lease(unsigned wait_ms)const{
        if(wait_ms>5000)fail("FENCE_ARGUMENT","Fence wait bound exceeds5000ms");
        const auto start=std::chrono::steady_clock::now();
        for(;;){try{return std::make_unique<File>(lock_path,true,false,true);}catch(const DbError& e){if(e.code!="MEDIA_LOCKED")throw;if(io_elapsed(start)/1000000>=wait_ms)fail("FENCE_BUSY","Another durable commit or promotion holds the same-machine lease");std::this_thread::sleep_for(std::chrono::milliseconds(5));}}
    }
    bool matches(const json& state,const WriterIdentity& who)const{return !state.contains("departed")&&state.at("owner_instance_id")==who.instance_id&&state.at("timeline")==who.timeline&&state.at("epoch").get<std::uint64_t>()==who.epoch;}
};
FencePermit::FencePermit(std::unique_ptr<Impl> state):impl_(std::move(state)){}
FencePermit::~FencePermit()=default;
FencePermit::FencePermit(FencePermit&&)noexcept=default;
FencePermit& FencePermit::operator=(FencePermit&&)noexcept=default;
const json& FencePermit::state()const{if(!impl_)throw DbError("FENCE_ARGUMENT","Empty fence permit");return impl_->state;}
FenceDomain::FenceDomain(const std::string& database,const std::string& directory):impl_(std::make_unique<Impl>(database,directory)){}
FenceDomain::~FenceDomain()=default;
json FenceDomain::register_primary(const WriterIdentity& who,unsigned wait_ms){identity_ok(who.instance_id);identity_ok(who.timeline);auto lock=impl_->lease(wait_ms);json state;if(!native_exists(impl_->state_path)){if(who.epoch)fail("FENCE_MISSING","Previously registered domain disappeared; refusing to recreate ownership");state={{"format","EduDBFence1"},{"uuid",impl_->uuid},{"epoch",std::uint64_t(1)},{"owner_instance_id",who.instance_id},{"timeline",who.timeline},{"parent_timeline",nullptr},{"fork_lsn",0}};persist(impl_->state_path,state);}else state=load_state(impl_->state_path,impl_->uuid);const bool same=!state.contains("departed")&&state.at("owner_instance_id")==who.instance_id&&state.at("timeline")==who.timeline&&(!who.epoch||state.at("epoch").get<std::uint64_t>()==who.epoch);if(!same)fail("FENCED","Another instance/timeline owns this database UUID");return state;}
FencePermit FenceDomain::acquire_writer(const WriterIdentity& who,unsigned wait_ms){identity_ok(who.instance_id);identity_ok(who.timeline);auto permit=std::make_unique<FencePermit::Impl>();permit->lease=impl_->lease(wait_ms);if(!native_exists(impl_->state_path))fail("FENCE_MISSING","Shared fence state disappeared");permit->state=load_state(impl_->state_path,impl_->uuid);if(!impl_->matches(permit->state,who))fail("FENCED","This instance cannot confirm writes after another timeline was promoted");return FencePermit(std::move(permit));}
FencePermit FenceDomain::promote(const std::string& instance,const std::string& source,std::uint64_t replayed,unsigned wait_ms){identity_ok(instance);identity_ok(source);auto permit=std::make_unique<FencePermit::Impl>();permit->lease=impl_->lease(wait_ms);if(!native_exists(impl_->state_path))fail("FENCE_MISSING","Promotion requires an existing source fencing domain");auto state=load_state(impl_->state_path,impl_->uuid);if(state.contains("departed"))fail("FENCED","Ownership was surrendered to an independent domain");if(state.at("owner_instance_id")==instance&&state.value("parent_timeline",json())==source&&state.at("fork_lsn")==replayed){permit->state=state;return FencePermit(std::move(permit));}if(state.at("timeline")!=source)fail("FENCE_STALE","Standby follows an obsolete source timeline");auto epoch=state.at("epoch").get<std::uint64_t>();if(epoch==std::numeric_limits<std::uint64_t>::max())fail("FENCE_LIMIT","Fence epoch exhausted");state.erase("accepted_ticket");state["epoch"]=epoch+1;state["owner_instance_id"]=instance;state["parent_timeline"]=source;state["timeline"]=new_identity();state["fork_lsn"]=replayed;persist(impl_->state_path,state);permit->state=state;return FencePermit(std::move(permit));}
#include "fencing_handoff.inc"
json FenceDomain::snapshot(unsigned wait_ms)const{auto lease=impl_->lease(wait_ms);if(!native_exists(impl_->state_path))fail("FENCE_MISSING","Shared fence state is absent");auto state=load_state(impl_->state_path,impl_->uuid);state["domain"]=impl_->root.u8string();return state;}
std::string FenceDomain::directory()const{return impl_->root.u8string();}
}
