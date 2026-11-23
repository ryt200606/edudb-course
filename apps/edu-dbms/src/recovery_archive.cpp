#include "recovery_archive.hpp"
#include "media_native.hpp"
#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <sstream>
#include <mutex>
#include <random>
#include <iomanip>
#include <thread>
#include <cstdlib>
#ifdef _WIN32
#include <wincrypt.h>
#endif

namespace edudb::recovery {
namespace {
using namespace media_detail;
constexpr std::size_t PAGE=8192,CHUNK=65536;
#include "recovery_native.inc"
struct Budget {
    Limits limits;std::chrono::steady_clock::time_point start=std::chrono::steady_clock::now();
    explicit Budget(Limits value):limits(value){if(!limits.data_bytes||limits.data_bytes>64ull*1024*1024*1024||!limits.wal_bytes||limits.wal_bytes>512ull*1024*1024||!limits.timeout_ms||limits.timeout_ms>600000||!limits.archive_directory_bytes||limits.archive_directory_bytes>64ull*1024*1024*1024||!limits.archive_files||limits.archive_files>8192)fail("BACKUP_LIMIT","Invalid or excessive backup/archive limits");}
    void tick()const{if(io_elapsed(start)/1000000>limits.timeout_ms)fail("BACKUP_TIMEOUT","Backup/restore exceeded its operation deadline");}
    void phase(const char* name)const{tick();const char* crash=std::getenv("EDUDB_BACKUP_CRASH_AT");if(crash&&std::strcmp(crash,name)==0)std::_Exit(86);if(limits.phase_hook)limits.phase_hook(name);tick();}
};
void uuid_check(const std::string& s){if(s.size()!=32||s.find_first_not_of("0123456789abcdef")!=std::string::npos)fail("BACKUP_IDENTITY","Expected 32 lowercase hex identity characters");}
json sealed(json j){j.erase("checksum");j["checksum"]=crc(j.dump());return j;}
json load_json(const fs::path& path){ReadOnlyFile f(path);auto n=f.size();if(n<2||n>4*1024*1024)fail("BACKUP_MANIFEST","Manifest size out of bounds");std::string text(static_cast<std::size_t>(n),'\0');f.read(0,reinterpret_cast<unsigned char*>(text.data()),text.size());try{auto j=json::parse(text);auto sum=j.at("checksum").get<std::uint32_t>();j.erase("checksum");if(crc(j.dump())!=sum)fail("BACKUP_MANIFEST","Manifest checksum mismatch");return j;}catch(const DbError&){throw;}catch(...){fail("BACKUP_MANIFEST","Malformed backup/archive manifest");}}
void write_json_new(const fs::path& path,const json& value){auto text=sealed(value).dump();auto temp=path;temp+=".tmp-"+new_identity();bool owned=false;try{{File f(temp,true,true);owned=true;f.write(0,reinterpret_cast<const unsigned char*>(text.data()),text.size());f.sync();}
    replace_atomic(temp,path,false);
    owned=false;}catch(...){if(owned){try{native_remove(temp);}catch(...){}}throw;}}
void new_directory(const fs::path& path){std::error_code ec;if(fs::exists(native_path(path),ec)||ec||!fs::is_directory(native_path(path.parent_path()),ec)||ec||!fs::create_directory(native_path(path),ec)||ec)fail("BACKUP_EXISTS","Destination must be an absent child of an existing directory");}
std::array<unsigned char,64> wal_header(const std::string& uuid,std::uint64_t generation,std::uint64_t base){std::array<unsigned char,64> h{};std::memcpy(h.data(),"EDUWAL3",7);std::memcpy(h.data()+8,uuid.data(),32);put64(h.data()+40,generation);put64(h.data()+48,base);put32(h.data()+60,crc(h.data(),h.size()));return h;}
bool page_valid(std::vector<unsigned char> p,std::uint64_t id){if(p.size()!=PAGE)return false;auto sum=get32(p.data()+60);put32(p.data()+60,0);auto slots=get32(p.data()+32)&65535u;auto lower=get32(p.data()+34)&65535u;auto upper=get32(p.data()+36)&65535u;auto type=get32(p.data()+6)&65535u;if(!(get32(p.data())==0x45445047&&p[4]==3&&p[5]==0&&get64(p.data()+8)==id&&type>=1&&type<=7&&lower==64+slots*8&&lower<=upper&&upper<=PAGE&&sum==crc(p.data(),p.size())))return false;std::vector<std::pair<unsigned,unsigned>> occupied;for(unsigned n=0;n<slots;++n){auto at=64+n*8,offset=get32(p.data()+at)&65535u,length=get32(p.data()+at+2)&65535u;if(!get32(p.data()+at+4))return false;if(length){if(offset<upper||offset+length>PAGE)return false;occupied.push_back({offset,offset+length});}}std::sort(occupied.begin(),occupied.end());for(std::size_t n=1;n<occupied.size();++n)if(occupied[n-1].second>occupied[n].first)return false;return true;}
std::uint64_t number(const json& j,const char* field,std::uint64_t low,std::uint64_t high){if(!j.contains(field)||!j[field].is_number_integer()||(j[field].is_number_integer()&&!j[field].is_number_unsigned()&&j[field].get<std::int64_t>()<0))fail("BACKUP_MANIFEST",std::string("Invalid integer field: ")+field);auto n=j[field].get<std::uint64_t>();if(n<low||n>high)fail("BACKUP_MANIFEST",std::string("Out-of-range field: ")+field);return n;}
void validate_control(const json& c){auto version=c.value("version",0);if(c.value("format",std::string())!="EduDB"||(version!=3&&version!=4)||c.value("page_size",0)!=PAGE||c.value("page_format",version==3?3:0)!=3||c.value("control_version",version)!=version)fail("BACKUP_MANIFEST","Supported control versions3/4 require physical page_format3; control4 must declare it explicitly");uuid_check(c.at("uuid").get<std::string>());if(c.value("recovery_mode",std::string("REDO_PRIVATE"))!="REDO_PRIVATE")fail("BACKUP_MODE","Only REDO_PRIVATE can use this archive format");number(c,"wal_generation",1,std::numeric_limits<std::uint64_t>::max());number(c,"checkpoint_lsn",0,std::numeric_limits<std::uint64_t>::max());number(c,"next_page_id",2,(1ull<<48)-1);number(c,"next_tx_id",1,std::numeric_limits<std::uint64_t>::max()-1);}
void validate_data(DataDevice& data,const json& control,std::uint64_t end,Budget& budget){const auto bytes=data.size();if(bytes<2*PAGE||bytes%PAGE||bytes>budget.limits.data_bytes)fail("BACKUP_LIMIT","Logical data must have complete pages within budget");for(std::uint64_t at=0;at<bytes;at+=PAGE){budget.tick();std::vector<unsigned char> p(PAGE);data.read(at,p.data(),p.size());const bool zero=std::all_of(p.begin(),p.end(),[](unsigned char b){return !b;});if(zero&&at>=2*PAGE)continue;if(!page_valid(p,at/PAGE)||get64(p.data()+16)>end)fail("BACKUP_PAGE","Invalid page or data page newer than baseline end");if(at==0){if((get32(p.data()+6)&65535u)!=1||(get32(p.data()+32)&65535u)!=1)fail("BACKUP_IDENTITY","Invalid backup superblock");auto offset=get32(p.data()+64)&65535u,length=get32(p.data()+66)&65535u;try{auto super=json::parse(p.begin()+offset,p.begin()+offset+length);if(super.at("uuid")!=control.at("uuid")||super.value("format_version",0)!=3||super.value("page_size",0)!=PAGE)fail("BACKUP_IDENTITY","Superblock identity differs from backup control");}catch(const DbError&){throw;}catch(...){fail("BACKUP_IDENTITY","Malformed superblock identity");}}if(at==PAGE&&(get32(p.data()+6)&65535u)!=2)fail("BACKUP_PAGE","Page one is not the catalog");}}
struct TransactionImage {std::uint64_t begin=0,end=0,tx=0;std::vector<unsigned char> records;};
struct Log {std::string uuid;std::uint64_t generation=0,base=0,end=0;std::vector<TransactionImage> transactions;std::vector<unsigned char> bytes;};
Log parse_log(DataDevice& file,const std::string& expected,Budget& budget){
    auto size=file.size();if(size<64||size>budget.limits.wal_bytes)fail("ARCHIVE_LIMIT","WAL size is outside the archive budget");
    Log log;log.bytes.resize(static_cast<std::size_t>(size));for(std::size_t at=0;at<log.bytes.size();){budget.tick();auto n=std::min(CHUNK,log.bytes.size()-at);file.read(at,log.bytes.data()+at,n);at+=n;}
    auto h=std::vector<unsigned char>(log.bytes.begin(),log.bytes.begin()+64);auto sum=get32(h.data()+60);put32(h.data()+60,0);
    log.uuid=std::string(reinterpret_cast<const char*>(h.data()+8),32);uuid_check(log.uuid);log.generation=get64(h.data()+40);log.base=log.end=get64(h.data()+48);
    if(std::memcmp(h.data(),"EDUWAL3",7)||sum!=crc(h.data(),h.size())||log.uuid!=expected||!log.generation)fail("ARCHIVE_WAL","WAL header identity/generation/checksum mismatch");
    std::uint64_t previous=log.base;std::uint32_t digest=0,pages=0;TransactionImage pending;
    for(std::size_t at=64;at<log.bytes.size();){
        budget.tick();if(log.bytes.size()-at<48)fail("ARCHIVE_WAL","Archive source contains an unfinished header");
        std::array<unsigned char,48> r{};std::copy_n(log.bytes.data()+at,48,r.data());auto hc=get32(r.data()+36);put32(r.data()+36,0);auto length=get32(r.data()+4),type=get32(r.data()+8);
        if(get32(r.data())!=0x45445752||get32(r.data()+12)!=3||hc!=crc(r.data(),r.size())||type<1||type>3||(type==1&&length)||(type==2&&length!=PAGE+8)||(type==3&&length!=8))fail("ARCHIVE_WAL","Invalid fixed WAL record header");
        if(length>log.bytes.size()-at-48)fail("ARCHIVE_WAL","Archive source contains an unfinished payload");
        auto* payload=log.bytes.data()+at+48;
        if(get32(r.data()+32)!=crc(payload,length))fail("ARCHIVE_WAL","Complete WAL payload checksum mismatch");
        auto lsn=get64(r.data()+16),tx=get64(r.data()+24);if(!tx||previous==std::numeric_limits<std::uint64_t>::max()||lsn!=previous+1)fail("ARCHIVE_GAP","WAL LSNs are not contiguous");previous=lsn;
        if(type==1){if(pending.tx)fail("ARCHIVE_WAL","Nested WAL transaction");pending={lsn,0,tx,{}};pages=0;digest=0;}
        else if(!pending.tx||pending.tx!=tx)fail("ARCHIVE_WAL","Record lacks its matching BEGIN");
        if(type==2){auto id=get64(payload);std::vector<unsigned char> image(payload+8,payload+8+PAGE);if(id<1||id>=(1ull<<48)||!page_valid(image,id)||get64(image.data()+16)!=lsn)fail("ARCHIVE_WAL","Invalid full-page WAL image");digest=((digest<<1)|(digest>>31))^crc(image.data(),image.size());if(++pages>131072)fail("ARCHIVE_LIMIT","Transaction image count is too large");}
        pending.records.insert(pending.records.end(),log.bytes.begin()+at,log.bytes.begin()+at+48+length);
        if(type==3){if(get32(payload)!=pages||get32(payload+4)!=digest)fail("ARCHIVE_WAL","COMMIT image digest/count mismatch");pending.end=lsn;log.end=lsn;log.transactions.push_back(std::move(pending));pending={};}
        at+=48+length;
    }
    if(pending.tx)fail("ARCHIVE_WAL","Archive source has a transaction without COMMIT");
    return log;
}
using SourceFile=ReadOnlyFile;
json file_proof(DataDevice& source,Budget& budget,std::uint64_t maximum){auto n=source.size();if(n>maximum)fail("BACKUP_LIMIT","Input file exceeds its budget");json sums=json::array();Sha256 digest;std::vector<unsigned char> b(CHUNK);for(std::uint64_t at=0;at<n;){budget.tick();auto count=static_cast<std::size_t>(std::min<std::uint64_t>(CHUNK,n-at));source.read(at,b.data(),count);sums.push_back(crc(b.data(),count));digest.add(b.data(),count);at+=count;}if(source.size()!=n)fail("BACKUP_CHANGED","Source file length changed while hashing");return {{"bytes",n},{"chunk_bytes",CHUNK},{"chunk_crc32",sums},{"sha256",digest.finish()}};}
json file_proof(const fs::path& p,Budget& b,std::uint64_t maximum){SourceFile source(p);return file_proof(source,b,maximum);}
std::uint64_t file_budget(const std::string& name,const Limits& limits){return name=="data.pages"?limits.data_bytes:name=="control.json"?4ull*1024*1024:limits.wal_bytes;}
void archive_quota(const fs::path& directory,const std::string& identity,std::uint64_t incoming,std::size_t new_files,Budget& budget){std::uint64_t total=0;std::size_t files=0;auto account=[&](const fs::path& file){budget.tick();if(native_reparse(file)||!native_regular(file))fail("BACKUP_PATH","Archive quota found a foreign/reparse entry");SourceFile input(file);auto bytes=input.size();if(total>budget.limits.archive_directory_bytes||bytes>budget.limits.archive_directory_bytes-total)fail("ARCHIVE_LIMIT","Retained archive bytes exceed budget");total+=bytes;if(++files>budget.limits.archive_files)fail("ARCHIVE_LIMIT","Retained archive file count exceeds budget");};for(const auto& entry:native_entries(directory)){if(native_directory(entry)){if(native_reparse(entry)||entry.filename().u8string().rfind("receive-stage-",0)!=0)fail("BACKUP_PATH","Archive directory contains an unknown subdirectory");auto owner=load_json(entry/"control-stage-owner.json");if(owner.at("format")!="EduDBReceiveStage1"||owner.at("uuid")!=identity||entry.filename().u8string()!="receive-stage-"+owner.at("id").get<std::string>())fail("BACKUP_PATH","Foreign staging directory in archive quota");for(const auto& child:native_entries(entry,16))account(child);}else account(entry);}if(total>budget.limits.archive_directory_bytes||incoming>budget.limits.archive_directory_bytes-total||new_files>budget.limits.archive_files-files)fail("ARCHIVE_LIMIT","New immutable archive would exceed retained directory budget");}
bool same_bytes(DataDevice& source,const fs::path& path,Budget& budget){SourceFile other(path);const auto size=source.size();if(other.size()!=size)return false;std::vector<unsigned char> a(CHUNK),b(CHUNK);for(std::uint64_t at=0;at<size;){budget.tick();auto n=static_cast<std::size_t>(std::min<std::uint64_t>(CHUNK,size-at));source.read(at,a.data(),n);other.read(at,b.data(),n);if(!std::equal(a.begin(),a.begin()+n,b.begin()))return false;at+=n;}return source.size()==size;}
void copy_bytes(DataDevice& source,const fs::path& target,Budget& budget,std::uint64_t maximum){const auto size=source.size();if(size>maximum)fail("BACKUP_LIMIT","Input exceeds copy budget");File out(target,true,true);std::vector<unsigned char> b(CHUNK);for(std::uint64_t at=0;at<size;){budget.tick();auto count=static_cast<std::size_t>(std::min<std::uint64_t>(CHUNK,size-at));source.read(at,b.data(),count);out.write(at,b.data(),count);at+=count;}if(source.size()!=size)fail("BACKUP_CHANGED","Source file changed length while copying");budget.phase("copy_before_sync");out.sync();}
json checked_backup(const fs::path& path,Budget& budget){
    auto j=load_json(path/"backup-complete.json");if(j.value("format",std::string())!="EduDBPhysicalBackup1"||j.value("state",std::string())!="COMPLETE")fail("BACKUP_MANIFEST","Not a complete physical backup");uuid_check(j.at("uuid").get<std::string>());uuid_check(j.at("timeline").get<std::string>());
    const std::set<std::string> names={"control.json","data.pages","wal-1.log"};if(!j.at("files").is_object()||j["files"].size()!=names.size())fail("BACKUP_MANIFEST","Unexpected backup file set");
    for(const auto& name:names){if(!j["files"].contains(name)||native_reparse(path/name)||!native_regular(path/name))fail("BACKUP_MANIFEST","Missing/foreign backup file");auto actual=file_proof(path/name,budget,file_budget(name,budget.limits));if(actual!=j["files"][name])fail("BACKUP_CHECKSUM","Backup file length or chunk checksum mismatch: "+name);}
    auto c=load_json(path/"control.json");validate_control(c);if(c.at("uuid")!=j.at("uuid")||c.at("checkpoint_lsn")!=j.at("end_lsn")||c.at("wal_generation")!=1||c.at("timeline")!=j.at("timeline")||c.at("data_layout")!=json{{"kind","single"}}||c.value("recovery_role",std::string())!="RESTORED_READ_ONLY")fail("BACKUP_MANIFEST","Backup control and completion marker disagree");uuid_check(c.at("instance_id").get<std::string>());number(j,"source_wal_generation",1,std::numeric_limits<std::uint64_t>::max());number(j,"source_wal_base_lsn",0,j.at("end_lsn").get<std::uint64_t>());SourceFile physical(path/"data.pages");validate_data(physical,c,j.at("end_lsn").get<std::uint64_t>(),budget);if(c.at("next_page_id").get<std::uint64_t>()<physical.size()/PAGE)fail("BACKUP_MANIFEST","Backup allocation high-water is below physical length");
    if(j.value("page_format",0)!=3||j.at("source_control_version")!=c.at("version"))fail("BACKUP_MANIFEST","Backup manifest control/page versions disagree");
    SourceFile wal(path/"wal-1.log");auto parsed=parse_log(wal,j.at("uuid").get<std::string>(),budget);if(parsed.base!=j.at("end_lsn").get<std::uint64_t>()||parsed.end!=parsed.base||!parsed.transactions.empty())fail("BACKUP_MANIFEST","Backup WAL is not the promised empty baseline");return j;
}
}
std::unique_ptr<DataDevice> open_read_only(const std::string& filename){return std::make_unique<ReadOnlyFile>(fs::u8path(filename));}
json inspect_wal(DataDevice& wal,const std::string& uuid,Limits limits){Budget budget(limits);auto log=parse_log(wal,uuid,budget);json commits=json::array();for(const auto& t:log.transactions)commits.push_back({{"tx_id",t.tx},{"begin_lsn",t.begin},{"commit_lsn",t.end}});return {{"uuid",log.uuid},{"generation",log.generation},{"base_lsn",log.base},{"end_lsn",log.end},{"commits",commits}};}
json create_backup(DataDevice& data,const json& source,std::uint64_t end,const std::string& directory,Limits limits){
    Budget budget(limits);validate_control(source);auto uuid=source.at("uuid").get<std::string>();uuid_check(uuid);auto timeline=source.value("timeline",uuid);uuid_check(timeline);if(source.at("checkpoint_lsn").get<std::uint64_t>()>end)fail("BACKUP_MANIFEST","Backup end precedes source WAL base");
    auto bytes=data.size();if(bytes<2*PAGE||bytes%PAGE||bytes>limits.data_bytes)fail("BACKUP_LIMIT","Logical data file must contain complete pages within budget");
    auto path=absolute_path(fs::u8path(directory));new_directory(path);write_json_new(path/"backup-incomplete.json",{{"format","EduDBPhysicalBackup1"},{"state","INCOMPLETE"},{"uuid",uuid}});
    // A baseline cannot contain pages newer than its own recoverable start.
    validate_data(data,source,end,budget);
    copy_bytes(data,path/"data.pages",budget,limits.data_bytes);budget.phase("backup_after_data_copy");auto control=source;control.erase("checksum");control["data_layout"]={{"kind","single"}};control["recovery_mode"]="REDO_PRIVATE";control["wal_generation"]=1;control["checkpoint_lsn"]=end;control["timeline"]=timeline;control["recovery_role"]="RESTORED_READ_ONLY";control["instance_id"]=new_identity();control["fence_epoch"]=0;control["next_page_id"]=std::max(control.at("next_page_id").get<std::uint64_t>(),bytes/PAGE);write_json_new(path/"control.json",control);{SourceFile copied(path/"data.pages");validate_data(copied,control,end,budget);}
    auto h=wal_header(uuid,1,end);{File f(path/"wal-1.log",true,true);f.write(0,h.data(),h.size());f.sync();}
    if(data.size()!=bytes)fail("BACKUP_CHANGED","Source data length changed across baseline copy");
    json files=json::object();for(const auto& name:{"control.json","data.pages","wal-1.log"})files[name]=file_proof(path/name,budget,file_budget(name,limits));
    json manifest={{"format","EduDBPhysicalBackup1"},{"state","COMPLETE"},{"uuid",uuid},{"timeline",timeline},{"end_lsn",end},{"source_control_version",source.at("version")},{"page_format",3},{"source_wal_generation",source.at("wal_generation")},{"source_wal_base_lsn",source.at("checkpoint_lsn")},{"source_instance_id",source.value("instance_id",std::string())},{"source_data_layout",source.value("data_layout",json{{"kind","single"}})},{"layout_conversion","logical page image stored as SINGLE"},{"consistency","writes paused; all active transactions ended and committed data synchronized by caller"},{"files",files}};budget.phase("backup_before_complete");write_json_new(path/"backup-complete.json",manifest);budget.phase("backup_after_complete");return manifest;
}
json archive_wal(DataDevice& wal,const std::string& uuid,const std::string& timeline,const std::string& directory,Limits limits){
    uuid_check(uuid);uuid_check(timeline);Budget budget(limits);auto log=parse_log(wal,uuid,budget);auto path=absolute_path(fs::u8path(directory));fs::create_directories(native_path(path));auto token=std::to_string(log.generation)+"-"+std::to_string(log.end)+"-"+timeline;auto filename="wal-archive-"+token+".log";auto marker=path/("control-archive-"+token+".json");auto proof=file_proof(wal,budget,limits.wal_bytes);
    json metadata={{"format","EduDBWalArchive1"},{"state","COMPLETE"},{"uuid",uuid},{"timeline",timeline},{"generation",log.generation},{"base_lsn",log.base},{"end_lsn",log.end},{"file",filename},{"proof",proof}};
    if(native_exists(marker)){if(load_json(marker)!=metadata||file_proof(path/filename,budget,limits.wal_bytes)!=proof||!same_bytes(wal,path/filename,budget))fail("ARCHIVE_FORK","Same archive name has different history");return metadata;}
    archive_quota(path,uuid,(native_exists(path/filename)?0:wal.size())+sealed(metadata).dump().size(),native_exists(path/filename)?1:2,budget);
    if(native_exists(path/filename)){if(file_proof(path/filename,budget,limits.wal_bytes)!=proof||!same_bytes(wal,path/filename,budget))fail("ARCHIVE_FORK","Unpublished archive file differs");File unpublished(path/filename);budget.phase("copy_before_sync");unpublished.sync();}else copy_bytes(wal,path/filename,budget,limits.wal_bytes);
    if(file_proof(path/filename,budget,limits.wal_bytes)!=proof||!same_bytes(wal,path/filename,budget))fail("ARCHIVE_CHANGED","Archive source changed during copying");
    budget.phase("archive_before_complete");write_json_new(marker,metadata);budget.phase("archive_after_complete");return metadata;
}
json inspect_backup(const std::string& directory,Limits limits){Budget budget(limits);return checked_backup(absolute_path(fs::u8path(directory)),budget);}
void check_archive_capacity(const std::string& directory,const std::string& database_uuid,std::uint64_t additional_bytes,std::size_t additional_files,Limits limits){Budget budget(limits);uuid_check(database_uuid);archive_quota(absolute_path(fs::u8path(directory)),database_uuid,additional_bytes,additional_files,budget);}
json inspect_archive(const std::string& archive_manifest,Limits limits){Budget budget(limits);auto marker=absolute_path(fs::u8path(archive_manifest));auto m=load_json(marker);if(m.value("format",std::string())!="EduDBWalArchive1"||m.value("state",std::string())!="COMPLETE")fail("ARCHIVE_MANIFEST","Incomplete archive marker");uuid_check(m.at("uuid").get<std::string>());uuid_check(m.at("timeline").get<std::string>());auto filename=m.at("file").get<std::string>();if(filename.rfind("wal-archive-",0)!=0||filename.size()<16||fs::u8path(filename).filename().u8string()!=filename||filename.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-.")!=std::string::npos||filename.substr(filename.size()-4)!=".log")fail("ARCHIVE_MANIFEST","Unsafe archive filename");auto path=marker.parent_path()/filename;if(file_proof(path,budget,limits.wal_bytes)!=m.at("proof"))fail("ARCHIVE_CHECKSUM","Archive proof mismatch");SourceFile input(path);auto log=parse_log(input,m.at("uuid").get<std::string>(),budget);if(log.generation!=number(m,"generation",1,std::numeric_limits<std::uint64_t>::max())||log.base!=number(m,"base_lsn",0,std::numeric_limits<std::uint64_t>::max())||log.end!=number(m,"end_lsn",0,std::numeric_limits<std::uint64_t>::max()))fail("ARCHIVE_MANIFEST","Archive boundaries differ");return m;}
json restore_backup(const std::string& directory,const std::vector<std::string>& archives,std::uint64_t target,const std::string& destination,Limits limits){
#include "recovery_restore.inc"

}
json replay_archive(DataDevice& data,DataDevice& active_wal,const json& control,std::uint64_t current_end_lsn,const std::string& archive_manifest,Limits limits){
#include "recovery_replay.inc"
}
} // namespace edudb::recovery
