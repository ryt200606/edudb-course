#include "aries_recovery.hpp"
#include "managed_block_device.hpp"
#include "io.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <utility>
#include <vector>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
namespace edudb {namespace aries {
namespace {
namespace fs=std::filesystem;using Clock=std::chrono::steady_clock;
constexpr std::uint32_t page_magic=0x41525047,record_magic=0x41525734;
constexpr std::size_t log_header_size=64,record_header_size=80;
void put32(unsigned char* p,std::uint32_t n){for(unsigned i=0;i<4;++i)p[i]=static_cast<unsigned char>(n>>(i*8));}
void put64(unsigned char* p,std::uint64_t n){for(unsigned i=0;i<8;++i)p[i]=static_cast<unsigned char>(n>>(i*8));}
std::uint32_t get32(const unsigned char* p){std::uint32_t n=0;for(int i=3;i>=0;--i)n=(n<<8)|p[i];return n;}
std::uint64_t get64(const unsigned char* p){std::uint64_t n=0;for(int i=7;i>=0;--i)n=(n<<8)|p[i];return n;}
std::uint32_t crc(const unsigned char* p,std::size_t n){static const auto table=[]{std::array<std::uint32_t,256> a{};for(std::uint32_t i=0;i<256;++i){auto c=i;for(int j=0;j<8;++j)c=(c&1)?0xedb88320u^(c>>1):c>>1;a[i]=c;}return a;}();auto c=0xffffffffu;for(std::size_t i=0;i<n;++i)c=table[(c^p[i])&255]^(c>>8);return c^0xffffffffu;}
void crash(const char* hook){const char* at=std::getenv("EDUDB_ARIES_CRASH_AT");if(at&&std::strcmp(at,hook)==0)std::_Exit(86);}
enum class Type:std::uint32_t {Begin=1,Update=2,Commit=3,Abort=4,End=5,Clr=6,BeginCheckpoint=7,EndCheckpoint=8};
const char* type_name(Type t){switch(t){case Type::Begin:return "BEGIN";case Type::Update:return "UPDATE";case Type::Commit:return "COMMIT";case Type::Abort:return "ABORT";case Type::End:return "END";case Type::Clr:return "CLR";case Type::BeginCheckpoint:return "BEGIN_CHECKPOINT";case Type::EndCheckpoint:return "END_CHECKPOINT";}return "INVALID";}
bool page_record(Type t){return t==Type::Update||t==Type::Clr;}
#include "aries_file.inc"
}
PageCodec default_page_codec(){
    PageCodec codec;
    codec.lsn=[](const Page& p){return get64(p.data()+16);};
    codec.stamp=[](Page& p,Lsn lsn){put64(p.data()+16,lsn);put32(p.data()+60,0);put32(p.data()+60,crc(p.data(),p.size()));};
    codec.empty=[stamp=codec.stamp](PageId id){Page p{};put32(p.data(),page_magic);put32(p.data()+4,1);put64(p.data()+8,id);stamp(p,0);return p;};
    codec.valid=[](const Page& p,PageId id){if(get32(p.data())!=page_magic||get32(p.data()+4)!=1||get64(p.data()+8)!=id)return false;auto copy=p;auto expected=get32(copy.data()+60);put32(copy.data()+60,0);return crc(copy.data(),copy.size())==expected;};
    return codec;
}
std::unique_ptr<DataDevice> open_native_data_device(const std::string& path){return std::make_unique<NativeFile>(fs::u8path(path),true);}

struct Manager::Impl {
    enum class Status {Active,Committed,Aborting};
    struct Tx {Status status=Status::Active;Lsn last=0;bool read_only=false;std::vector<std::pair<std::string,Lsn>> savepoints;std::uint64_t wal_bytes=0;std::set<PageId> dirty{};bool compensation=false;std::uint64_t credit_left=0,update_left=0;};
    struct Record {Type type;Lsn lsn=0;TxId tx=0;Lsn prev=0;PageId page=0;Lsn undo_next=0;std::uint64_t offset=0;std::uint32_t length=0;};
    struct Frame {Page image;bool dirty=false;std::uint64_t tick=0;};
    fs::path directory;DataDevice& data;std::string uuid;PageCodec codec;Options options;
    std::unique_ptr<NativeFile> directory_lock,wal;
    mutable std::mutex mutex;std::map<TxId,Tx> transactions;std::map<PageId,TxId> owners;
    std::map<PageId,Frame> frames;std::map<PageId,Lsn> dpt,stable_page_lsn;std::map<Lsn,Record> index;
    std::set<TxId> used_transactions;Lsn last_lsn=0,flushed_lsn=0,checkpoint_begin=0,checkpoint_end=0;
    std::uint64_t wal_end=64,forced_end=64,tick=0,max_tx=0,redo_pages=0,undo_pages=0,clr_records=0,flushes=0,steals=0,forces=0,torn_repairs=0;
    bool failed=false,tail_truncated=false,checkpoint_running=false,recovering=false;
    std::uint64_t external_reserved=0;
    json recovery=json::object();
    Impl(const std::string& path,DataDevice& device,std::string identity,PageCodec c,Options opts)
        :directory(fs::absolute(fs::u8path(path)).lexically_normal()),data(device),uuid(std::move(identity)),codec(std::move(c)),options(std::move(opts)) {
        if(uuid.size()!=32||uuid.find_first_not_of("0123456789abcdefABCDEF")!=std::string::npos)throw DbError("ARGUMENT","ARIES database UUID must contain 32 hexadecimal digits");
        if(!codec.empty||!codec.valid||!codec.lsn||!codec.stamp||options.buffer_frames<1||options.buffer_frames>65536||options.wal_limit_bytes<65536)throw DbError("ARGUMENT","Invalid ARIES codec/buffer/log configuration");
        fs::create_directories(directory);directory_lock=std::make_unique<NativeFile>(directory/"control-aries.lock",true,false,true);
        const auto log=directory/"wal-aries.log";const bool fresh=!fs::exists(log);
        if(fresh&&(fs::exists(directory/"control-aries-master.json")||fs::exists(directory/"control-aries-durable.json")))throw DbError("CORRUPT_ARIES_CONTROL","ARIES control exists without its WAL");
        wal=std::make_unique<NativeFile>(log,true);
        if(fresh){auto h=log_header();wal->write(0,h.data(),h.size());wal->sync();atomic_control(directory/"control-aries-durable.json",watermark(0,64));atomic_control(directory/"control-aries-master.json",master(0,0));}
        validate_header();auto durable=read_control(directory/"control-aries-durable.json");validate_control(durable);
        flushed_lsn=durable.at("forced_lsn").get<Lsn>();forced_end=durable.at("forced_end").get<std::uint64_t>();
        auto control=read_control(directory/"control-aries-master.json");validate_control(control);checkpoint_begin=control.at("checkpoint_begin").get<Lsn>();checkpoint_end=control.at("checkpoint_end").get<Lsn>();
        if(data.size()<control.value("data_bytes",std::uint64_t(0)))throw DbError("MEDIA_RECOVERY_REQUIRED","Data device is shorter than the durable checkpoint baseline");
        scan_log();recover();
    }
    std::array<unsigned char,64> log_header(){std::array<unsigned char,64> h{};std::memcpy(h.data(),"EDARW01",7);std::memcpy(h.data()+8,uuid.data(),32);put64(h.data()+40,1);put64(h.data()+48,page_size);put32(h.data()+56,1);put32(h.data()+60,crc(h.data(),h.size()));return h;}
    json identity(){return {{"format","EduDB-ARIES-PAGE"},{"version",1},{"database_uuid",uuid},{"wal_epoch",1},{"page_size",page_size}};}
    json watermark(Lsn lsn,std::uint64_t end){auto j=identity();j["forced_lsn"]=lsn;j["forced_end"]=end;return j;}
    json master(Lsn begin,Lsn end){auto j=identity();j["checkpoint_begin"]=begin;j["checkpoint_end"]=end;j["data_bytes"]=data.size();return j;}
    void validate_control(const json& j){if(j.value("format",std::string())!="EduDB-ARIES-PAGE"||j.value("version",0)!=1||j.value("database_uuid",std::string())!=uuid||j.value("wal_epoch",0)!=1||j.value("page_size",0)!=page_size)throw DbError("CORRUPT_ARIES_CONTROL","ARIES control identity/version mismatch");}
    void validate_header(){if(wal->size()<64)throw DbError("CORRUPT_ARIES_WAL","Truncated ARIES WAL header");std::array<unsigned char,64> h{};wal->read(0,h.data(),h.size());auto expected=get32(h.data()+60);put32(h.data()+60,0);if(std::memcmp(h.data(),"EDARW01",7)||std::string(reinterpret_cast<char*>(h.data()+8),32)!=uuid||get64(h.data()+40)!=1||get64(h.data()+48)!=page_size||get32(h.data()+56)!=1||crc(h.data(),h.size())!=expected)throw DbError("CORRUPT_ARIES_WAL","ARIES WAL header identity/checksum mismatch");}
    void require(TxId id){if(failed)throw DbError("RECOVERY_REQUIRED","ARIES I/O failed; restart/recovery required");if(!transactions.count(id))throw DbError("NO_TRANSACTION","Unknown ARIES transaction");}
    void inject(const char* point){if(options.io_fault)options.io_fault(point);}
    std::vector<unsigned char> payload(const Record& r){std::vector<unsigned char> bytes(r.length);if(r.length)wal->read(r.offset+record_header_size,bytes.data(),bytes.size());return bytes;}
    Page image(const Record& r,bool before=false){auto bytes=payload(r);Page p{};auto offset=before?0:r.type==Type::Update?page_size:0;std::copy_n(bytes.data()+offset,page_size,p.data());return p;}
    Lsn append(Type type,TxId tx=0,PageId page=0,Lsn undo_next=0,std::vector<unsigned char> bytes={}) {
        if(failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");
        const Lsn lsn=last_lsn+1;Lsn prev=tx?transactions.at(tx).last:0;
        if(lsn==0||wal_end>options.wal_limit_bytes||record_header_size+bytes.size()>options.wal_limit_bytes-wal_end)throw DbError("ARIES_LOG_LIMIT","ARIES WAL budget exceeded before append; archive/retention integration required");
        const bool compensation=tx&&transactions.at(tx).compensation;
        const auto record_bytes=record_header_size+bytes.size();
        if(!recovering&&compensation&&record_bytes>transactions.at(tx).credit_left)throw DbError("COMPENSATION_CREDIT","Physical compensation exhausted its pre-reserved credit");
        if(!recovering&&!compensation&&(external_reserved>options.wal_limit_bytes-wal_end||record_bytes>options.wal_limit_bytes-wal_end-external_reserved))throw DbError("ARIES_LOG_RESERVED","Normal record cannot consume logical UNDO reservation");
        const bool online_allocation=type==Type::Begin||type==Type::Update||type==Type::BeginCheckpoint||type==Type::EndCheckpoint;
        const auto online=(options.wal_limit_bytes-external_reserved)/2;
        if(!recovering&&!compensation&&online_allocation&&(wal_end>online||record_bytes>online-wal_end))throw DbError("ARIES_LOG_LIMIT","Online WAL allowance exhausted; physical and logical undo remains reserved");
        if((type==Type::Update&&bytes.size()!=2*page_size)||(type==Type::Clr&&bytes.size()!=page_size))throw DbError("INTERNAL_ERROR","ARIES page-record payload has wrong size");
        if(page_record(type)){auto offset=type==Type::Update?page_size:0;Page after{};std::copy_n(bytes.data()+offset,page_size,after.data());codec.stamp(after,lsn);std::copy(after.begin(),after.end(),bytes.begin()+static_cast<std::ptrdiff_t>(offset));}
        std::array<unsigned char,80> h{};put32(h.data(),record_magic);put32(h.data()+4,static_cast<std::uint32_t>(type));put32(h.data()+8,static_cast<std::uint32_t>(bytes.size()));put64(h.data()+16,lsn);put64(h.data()+24,tx);put64(h.data()+32,prev);put64(h.data()+40,page);put64(h.data()+48,undo_next);put32(h.data()+56,crc(bytes.data(),bytes.size()));put32(h.data()+60,1);put32(h.data()+12,crc(h.data(),h.size()));
        // Once native append starts, both I/O and in-memory bookkeeping must
        // fail-stop together. A successfully written COMMIT cannot disappear
        // from the authority merely because index/accounting allocation threw.
        try{
            inject("before_wal_write");wal->write(wal_end,h.data(),h.size());if(!bytes.empty())wal->write(wal_end+h.size(),bytes.data(),bytes.size());
            inject("after_wal_write_before_bookkeeping");
            index.emplace(lsn,Record{type,lsn,tx,prev,page,undo_next,wal_end,static_cast<std::uint32_t>(bytes.size())});wal_end+=h.size()+bytes.size();last_lsn=lsn;
            if(tx){transactions.at(tx).last=lsn;transactions.at(tx).wal_bytes+=h.size()+bytes.size();max_tx=std::max(max_tx,tx);}if(type==Type::Clr)++clr_records;
            if(!recovering&&compensation){transactions.at(tx).credit_left-=record_bytes;external_reserved-=record_bytes;}
            return lsn;
        }catch(...){failed=true;throw;}
    }
    void force(Lsn through){if(through<=flushed_lsn)return;if(through>last_lsn)throw DbError("WAL_ORDER","Cannot force a nonexistent ARIES record");try{inject("before_wal_sync");wal->sync();inject("before_durable_control_write");atomic_control(directory/"control-aries-durable.json",watermark(last_lsn,wal_end));flushed_lsn=last_lsn;forced_end=wal_end;++forces;}catch(...){failed=true;throw;}}
    void scan_log(){
        auto size=wal->size();if(size<forced_end||forced_end<64)throw DbError("CORRUPT_ARIES_WAL","WAL shorter than independent durable-end watermark");
        std::uint64_t offset=64;Lsn previous=0;std::map<TxId,std::pair<Lsn,Status>> chain;bool tail=false;
        while(offset<size){if(size-offset<80){tail=true;break;}std::array<unsigned char,80> h{};wal->read(offset,h.data(),h.size());auto expected=get32(h.data()+12);put32(h.data()+12,0);if(crc(h.data(),h.size())!=expected||get32(h.data())!=record_magic||get32(h.data()+60)!=1)throw DbError("CORRUPT_ARIES_WAL","ARIES record header checksum/version mismatch");
            auto raw_type=get32(h.data()+4),length=get32(h.data()+8);if(raw_type<1||raw_type>8)throw DbError("CORRUPT_ARIES_WAL","Unknown ARIES record type");auto type=static_cast<Type>(raw_type);
            if((type==Type::Update&&length!=2*page_size)||(type==Type::Clr&&length!=page_size)||(type==Type::EndCheckpoint&&(length<2||length>16*1024*1024))||(!page_record(type)&&type!=Type::EndCheckpoint&&length!=0))throw DbError("CORRUPT_ARIES_WAL","ARIES record has invalid fixed payload size");
            if(offset+80+length>size){tail=true;break;}std::vector<unsigned char> bytes(length);if(length)wal->read(offset+80,bytes.data(),bytes.size());if(get32(h.data()+56)!=crc(bytes.data(),bytes.size()))throw DbError("CORRUPT_ARIES_WAL","Complete ARIES record payload checksum mismatch");
            Record r{type,get64(h.data()+16),get64(h.data()+24),get64(h.data()+32),get64(h.data()+40),get64(h.data()+48),offset,length};if(r.lsn!=previous+1)throw DbError("CORRUPT_ARIES_WAL","ARIES LSN sequence has a gap or duplicate");
            if(type==Type::BeginCheckpoint||type==Type::EndCheckpoint){if(r.tx||r.prev||r.page||r.undo_next)throw DbError("CORRUPT_ARIES_WAL","Checkpoint record contains transaction fields");}
            else if(type==Type::Begin){if(!r.tx||r.prev||used_transactions.count(r.tx))throw DbError("CORRUPT_ARIES_WAL","Invalid/reused ARIES transaction BEGIN");used_transactions.insert(r.tx);chain[r.tx]={r.lsn,Status::Active};}
            else {auto t=chain.find(r.tx);if(!r.tx||t==chain.end()||r.prev!=t->second.first)throw DbError("CORRUPT_ARIES_WAL","Broken transaction PrevLSN chain");if(type==Type::Commit){if(t->second.second!=Status::Active)throw DbError("CORRUPT_ARIES_WAL","COMMIT after ABORT");t->second.second=Status::Committed;}else if(type==Type::Abort){if(t->second.second!=Status::Active)throw DbError("CORRUPT_ARIES_WAL","Repeated ABORT or ABORT after COMMIT");t->second.second=Status::Aborting;}else if(type==Type::End){if(t->second.second==Status::Active)throw DbError("CORRUPT_ARIES_WAL","END without COMMIT/ABORT");chain.erase(t);}else if(t->second.second==Status::Committed)throw DbError("CORRUPT_ARIES_WAL","Page update after COMMIT");if(type!=Type::End)chain.at(r.tx).first=r.lsn;}
            if(page_record(type)){if(r.page>=(std::uint64_t(1)<<48))throw DbError("CORRUPT_ARIES_WAL","ARIES page ID exceeds format range");Page after{};std::copy_n(bytes.data()+(type==Type::Update?page_size:0),page_size,after.data());if(!codec.valid(after,r.page)||codec.lsn(after)!=r.lsn)throw DbError("CORRUPT_ARIES_WAL","Invalid after/CLR page image");if(type==Type::Update){Page before{};std::copy_n(bytes.data(),page_size,before.data());if(!codec.valid(before,r.page)||codec.lsn(before)>=r.lsn||r.undo_next)throw DbError("CORRUPT_ARIES_WAL","Invalid before image");}else if(r.undo_next&&(r.undo_next>=r.lsn||!index.count(r.undo_next)||index.at(r.undo_next).tx!=r.tx))throw DbError("CORRUPT_ARIES_WAL","Invalid CLR UndoNextLSN");}
            else if(r.page||r.undo_next)throw DbError("CORRUPT_ARIES_WAL","Non-page record contains page/undo fields");
            if(type==Type::EndCheckpoint)(void)decode_checkpoint(r);
            index.emplace(r.lsn,r);previous=r.lsn;max_tx=std::max(max_tx,r.tx);offset+=80+length;
        }
        if(offset<forced_end||previous<flushed_lsn)throw DbError("CORRUPT_ARIES_WAL","Incomplete tail intersects independently durable WAL");
        if((flushed_lsn==0&&forced_end!=64)||(flushed_lsn&&(!index.count(flushed_lsn)||index.at(flushed_lsn).offset+80+index.at(flushed_lsn).length!=forced_end)))throw DbError("CORRUPT_ARIES_CONTROL","Durable-end watermark is not a record boundary");
        if((checkpoint_begin==0)!=(checkpoint_end==0))throw DbError("CORRUPT_ARIES_CONTROL","Incomplete master checkpoint pair");
        if(checkpoint_begin&&( !index.count(checkpoint_begin)||index.at(checkpoint_begin).type!=Type::BeginCheckpoint||!index.count(checkpoint_end)||index.at(checkpoint_end).type!=Type::EndCheckpoint||checkpoint_end<checkpoint_begin))throw DbError("CORRUPT_ARIES_CONTROL","Master checkpoint does not identify complete WAL records");
        wal_end=offset;last_lsn=previous;
        // Before truncating an unforced tail, reject valid pages that claim an
        // absent WAL record. Uncommitted stolen pages remain allowed when their
        // full UPDATE record is present and protected by the durable watermark.
        const auto pages=(data.size()+page_size-1)/page_size;
        for(PageId id=0;id<pages;++id){Page p{};auto available=std::min<std::uint64_t>(page_size,data.size()-id*page_size);data.read(id*page_size,p.data(),static_cast<std::size_t>(available));if(codec.valid(p,id)&&codec.lsn(p)>last_lsn)throw DbError("CORRUPT_ARIES_WAL","Data page LSN is beyond complete WAL");}
        if(tail){wal->truncate(offset);wal->sync();tail_truncated=true;}
    }
    Frame& frame(PageId id,bool create=false){
        auto found=frames.find(id);if(found!=frames.end()){found->second.tick=++tick;return found->second;}
        while(frames.size()>=options.buffer_frames){auto victim=frames.end();for(auto i=frames.begin();i!=frames.end();++i){if(!options.steal&&i->second.dirty&&owners.count(i->first))continue;if(victim==frames.end()||i->second.tick<victim->second.tick)victim=i;}if(victim==frames.end())throw DbError("BUFFER_BUSY","No-steal buffer contains only active dirty pages");if(victim->second.dirty)flush(victim->first);frames.erase(victim);}
        Page p{};if(data.size()>=(id+1)*page_size){data.read(id*page_size,p.data(),p.size());if(!codec.valid(p,id)){const bool zero=std::all_of(p.begin(),p.end(),[](unsigned char b){return b==0;});if(!create||!zero)throw DbError("CORRUPT_PAGE","Invalid ARIES data page: "+std::to_string(id));p=codec.empty(id);}}else{if(!create)throw DbError("PAGE_NOT_FOUND","ARIES page does not exist");p=codec.empty(id);}
        return frames.emplace(id,Frame{p,false,++tick}).first->second;
    }
    void flush(PageId id){auto it=frames.find(id);if(it==frames.end()||!it->second.dirty)return;auto& f=it->second;auto owner=owners.find(id);const bool uncommitted=owner!=owners.end()&&transactions.count(owner->second)&&transactions.at(owner->second).status!=Status::Committed;if(uncommitted&&!options.steal&&!recovering)throw DbError("BUFFER_BUSY","No-steal policy forbids flushing an active transaction page");force(codec.lsn(f.image));crash("before_page_flush");try{inject("before_data_write");data.write(id*page_size,f.image.data(),f.image.size());inject("before_data_sync");data.sync();}catch(...){failed=true;throw;}if(uncommitted)++steals;if(owner!=owners.end()&&transactions.count(owner->second))transactions.at(owner->second).dirty.erase(id);stable_page_lsn[id]=codec.lsn(f.image);f.dirty=false;dpt.erase(id);++flushes;crash("after_page_flush");}
    void mark(PageId id,Page p,Lsn lsn){auto& f=frame(id,true);codec.stamp(p,lsn);f.image=std::move(p);f.dirty=true;f.tick=++tick;if(!dpt.count(id))dpt[id]=lsn;auto owner=owners.find(id);if(owner!=owners.end()&&transactions.count(owner->second))transactions.at(owner->second).dirty.insert(id);}
    void logged_begin(TxId id){auto& t=transactions.at(id);if(!t.last){append(Type::Begin,id);used_transactions.insert(id);}}
    void release_owners(TxId id){for(auto i=owners.begin();i!=owners.end();)if(i->second==id)i=owners.erase(i);else ++i;}
    void undo_to(TxId id,Lsn target,bool ending){
        auto cursor=transactions.at(id).last;
        while(cursor&&cursor!=target){auto r=index.at(cursor);if(r.type==Type::Clr){cursor=r.undo_next;continue;}if(r.type==Type::Update){auto before=image(r,true);std::vector<unsigned char> bytes(before.begin(),before.end());auto lsn=append(Type::Clr,id,r.page,r.prev,std::move(bytes));force(lsn);crash("after_clr_force");mark(r.page,before,lsn);++undo_pages;crash("after_undo_page");cursor=r.prev;}else cursor=r.prev;}
        if(target&&cursor!=target)throw DbError("INVALID_SAVEPOINT","Savepoint is not in the ARIES transaction chain");
        if(ending){append(Type::End,id);release_owners(id);transactions.erase(id);}
    }
    json checkpoint_body(Lsn begin){json txs=json::array(),pages=json::array(),stable=json::array();for(const auto& t:transactions)if(t.second.last)txs.push_back({{"tx",t.first},{"last_lsn",t.second.last},{"status",t.second.status==Status::Active?"ACTIVE":t.second.status==Status::Committed?"COMMITTED":"ABORTING"}});for(const auto& p:dpt)pages.push_back({{"page",p.first},{"rec_lsn",p.second}});for(const auto& p:stable_page_lsn)if(p.second)stable.push_back({{"page",p.first},{"page_lsn",p.second}});return {{"begin_lsn",begin},{"transactions",txs},{"dirty_pages",pages},{"stable_pages",stable}};}
    json decode_checkpoint(const Record& r){
        try{
            auto bytes=payload(r);auto j=json::parse(bytes.begin(),bytes.end());
            auto begin=j.at("begin_lsn").get<Lsn>();
            if(!j.is_object()||!j["transactions"].is_array()||!j["dirty_pages"].is_array()||begin>=r.lsn||!index.count(begin)||index.at(begin).type!=Type::BeginCheckpoint)throw std::runtime_error("shape");
            std::set<PageId> pages;std::set<TxId> ids;
            for(const auto& p:j["dirty_pages"]){auto id=p.at("page").get<PageId>();auto lsn=p.at("rec_lsn").get<Lsn>();if(id>=(std::uint64_t(1)<<48)||!pages.insert(id).second||!lsn||lsn>=r.lsn||!index.count(lsn)||!page_record(index.at(lsn).type)||index.at(lsn).page!=id)throw std::runtime_error("recLSN");}
            for(const auto& t:j["transactions"]){auto id=t.at("tx").get<TxId>();auto last=t.at("last_lsn").get<Lsn>();auto status=t.at("status").get<std::string>();if(!id||!ids.insert(id).second||!last||last>=r.lsn||!index.count(last)||index.at(last).tx!=id)throw std::runtime_error("lastLSN");auto type=index.at(last).type;bool valid=status=="ACTIVE"?(type==Type::Begin||type==Type::Update||type==Type::Clr):status=="ABORTING"?(type==Type::Abort||type==Type::Clr):status=="COMMITTED"&&type==Type::Commit;if(!valid)throw std::runtime_error("status");}
            if(!j.at("stable_pages").is_array())throw std::runtime_error("stable pages");
            std::set<PageId> stable;
            for(const auto& p:j["stable_pages"]){auto id=p.at("page").get<PageId>();auto lsn=p.at("page_lsn").get<Lsn>();if(!stable.insert(id).second||!lsn||lsn>=r.lsn||!index.count(lsn)||!page_record(index.at(lsn).type)||index.at(lsn).page!=id)throw std::runtime_error("stable pageLSN");}
            return j;
        }catch(const DbError&){throw;}catch(...){throw DbError("CORRUPT_ARIES_WAL","Invalid END_CHECKPOINT TT/DPT payload");}
    }
    void merge_checkpoint(const Record& r){
        auto j=decode_checkpoint(r);
        for(const auto& p:j["dirty_pages"]){auto id=p.at("page").get<PageId>();auto lsn=p.at("rec_lsn").get<Lsn>();if(!dpt.count(id)||lsn<dpt[id])dpt[id]=lsn;}
        for(const auto& item:j["transactions"]){auto id=item.at("tx").get<TxId>();auto last=item.at("last_lsn").get<Lsn>();auto status=item.at("status").get<std::string>();auto& t=transactions[id];if(last>=t.last){t.last=last;t.status=status=="ACTIVE"?Status::Active:status=="COMMITTED"?Status::Committed:Status::Aborting;}}
    }
    void repair_torn_pages(){
        const auto count=(data.size()+page_size-1)/page_size;
        for(PageId id=0;id<count;++id){Page p{};const auto available=std::min<std::uint64_t>(page_size,data.size()-id*page_size);data.read(id*page_size,p.data(),static_cast<std::size_t>(available));if(codec.valid(p,id)){auto lsn=codec.lsn(p);if(lsn&&(!index.count(lsn)||!page_record(index.at(lsn).type)||index.at(lsn).page!=id))throw DbError("CORRUPT_PAGE","Data PageLSN does not name its own UPDATE/CLR");continue;}const bool zero=std::all_of(p.begin(),p.end(),[](unsigned char b){return b==0;});auto record=index.rbegin();for(;record!=index.rend();++record)if(page_record(record->second.type)&&record->second.page==id)break;if(record==index.rend()){if(zero)continue;throw DbError("CORRUPT_PAGE","Torn page has no full WAL image");}auto restored=image(record->second);force(record->first);inject("before_data_write");data.write(id*page_size,restored.data(),restored.size());inject("before_data_sync");data.sync();++torn_repairs;}
    }
    void recover(){
        recovering=true;try{
        if(checkpoint_end){auto certified=decode_checkpoint(index.at(checkpoint_end));for(const auto& item:certified["stable_pages"]){auto id=item.at("page").get<PageId>();Page p{};if(data.size()>=(id+1)*page_size){data.read(id*page_size,p.data(),p.size());if(codec.valid(p,id)&&codec.lsn(p)<item.at("page_lsn").get<Lsn>())throw DbError("MEDIA_RECOVERY_REQUIRED","Valid data page predates the checkpoint stable-page certificate");}}}
        repair_torn_pages();
        for(PageId id=0;id<data.size()/page_size;++id){Page p{};data.read(id*page_size,p.data(),p.size());if(codec.valid(p,id)&&codec.lsn(p))stable_page_lsn[id]=codec.lsn(p);}
        Lsn analysis_start=checkpoint_begin?checkpoint_begin:1;std::uint64_t analyzed=0;
        for(auto i=index.lower_bound(analysis_start);i!=index.end();++i){auto r=i->second;++analyzed;if(r.type==Type::EndCheckpoint){merge_checkpoint(r);continue;}if(r.type==Type::BeginCheckpoint)continue;if(r.type==Type::End){transactions.erase(r.tx);continue;}auto& t=transactions[r.tx];t.last=std::max(t.last,r.lsn);if(r.type==Type::Commit)t.status=Status::Committed;else if(r.type==Type::Abort)t.status=Status::Aborting;if(page_record(r.type)&&(!dpt.count(r.page)||r.lsn<dpt[r.page]))dpt[r.page]=r.lsn;}
        json tt_before=json::array(),dpt_before=json::array();for(const auto& t:transactions)tt_before.push_back({{"tx",t.first},{"last_lsn",t.second.last},{"status",t.second.status==Status::Committed?"COMMITTED":t.second.status==Status::Aborting?"ABORTING":"ACTIVE"}});for(const auto& p:dpt)dpt_before.push_back({{"page",p.first},{"rec_lsn",p.second}});
        Lsn redo_start=last_lsn+1;for(const auto& p:dpt)redo_start=std::min(redo_start,p.second);force(last_lsn);
        // Repeat history includes UPDATEs of losers and all CLRs, not winners only.
        const auto redo_end=last_lsn;const auto redo_dpt=dpt;
        for(auto i=index.lower_bound(redo_start);i!=index.end()&&i->first<=redo_end;++i){auto r=i->second;auto expected=redo_dpt.find(r.page);if(!page_record(r.type)||expected==redo_dpt.end()||r.lsn<expected->second)continue;auto& f=frame(r.page,true);if(codec.lsn(f.image)<r.lsn){mark(r.page,image(r),r.lsn);++redo_pages;}}
        std::vector<TxId> winners;std::priority_queue<std::pair<Lsn,TxId>> losers;
        for(const auto& t:transactions){if(t.second.status==Status::Committed)winners.push_back(t.first);else losers.push({t.second.last,t.first});}
        for(auto tx:winners){append(Type::End,tx);transactions.erase(tx);}
        while(!losers.empty()){auto pair=losers.top();losers.pop();auto id=pair.second;if(!transactions.count(id))continue;if(transactions.at(id).status==Status::Active){append(Type::Abort,id);transactions.at(id).status=Status::Aborting;}undo_to(id,0,true);}
        for(auto& f:frames)if(f.second.dirty)flush(f.first);
        force(last_lsn);owners.clear();transactions.clear();dpt.clear();recovering=false;
        recovery={{"status","PASS"},{"analysis_start_lsn",analysis_start},{"analysis_records",analyzed},{"analysis_transaction_table",tt_before},{"analysis_dirty_page_table",dpt_before},{"redo_start_lsn",redo_start},{"redo_end_lsn",redo_end},{"redo_pages",redo_pages},{"undo_pages",undo_pages},{"clr_records",clr_records},{"torn_pages_repaired",torn_repairs},{"scope","analysis / repeat-history redo (including losers) / undo with redo-only CLR and UndoNextLSN"}};
        }catch(...){failed=true;recovering=false;throw;}
    }
};

Manager::Manager(const std::string& directory,DataDevice& data,const std::string& uuid,PageCodec codec,Options options):impl_(std::make_unique<Impl>(directory,data,uuid,std::move(codec),std::move(options))){}
Manager::~Manager()=default;
void Manager::begin(TxId tx,bool read_only){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;if(s.failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");if(!tx||tx==std::numeric_limits<TxId>::max()||s.transactions.count(tx)||s.used_transactions.count(tx))throw DbError("TRANSACTION_ACTIVE","ARIES transaction ID must be new and nonzero");s.transactions[tx]=Impl::Tx{Impl::Status::Active,0,read_only,{}};s.max_tx=std::max(s.max_tx,tx);}
void Manager::set_external_reservation(std::uint64_t bytes){
    std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;
    if(s.failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");
    for(const auto& item:s.transactions)if(item.second.last&&item.second.status==Impl::Status::Active)throw DbError("ARIES_BUSY","Cannot change reservation during a physical operation");
    if(bytes>s.options.wal_limit_bytes-s.wal_end)throw DbError("UNDO_RESERVATION","Insufficient WAL for outstanding logical compensation");
    s.external_reserved=bytes;
}
void Manager::begin_compensation(TxId tx,std::uint64_t credit,std::uint64_t updates){
    std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;
    if(s.failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");
    if(!tx||tx==std::numeric_limits<TxId>::max()||s.transactions.count(tx)||s.used_transactions.count(tx))throw DbError("TRANSACTION_ACTIVE","Compensation mini-transaction ID must be new");
    if(credit>s.external_reserved||credit<640||updates<record_header_size+2*page_size||updates>credit-320)throw DbError("COMPENSATION_CREDIT","Invalid or unreserved compensation allowance");
    for(const auto& item:s.transactions)if(item.second.last&&item.second.status==Impl::Status::Active)throw DbError("ARIES_BUSY","Physical operations must be structurally serialized");
    auto& t=s.transactions[tx];t.compensation=true;t.credit_left=credit;t.update_left=updates;s.max_tx=std::max(s.max_tx,tx);
}
Manager::LogSpace Manager::log_space()const{std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;return {s.wal_end,s.options.wal_limit_bytes,s.external_reserved};}
std::uint64_t Manager::transaction_record_bytes(TxId tx)const{std::lock_guard<std::mutex> lock(impl_->mutex);std::uint64_t total=0;for(const auto& item:impl_->index)if(item.second.tx==tx)total+=record_header_size+item.second.length;return total;}
bool Manager::transaction_committed(TxId tx)const{std::lock_guard<std::mutex> lock(impl_->mutex);for(const auto& item:impl_->index)if(item.second.tx==tx&&item.second.type==Type::Commit&&item.first<=impl_->flushed_lsn)return true;return false;}
void Manager::recovery_barrier(){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;if(s.failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");if(!s.transactions.empty())throw DbError("ARIES_BUSY","Recovery barrier requires no active physical transaction");for(auto& p:s.frames)if(p.second.dirty)s.flush(p.first);s.force(s.last_lsn);}
Page Manager::read(TxId tx,PageId page){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;if(tx)s.require(tx);if(s.failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");auto owner=s.owners.find(page);if(owner!=s.owners.end()&&owner->second!=tx)throw DbError("ARIES_LOCK_REQUIRED","Dirty read rejected; caller must hold strict transaction locks");return s.frame(page).image;}
void Manager::update(TxId tx,PageId page,const Page& image){
    std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require(tx);auto& t=s.transactions.at(tx);
    if(t.read_only)throw DbError("READ_ONLY_TRANSACTION","ARIES read-only transaction cannot update");
    if(t.status!=Impl::Status::Active)throw DbError("TRANSACTION_ABORTED","ARIES transaction is not active");
    if(!s.codec.valid(image,page))throw DbError("INVALID_PAGE","ARIES update must supply a valid identity/CRC image");
    auto owner=s.owners.find(page);if(owner!=s.owners.end()&&owner->second!=tx)throw DbError("ARIES_PAGE_CONFLICT","Another transaction owns this full page");
    const auto needed=record_header_size+2*page_size+(t.last?0:record_header_size);
    const auto online=t.compensation?s.options.wal_limit_bytes:(s.options.wal_limit_bytes-s.external_reserved)/2;
    if(s.wal_end>online||needed>online-s.wal_end)throw DbError("ARIES_LOG_LIMIT","Reserve complete BEGIN+UPDATE before either is logged");
    if(t.compensation&&(needed>t.update_left||needed>t.credit_left))throw DbError("COMPENSATION_CREDIT","Compensation update exceeds its proven page/byte bound");
    auto before=s.frame(page,true).image;s.logged_begin(tx);std::vector<unsigned char> bytes(before.begin(),before.end());bytes.insert(bytes.end(),image.begin(),image.end());
    auto lsn=s.append(Type::Update,tx,page,0,std::move(bytes));if(t.compensation)t.update_left-=needed;s.owners[page]=tx;s.mark(page,image,lsn);
}
json Manager::commit(TxId tx){
    std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require(tx);auto& t=s.transactions.at(tx);
    if(t.status!=Impl::Status::Active)throw DbError("TRANSACTION_ABORTED","ARIES transaction cannot commit");
    if(!t.last){s.transactions.erase(tx);return {{"status","COMMITTED_READ_ONLY"},{"commit_lsn",0},{"wal_bytes",0},{"dirty_pages",0}};}
    bool commit_record_appended=false;
    try{
        auto lsn=s.append(Type::Commit,tx);commit_record_appended=true;t.status=Impl::Status::Committed;
        s.force(lsn);s.inject("after_commit_force");crash("after_commit_force");
        if(s.options.force_at_commit)for(const auto& owner:s.owners)if(owner.second==tx)s.flush(owner.first);
        s.append(Type::End,tx);auto bytes=t.wal_bytes;auto dirty=t.dirty.size();
        // Allocate the response before erasing the transaction. Any failure
        // after a possible COMMIT is outcome-unknown, never an ABORT candidate.
        json result={{"status","COMMITTED"},{"commit_lsn",lsn},{"flushed_lsn",s.flushed_lsn},{"wal_bytes",bytes},{"dirty_pages",dirty},{"force_at_commit",s.options.force_at_commit}};
        s.release_owners(tx);s.transactions.erase(tx);return result;
    }catch(...){if(commit_record_appended)s.failed=true;throw;}
}
void Manager::abort(TxId tx){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require(tx);auto& t=s.transactions.at(tx);if(!t.last){s.transactions.erase(tx);return;}if(t.status==Impl::Status::Committed)throw DbError("COMMIT_OUTCOME_UNKNOWN","Durable commit cannot be rolled back; restart/recover if I/O failed");if(t.status==Impl::Status::Active){s.append(Type::Abort,tx);t.status=Impl::Status::Aborting;}s.undo_to(tx,0,true);}
void Manager::savepoint(TxId tx,const std::string& name){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require(tx);if(name.empty())throw DbError("INVALID_SAVEPOINT","Empty ARIES savepoint name");auto& t=s.transactions.at(tx);if(t.status!=Impl::Status::Active)throw DbError("TRANSACTION_ABORTED","ARIES transaction is not active");auto i=std::find_if(t.savepoints.begin(),t.savepoints.end(),[&](const auto& p){return p.first==name;});if(i!=t.savepoints.end())t.savepoints.erase(i,t.savepoints.end());if(t.savepoints.size()>=64)throw DbError("SAVEPOINT_LIMIT","ARIES savepoint limit is64");t.savepoints.push_back({name,t.last});}
void Manager::rollback_to(TxId tx,const std::string& name){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require(tx);auto& t=s.transactions.at(tx);if(t.status!=Impl::Status::Active)throw DbError("TRANSACTION_ABORTED","ARIES transaction is not active");auto i=std::find_if(t.savepoints.begin(),t.savepoints.end(),[&](const auto& p){return p.first==name;});if(i==t.savepoints.end())throw DbError("SAVEPOINT_NOT_FOUND","Unknown ARIES savepoint");auto number=static_cast<std::size_t>(i-t.savepoints.begin());auto target=i->second;s.undo_to(tx,target,false);t.savepoints.resize(number+1);}
void Manager::release_savepoint(TxId tx,const std::string& name){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require(tx);auto& points=s.transactions.at(tx).savepoints;auto i=std::find_if(points.begin(),points.end(),[&](const auto& p){return p.first==name;});if(i==points.end())throw DbError("SAVEPOINT_NOT_FOUND","Unknown ARIES savepoint");points.erase(i,points.end());}
json Manager::checkpoint_fuzzy(){auto& s=*impl_;Lsn begin=0;{std::lock_guard<std::mutex> lock(s.mutex);if(s.failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");if(s.checkpoint_running)throw DbError("CHECKPOINT_BUSY","ARIES checkpoint already running");begin=s.append(Type::BeginCheckpoint);s.checkpoint_running=true;}
    try{crash("checkpoint_after_begin");if(s.options.checkpoint_interleave)s.options.checkpoint_interleave();std::lock_guard<std::mutex> lock(s.mutex);auto body=s.checkpoint_body(begin);auto text=body.dump();auto end=s.append(Type::EndCheckpoint,0,0,0,std::vector<unsigned char>(text.begin(),text.end()));s.force(end);crash("checkpoint_after_end_force");atomic_control(s.directory/"control-aries-master.json",s.master(begin,end));s.checkpoint_begin=begin;s.checkpoint_end=end;s.checkpoint_running=false;crash("checkpoint_after_master");return {{"status","PASS"},{"mode","ARIES_FUZZY"},{"begin_lsn",begin},{"end_lsn",end},{"active_transactions",s.transactions.size()},{"dirty_pages",s.dpt.size()},{"data_pages_forced",false},{"snapshot",body}};}catch(...){std::lock_guard<std::mutex> lock(s.mutex);s.checkpoint_running=false;if(s.failed)throw;throw;}}
void Manager::flush_page(PageId page){std::lock_guard<std::mutex> lock(impl_->mutex);if(impl_->failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");impl_->flush(page);}
void Manager::flush_all(){std::lock_guard<std::mutex> lock(impl_->mutex);if(impl_->failed)throw DbError("RECOVERY_REQUIRED","ARIES requires recovery");for(auto& p:impl_->frames)if(p.second.dirty)impl_->flush(p.first);}
std::uint64_t Manager::transaction_wal_bytes(TxId tx)const{std::lock_guard<std::mutex> lock(impl_->mutex);auto found=impl_->transactions.find(tx);if(found==impl_->transactions.end())throw DbError("NO_TRANSACTION","Unknown ARIES transaction");return found->second.wal_bytes;}
json Manager::transaction_metrics(TxId tx)const{std::lock_guard<std::mutex> lock(impl_->mutex);auto found=impl_->transactions.find(tx);if(found==impl_->transactions.end())throw DbError("NO_TRANSACTION","Unknown ARIES transaction");return {{"wal_bytes",found->second.wal_bytes},{"dirty_pages",found->second.dirty.size()},{"last_lsn",found->second.last}};}
json Manager::diagnostics()const{std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;json txs=json::array(),dirty=json::array(),buffer=json::array(),owners=json::array();for(const auto& t:s.transactions)txs.push_back({{"tx",t.first},{"last_lsn",t.second.last},{"status",t.second.status==Impl::Status::Active?"ACTIVE":t.second.status==Impl::Status::Committed?"COMMITTED":"ABORTING"}});for(const auto& p:s.dpt)dirty.push_back({{"page",p.first},{"rec_lsn",p.second}});for(const auto& p:s.frames)buffer.push_back({{"page",p.first},{"page_lsn",s.codec.lsn(p.second.image)},{"dirty",p.second.dirty}});for(const auto& p:s.owners)owners.push_back({{"page",p.first},{"tx",p.second}});return {{"mode","ARIES_PAGE"},{"page_size",page_size},{"buffer_frames",s.options.buffer_frames},{"steal",s.options.steal},{"force_at_commit",s.options.force_at_commit},{"transaction_table",txs},{"dirty_page_table",dirty},{"buffer",buffer},{"page_owners",owners},{"last_lsn",s.last_lsn},{"flushed_lsn",s.flushed_lsn},{"durable_end",s.forced_end},{"wal_bytes",s.wal_end},{"checkpoint_begin",s.checkpoint_begin},{"checkpoint_end",s.checkpoint_end},{"max_tx_id",s.max_tx},{"redo_pages",s.redo_pages},{"undo_pages",s.undo_pages},{"clr_records",s.clr_records},{"page_flushes",s.flushes},{"steal_flushes",s.steals},{"wal_forces",s.forces},{"wal_tail_truncated",s.tail_truncated},{"recovery_required",s.failed},{"recovery",s.recovery},{"io",io_snapshot(s.directory)},{"limits","full-page undo requires strict transaction/page ownership; no early-SMO-release/logical-undo, no log truncation, no hardware power-loss claim"}};}
json Manager::wal_records(std::size_t limit)const{std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;if(limit>65536)throw DbError("ARGUMENT","ARIES inspection limit exceeds65536");json records=json::array();for(const auto& pair:s.index){if(records.size()>=limit)break;auto r=pair.second;json row={{"lsn",r.lsn},{"type",type_name(r.type)},{"tx",r.tx},{"prev_lsn",r.prev},{"page",r.page},{"undo_next_lsn",r.undo_next},{"payload_bytes",r.length},{"offset",r.offset}};if(r.type==Type::EndCheckpoint){auto bytes=s.payload(r);row["checkpoint"]=json::parse(bytes.begin(),bytes.end());}records.push_back(row);}return {{"records",records},{"total_records",s.index.size()},{"truncated",records.size()<s.index.size()}};}
} } // namespace edudb::aries
