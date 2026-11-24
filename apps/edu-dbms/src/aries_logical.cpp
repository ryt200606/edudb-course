#include "aries_logical.hpp"
#include "edudb.hpp"
#include "managed_block_device.hpp"
#include "io.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <thread>
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
namespace edudb::logical {
namespace {
namespace fs=std::filesystem;using Clock=std::chrono::steady_clock;
constexpr std::uint64_t log_limit=64ull*1024*1024;
constexpr std::size_t header_size=80;
void put32(unsigned char* p,std::uint32_t n){for(unsigned i=0;i<4;++i)p[i]=static_cast<unsigned char>(n>>(8*i));}
void put64(unsigned char* p,std::uint64_t n){for(unsigned i=0;i<8;++i)p[i]=static_cast<unsigned char>(n>>(8*i));}
std::uint32_t get32(const unsigned char* p){std::uint32_t n=0;for(int i=3;i>=0;--i)n=(n<<8)|p[i];return n;}
std::uint64_t get64(const unsigned char* p){std::uint64_t n=0;for(int i=7;i>=0;--i)n=(n<<8)|p[i];return n;}
std::uint32_t crc(const unsigned char* p,std::size_t n){auto c=0xffffffffu;for(std::size_t i=0;i<n;++i){c^=p[i];for(unsigned b=0;b<8;++b)c=(c&1)?0xedb88320u^(c>>1):c>>1;}return c^0xffffffffu;}
#include "logical_file.inc"
enum class Type:std::uint32_t {Begin=1,Operation=2,OperationEnd=3,Commit=4,Abort=5,UndoBegin=6,Clr=7,End=8};
void crash(const char* phase){
    const char* pause=std::getenv("EDUDB_LOGICAL_PAUSE_AT");
    if(pause&&std::strcmp(pause,phase)==0){const char* ready=std::getenv("EDUDB_LOGICAL_PHASE_READY");if(!ready)throw DbError("ARGUMENT","Test pause requires an owned ready-file path");std::ofstream out(fs::u8path(ready),std::ios::binary);out<<json{{"phase",phase},{"status","PAUSED_FOR_OWNED_EXTERNAL_KILL"}}.dump();out.close();if(!out)throw DbError("IO_ERROR","Cannot publish test-only logical phase marker");for(unsigned i=0;i<2000;++i)std::this_thread::sleep_for(std::chrono::milliseconds(10));std::_Exit(87);}
    const char* value=std::getenv("EDUDB_LOGICAL_CRASH_AT");if(value&&std::strcmp(value,phase)==0)std::_Exit(86);
}
[[noreturn]]void bad(const std::string& message){throw DbError("CORRUPT_LOGICAL_WAL",message);}
json logical_read_control(const fs::path& path){
    NativeFile file(path,false,false,false,false,true);auto n=file.size();if(n<2||n>65536)bad("Logical control size bound");std::string text(static_cast<std::size_t>(n),'\0');file.read(0,reinterpret_cast<unsigned char*>(text.data()),text.size());json value;
    try{value=json::parse(text,[](int depth,json::parse_event_t,json&){if(depth>8)bad("Logical control nesting bound");return true;});}catch(const DbError&){throw;}catch(...){bad("Invalid logical control JSON");}
    if(!value.is_object()||!value.contains("crc")||!value["crc"].is_number_unsigned()||value["crc"].get<std::uint64_t>()>std::numeric_limits<std::uint32_t>::max())bad("Logical control CRC field");
    auto expected=value["crc"].get<std::uint32_t>();value.erase("crc");auto plain=value.dump();if(crc(reinterpret_cast<const unsigned char*>(plain.data()),plain.size())!=expected)bad("Logical control CRC mismatch");return value;
}
std::uint64_t number(const json& j,const char* key){if(!j.contains(key)||!j[key].is_number_integer()||(!j[key].is_number_unsigned()&&j[key].get<std::int64_t>()<0))bad(std::string("Invalid integer: ")+key);return j[key].get<std::uint64_t>();}
}
struct Journal::Impl {
    struct Tx {std::uint64_t last=0;bool begun=false,aborting=false,committed=false;std::vector<std::uint64_t> operations;std::vector<std::pair<std::string,std::size_t>> savepoints;};
    fs::path directory;std::string uuid;std::unique_ptr<NativeFile> lock,wal;mutable std::mutex mutex;
    std::map<std::uint64_t,Tx> transactions;std::map<std::uint64_t,Operation> operations;
    std::set<std::uint64_t> persistent_ids;
    std::uint64_t lsn=0,end=64,forced_lsn=0,forced_end=64,maximum_id=0,allocation_end=2,remaining_credit=0;
    bool failstop=false,tail_truncated=false;
    Impl(const std::string& path,std::string identity):directory(fs::absolute(fs::u8path(path)).lexically_normal()),uuid(std::move(identity)){
        if(uuid.size()!=32||uuid.find_first_not_of("0123456789abcdef")!=std::string::npos)throw DbError("ARGUMENT","Logical UUID must be32 lowercase hex digits");
        fs::create_directories(directory);lock=std::make_unique<NativeFile>(directory/"control-logical.lock",true,false,true);auto filename=directory/"wal-logical.log";const bool fresh=!fs::exists(filename);
        if(fresh&&fs::exists(directory/"control-logical-durable.json"))bad("Logical control without WAL");
        wal=std::make_unique<NativeFile>(filename,true);
        if(fresh){std::array<unsigned char,64> h{};std::memcpy(h.data(),"EDLGW01",7);std::memcpy(h.data()+8,uuid.data(),32);put32(h.data()+40,1);put32(h.data()+44,aries::page_size);put32(h.data()+60,crc(h.data(),h.size()));wal->write(0,h.data(),h.size());wal->sync();atomic_control(directory/"control-logical-durable.json",control(0,64));}
        auto c=logical_read_control(directory/"control-logical-durable.json");if(c.value("format",std::string())!="EduDB-ARIES-LOGICAL"||c.value("version",0)!=1||c.value("uuid",std::string())!=uuid||c.value("limits_version",0)!=1)bad("Logical control identity/limits mismatch");forced_lsn=number(c,"forced_lsn");forced_end=number(c,"forced_end");scan();
    }
    json control(std::uint64_t through,std::uint64_t bytes)const{return {{"format","EduDB-ARIES-LOGICAL"},{"version",1},{"limits_version",1},{"uuid",uuid},{"forced_lsn",through},{"forced_end",bytes}};}
    void require()const{if(failstop)throw DbError("RECOVERY_REQUIRED","Logical journal requires recovery");}
    Tx& tx(std::uint64_t id){require();auto it=transactions.find(id);if(it==transactions.end())throw DbError("NO_TRANSACTION","Logical transaction not active");return it->second;}
    Operation& op(std::uint64_t id){auto it=operations.find(id);if(it==operations.end())throw DbError("NO_OPERATION","Logical operation not active");return it->second;}
    void apply(Type type,std::uint64_t record_lsn,std::uint64_t id,std::uint64_t prev,std::uint64_t operation,std::uint64_t undo_next,const json& body){
        maximum_id=std::max(maximum_id,id);
        if(type==Type::Begin){if(!id||id==std::numeric_limits<std::uint64_t>::max()||prev||operation||undo_next||transactions.count(id)||transactions.size()>=Limits::active_transactions||!persistent_ids.insert(id).second)bad("Invalid/repeated/unbounded logical BEGIN");transactions[id].begun=true;transactions[id].last=record_lsn;return;}
        auto it=transactions.find(id);if(it==transactions.end()||it->second.last!=prev)bad("Broken logical PrevLSN chain");auto& t=it->second;
        if(t.committed&&type!=Type::End)bad("Logical operation after COMMIT");
        if(type!=Type::Clr&&undo_next)bad("UndoNext on non-CLR");
        if(type==Type::Operation){
            if(t.aborting||operation!=record_lsn||operations.size()>=Limits::operation_history)bad("Invalid operation identity/state/history bound");
            Operation o;o.id=record_lsn;o.tx=id;o.previous=t.operations.empty()?0:t.operations.back();o.mini_tx=number(body,"mini_tx");maximum_id=std::max(maximum_id,o.mini_tx);o.description=body.at("description");
            if(!o.mini_tx||o.mini_tx==std::numeric_limits<std::uint64_t>::max()||!persistent_ids.insert(o.mini_tx).second||body.at("previous_operation")!=o.previous||number(body,"reserved_bytes")!=Limits::operation_credit||number(body,"reserved_pages")!=Limits::operation_pages)bad("Operation chain/reservation mismatch");
            if(!o.description.is_object()||!o.description.at("table").is_string()||o.description.at("table").get_ref<const std::string&>().empty()||o.description.at("table").get_ref<const std::string&>().size()>4096||!o.description.at("key").is_number_integer()||!number(o.description,"lid")||(!o.description.at("before").is_null()&&!o.description.at("before").is_array())||(!o.description.at("after").is_null()&&!o.description.at("after").is_array())||o.description.at("before").dump().size()>Limits::row_bytes||o.description.at("after").dump().size()>Limits::row_bytes||(o.description.at("before").is_null()&&o.description.at("after").is_null()))bad("Logical stable-key/row descriptor is malformed");
            auto start=number(body,"allocation_start"),finish=number(body,"allocation_end");if(start>finish||finish>Limits::file_pages||finish-start>Limits::new_pages_per_attempt)bad("Forward allocation bound");allocation_end=std::max(allocation_end,finish);
            operations.emplace(o.id,std::move(o));t.operations.push_back(record_lsn);
        }else if(type==Type::OperationEnd){auto& o=op(operation);if(o.tx!=id||o.complete||o.compensated||body.at("mini_tx")!=o.mini_tx)bad("Invalid OP_END");o.complete=true;
        }else if(type==Type::Abort){if(t.aborting)bad("Repeated ABORT");t.aborting=true;
        }else if(type==Type::UndoBegin){
            auto& o=op(operation);if(o.tx!=id||o.compensated||o.attempts.size()>=Limits::recovery_attempts||number(body,"attempt")!=o.attempts.size()+1)bad("Invalid compensation attempt");
            Operation::Attempt a{number(body,"mini_tx"),number(body,"allocation_start"),number(body,"allocation_end")};if(!a.tx||!persistent_ids.insert(a.tx).second||a.allocation_start>a.allocation_end||a.allocation_end>Limits::file_pages||a.allocation_end-a.allocation_start>Limits::new_pages_per_attempt)bad("Undo allocation bound");maximum_id=std::max(maximum_id,a.tx);allocation_end=std::max(allocation_end,a.allocation_end);o.attempts.push_back(a);
        }else if(type==Type::Clr){auto& o=op(operation);if(o.tx!=id||o.compensated||undo_next!=o.previous||o.attempts.empty())bad("Invalid logical CLR UndoNext");o.compensated=true;o.undo_next=undo_next;
        }else if(type==Type::Commit){if(t.aborting)bad("COMMIT after ABORT");for(auto oid:t.operations){auto& o=op(oid);if(!o.complete&&!o.compensated)bad("COMMIT with unfinished operation");}t.committed=true;
        }else if(type==Type::End){if(!t.committed){if(!t.aborting)bad("END lacks terminal decision");for(auto oid:t.operations)if(!op(oid).compensated)bad("ABORT END before all logical CLR");}for(auto oid:t.operations)operations.erase(oid);transactions.erase(id);return;
        }else bad("Unknown logical record type");t.last=record_lsn;
    }
    void scan(){
        const auto size=wal->size();if(size<64||size>log_limit||forced_end<64||forced_end>size)bad("Logical WAL/control length");std::array<unsigned char,64> file{};wal->read(0,file.data(),file.size());auto sum=get32(file.data()+60);put32(file.data()+60,0);if(std::memcmp(file.data(),"EDLGW01",7)||std::string(reinterpret_cast<char*>(file.data()+8),32)!=uuid||get32(file.data()+40)!=1||get32(file.data()+44)!=aries::page_size||sum!=crc(file.data(),file.size()))bad("Logical header identity/CRC");
        std::uint64_t at=64,previous=0;bool durable_boundary=forced_lsn==0&&forced_end==64;
        while(at<size){if(size-at<header_size)break;std::array<unsigned char,header_size> h{};wal->read(at,h.data(),h.size());auto hc=get32(h.data()+12);put32(h.data()+12,0);auto raw=get32(h.data()+4),length=get32(h.data()+8);if(get32(h.data())!=0x4c475731||get32(h.data()+60)!=1||hc!=crc(h.data(),h.size())||raw<1||raw>8||length>16384)bad("Logical record fixed header/CRC");auto type=static_cast<Type>(raw);const bool payload=type==Type::Operation||type==Type::OperationEnd||type==Type::UndoBegin;if((payload&&length<2)||(!payload&&length))bad("Logical record fixed payload length");if(size-at-header_size<length)break;std::vector<unsigned char> b(length);if(length)wal->read(at+header_size,b.data(),b.size());if(get32(h.data()+56)!=crc(b.data(),b.size()))bad("Logical payload CRC");auto current=get64(h.data()+16);if(current!=previous+1)bad("Logical LSN gap");json body=json::object();if(length){try{body=json::parse(b.begin(),b.end(),[](int depth,json::parse_event_t,json&){if(depth>16)bad("Logical payload nesting bound");return true;});}catch(...){bad("Logical JSON payload");}if(!body.is_object())bad("Logical payload must be object");}apply(type,current,get64(h.data()+24),get64(h.data()+32),get64(h.data()+40),get64(h.data()+48),body);at+=header_size+length;previous=current;if(current==forced_lsn&&at==forced_end)durable_boundary=true;}
        if(at<forced_end||previous<forced_lsn||!durable_boundary)bad("Truncated record intersects durable boundary");
        if(at<size){wal->truncate(at);wal->sync();tail_truncated=true;}lsn=previous;end=at;
        // A complete unacknowledged COMMIT may survive. It is never inverted.
        for(auto it=transactions.begin();it!=transactions.end();)if(!it->second.begun)it=transactions.erase(it);else ++it;
        std::size_t active=0;for(const auto& item:operations)if(!item.second.compensated&&!transactions.at(item.second.tx).committed)++active;if(active>Limits::active_operations)bad("Persisted active operation count exceeds bound");
    }
    std::uint64_t append(Type type,std::uint64_t id,std::uint64_t operation=0,std::uint64_t undo_next=0,json body=json::object()){
        require();const bool has=type==Type::Operation||type==Type::OperationEnd||type==Type::UndoBegin;const auto text=has?body.dump():std::string();if(text.size()>16384)throw DbError("LOGICAL_ROW_LIMIT","Logical payload exceeds record bound");
        if(end>log_limit||header_size+text.size()>log_limit-end)throw DbError("LOGICAL_LOG_LIMIT","Logical journal hard limit exhausted");
        if(type==Type::Operation&&end+header_size+text.size()>log_limit/2)throw DbError("LOGICAL_LOG_LIMIT","Logical online allowance exhausted; terminal/undo remains reserved");
        const auto next=lsn+1;const auto previous=type==Type::Begin?0:transactions.at(id).last;if(type==Type::Operation)operation=next;
        std::array<unsigned char,header_size> h{};put32(h.data(),0x4c475731);put32(h.data()+4,static_cast<std::uint32_t>(type));put32(h.data()+8,static_cast<std::uint32_t>(text.size()));put64(h.data()+16,next);put64(h.data()+24,id);put64(h.data()+32,previous);put64(h.data()+40,operation);put64(h.data()+48,undo_next);put32(h.data()+56,crc(reinterpret_cast<const unsigned char*>(text.data()),text.size()));put32(h.data()+60,1);put32(h.data()+12,crc(h.data(),h.size()));
        // Validate/apply the checked state transition before native append. Any
        // allocation or invariant failure stops this instance; restart reads
        // the unchanged valid journal. Invalid metadata is never persisted.
        try{apply(type,next,id,previous,operation,undo_next,body);wal->write(end,h.data(),h.size());if(!text.empty())wal->write(end+header_size,reinterpret_cast<const unsigned char*>(text.data()),text.size());end+=header_size+text.size();lsn=next;wal->sync();atomic_control(directory/"control-logical-durable.json",control(lsn,end));forced_lsn=lsn;forced_end=end;return next;}catch(...){failstop=true;throw;}
    }
    void ensure_begin(std::uint64_t id){auto& t=tx(id);if(!t.begun){auto points=std::move(t.savepoints);transactions.erase(id);append(Type::Begin,id);transactions.at(id).savepoints=std::move(points);}}
};
Journal::Journal(const std::string& d,const std::string& uuid)try:impl_(std::make_unique<Impl>(d,uuid)){}catch(const json::exception& e){throw DbError("CORRUPT_LOGICAL_WAL",std::string("Malformed logical metadata: ")+e.what());}
Journal::~Journal()=default;
void Journal::begin(std::uint64_t id){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require();if(!id||id==std::numeric_limits<std::uint64_t>::max()||s.persistent_ids.count(id)||s.transactions.count(id))throw DbError("TRANSACTION_ACTIVE","Logical user ID must be new and nonzero");if(s.transactions.size()>=Limits::active_transactions)throw DbError("LOGICAL_TX_LIMIT","Logical active transaction count reached64");s.maximum_id=std::max(s.maximum_id,id);s.transactions[id]={};}
void Journal::admit(std::uint64_t id,std::uint64_t pages,aries::Manager& physical){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto& t=s.tx(id);if(s.end+2*header_size+16384>log_limit/2)throw DbError("LOGICAL_LOG_LIMIT","Logical online space cannot fit a complete checked operation");if(t.aborting||t.committed)throw DbError("TRANSACTION_ABORTED","Logical transaction is not writable");std::size_t count=0;for(const auto& item:s.operations)if(!item.second.compensated&&!s.transactions.at(item.second.tx).committed)++count;if(count>=Limits::active_operations)throw DbError("LOGICAL_OPERATION_LIMIT","At most3 uncommitted logical operations are permitted");if(s.operations.size()>=Limits::operation_history)throw DbError("LOGICAL_HISTORY_LIMIT","Commit/rollback to release the bounded256-operation history");if(pages>Limits::file_pages)throw DbError("LOGICAL_PAGE_LIMIT","One physical workset exceeds1024 pages");const auto space=physical.log_space();const auto forward=640+pages*(160+3*aries::page_size);if(space.used>space.limit||s.remaining_credit>space.limit-space.used||Limits::operation_credit>space.limit-space.used-s.remaining_credit||forward>space.limit-space.used-s.remaining_credit-Limits::operation_credit)throw DbError("UNDO_RESERVATION","Forward update plus logical inverse/interrupted CLR cannot fit");physical.set_external_reservation(s.remaining_credit+Limits::operation_credit);}
std::uint64_t Journal::start_operation(std::uint64_t id,std::uint64_t mini,json description,std::uint64_t start,std::uint64_t finish){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.ensure_begin(id);auto& t=s.tx(id);if(description.dump().size()>8192||!mini||finish<start||finish-start>Limits::new_pages_per_attempt||finish>Limits::file_pages)throw DbError("LOGICAL_ROW_LIMIT","Forward operation exceeds checked metadata/allocation bounds");auto previous=t.operations.empty()?0:t.operations.back();auto oid=s.append(Type::Operation,id,0,0,{{"mini_tx",mini},{"description",std::move(description)},{"previous_operation",previous},{"reserved_bytes",Limits::operation_credit},{"reserved_pages",Limits::operation_pages},{"allocation_start",start},{"allocation_end",finish}});s.remaining_credit+=Limits::operation_credit;crash("after_operation_begin");return oid;}
void Journal::complete_operation(std::uint64_t oid){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto o=s.op(oid);s.append(Type::OperationEnd,o.tx,oid,0,{{"mini_tx",o.mini_tx}});crash("after_operation_end");}
void Journal::abort_begin(std::uint64_t id){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto& t=s.tx(id);if(!t.begun)return;if(!t.aborting&&!t.committed)s.append(Type::Abort,id);}
std::vector<Operation> Journal::pending(std::uint64_t id)const{
    std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto& t=s.tx(id);std::vector<Operation> out;if(t.committed)return out;
    auto cursor=t.operations.empty()?0:t.operations.back();std::set<std::uint64_t> visited;
    while(cursor){if(!visited.insert(cursor).second||visited.size()>Limits::operation_history)bad("Cyclic logical UNDO chain");const auto& o=s.op(cursor);if(o.tx!=id)bad("Cross-transaction logical UNDO chain");if(o.compensated)cursor=o.undo_next;else{out.push_back(o);cursor=o.previous;}}
    return out;
}
void Journal::start_undo(std::uint64_t oid,std::uint64_t mini,std::uint64_t start,std::uint64_t finish){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto& o=s.op(oid);if(o.attempts.size()>=Limits::recovery_attempts)throw DbError("LOGICAL_RECOVERY_RETRY_LIMIT","Two inverse attempts exhausted; fail-closed, no rollback completion claimed");s.append(Type::UndoBegin,o.tx,oid,0,{{"mini_tx",mini},{"allocation_start",start},{"allocation_end",finish},{"attempt",o.attempts.size()+1}});crash("after_undo_begin");}
void Journal::compensated(std::uint64_t oid){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto o=s.op(oid);s.append(Type::Clr,o.tx,oid,o.previous);crash("after_logical_clr");}
void Journal::commit(std::uint64_t id){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto& t=s.tx(id);if(!t.begun){s.transactions.erase(id);return;}s.append(Type::Commit,id);crash("after_logical_commit_force");s.append(Type::End,id);}
void Journal::finish_abort(std::uint64_t id){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto& t=s.tx(id);if(!t.begun){s.transactions.erase(id);return;}if(t.committed){s.append(Type::End,id);return;}if(!t.aborting)s.append(Type::Abort,id);s.append(Type::End,id);}
void Journal::savepoint(std::uint64_t id,const std::string& name){std::lock_guard<std::mutex> lock(impl_->mutex);auto& t=impl_->tx(id);if(name.empty()||name.size()>128)throw DbError("INVALID_SAVEPOINT","Logical savepoint name must have1..128 UTF-8 bytes");auto p=std::find_if(t.savepoints.begin(),t.savepoints.end(),[&](const auto& v){return v.first==name;});if(p!=t.savepoints.end())t.savepoints.erase(p,t.savepoints.end());if(t.savepoints.size()>=Limits::savepoints)throw DbError("SAVEPOINT_LIMIT","Logical savepoint limit64");t.savepoints.push_back({name,t.operations.size()});}
std::vector<Operation> Journal::after_savepoint(std::uint64_t id,const std::string& name)const{
    std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;auto& t=s.tx(id);auto point=std::find_if(t.savepoints.begin(),t.savepoints.end(),[&](const auto& v){return v.first==name;});if(point==t.savepoints.end())throw DbError("SAVEPOINT_NOT_FOUND","Unknown logical savepoint");if(point->second>t.operations.size())bad("Invalid logical savepoint position");
    auto target=point->second?t.operations[point->second-1]:0;auto cursor=t.operations.empty()?0:t.operations.back();std::vector<Operation> out;std::set<std::uint64_t> visited;
    while(cursor!=target){if(!cursor||!visited.insert(cursor).second||visited.size()>Limits::operation_history)bad("Broken logical savepoint UNDO chain");const auto& o=s.op(cursor);if(o.tx!=id)bad("Cross-transaction savepoint UNDO");if(o.compensated)cursor=o.undo_next;else{out.push_back(o);cursor=o.previous;}}
    return out;
}
void Journal::keep_savepoint(std::uint64_t id,const std::string& name){std::lock_guard<std::mutex> lock(impl_->mutex);auto& t=impl_->tx(id);auto p=std::find_if(t.savepoints.begin(),t.savepoints.end(),[&](const auto& v){return v.first==name;});if(p==t.savepoints.end())throw DbError("SAVEPOINT_NOT_FOUND","Unknown logical savepoint");t.savepoints.erase(p+1,t.savepoints.end());}
void Journal::release_savepoint(std::uint64_t id,const std::string& name){std::lock_guard<std::mutex> lock(impl_->mutex);auto& t=impl_->tx(id);auto p=std::find_if(t.savepoints.begin(),t.savepoints.end(),[&](const auto& v){return v.first==name;});if(p==t.savepoints.end())throw DbError("SAVEPOINT_NOT_FOUND","Unknown logical savepoint");t.savepoints.erase(p,t.savepoints.end());}
json Journal::savepoint_stats(std::uint64_t id)const{std::lock_guard<std::mutex> lock(impl_->mutex);std::size_t names=0,count=0;auto it=impl_->transactions.find(id);if(it!=impl_->transactions.end()){count=it->second.savepoints.size();for(const auto& p:it->second.savepoints)names+=p.first.size();}return {{"count",count},{"bytes",names+count*(sizeof(std::string)+sizeof(std::size_t))},{"scope","name bytes plus bounded metadata entries; no copied page images or process-RSS claim"}};}
void Journal::synchronize_reservations(aries::Manager& physical){std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require();std::uint64_t total=0;for(const auto& item:s.operations){const auto& o=item.second;if(o.compensated||s.transactions.at(o.tx).committed)continue;std::uint64_t spent=0;for(const auto& attempt:o.attempts)spent+=physical.transaction_record_bytes(attempt.tx);if(spent>Limits::operation_credit)throw DbError("COMPENSATION_CREDIT","Persisted inverse consumed more than pre-reserved credit");total+=Limits::operation_credit-spent;}physical.set_external_reservation(total);s.remaining_credit=total;}
std::uint64_t Journal::remaining_pages()const{std::lock_guard<std::mutex> lock(impl_->mutex);std::uint64_t total=0;for(const auto& item:impl_->operations){auto& o=item.second;if(o.compensated||impl_->transactions.at(o.tx).committed)continue;std::uint64_t used=0;for(const auto& a:o.attempts)used+=a.allocation_end-a.allocation_start;if(used>Limits::operation_pages)throw DbError("LOGICAL_PAGE_LIMIT","Undo allocation reservation exceeded");total+=Limits::operation_pages-used;}return total;}
std::vector<std::uint64_t> Journal::loser_transactions()const{std::lock_guard<std::mutex> lock(impl_->mutex);std::vector<std::uint64_t> ids;for(const auto& item:impl_->transactions)ids.push_back(item.first);return ids;}
std::uint64_t Journal::max_id()const{std::lock_guard<std::mutex> lock(impl_->mutex);return impl_->maximum_id;}
std::uint64_t Journal::allocation_high_water()const{std::lock_guard<std::mutex> lock(impl_->mutex);return impl_->allocation_end;}
bool Journal::failed()const{std::lock_guard<std::mutex> lock(impl_->mutex);return impl_->failstop;}
json Journal::wal_records(std::size_t limit)const{
    if(limit>65536)throw DbError("ARGUMENT","Logical WAL inspection limit exceeds65536");
    std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;s.require();
    static const char* names[]={"LOGICAL_BEGIN","LOGICAL_OPERATION","LOGICAL_OP_END","LOGICAL_COMMIT","LOGICAL_ABORT","LOGICAL_UNDO_BEGIN","LOGICAL_CLR","LOGICAL_END"};
    const auto maximum=std::min<std::size_t>(limit,1024);constexpr std::size_t byte_budget=1024*1024;
    json rows=json::array();std::size_t serialized=0;std::uint64_t at=64;
    while(at<s.end&&rows.size()<maximum){
        if(s.end-at<header_size)bad("Inspection found a partial logical header");
        std::array<unsigned char,header_size> h{};s.wal->read(at,h.data(),h.size());auto expected=get32(h.data()+12);put32(h.data()+12,0);auto type=get32(h.data()+4),length=get32(h.data()+8);
        if(get32(h.data())!=0x4c475731||get32(h.data()+60)!=1||type<1||type>8||length>16384||length>s.end-at-header_size||expected!=crc(h.data(),h.size()))bad("Logical inspection fixed header/CRC");
        std::vector<unsigned char> payload(length);if(length)s.wal->read(at+header_size,payload.data(),payload.size());if(get32(h.data()+56)!=crc(payload.data(),payload.size()))bad("Logical inspection payload CRC");
        json body=nullptr;if(length){try{body=json::parse(payload.begin(),payload.end(),[](int depth,json::parse_event_t,json&){if(depth>16)bad("Logical inspection nesting bound");return true;});}catch(const DbError&){throw;}catch(...){bad("Logical inspection JSON");}}
        json row={{"lsn",get64(h.data()+16)},{"type",names[type-1]},{"tx",get64(h.data()+24)},{"prev_lsn",get64(h.data()+32)},{"op_id",get64(h.data()+40)},{"undo_next_lsn",get64(h.data()+48)},{"payload_bytes",length},{"offset",at},{"payload",body}};
        auto bytes=row.dump().size();if(bytes>byte_budget-serialized)break;serialized+=bytes;rows.push_back(std::move(row));at+=header_size+length;
    }
    return {{"records",rows},{"total_records",s.lsn},{"truncated",rows.size()<s.lsn},{"limit",maximum},{"serialized_record_bytes",serialized},{"byte_budget",byte_budget},{"sequence","LOGICAL_USER_TRANSACTION; independent of physical mini-transaction LSNs"}};
}
json Journal::diagnostics()const{std::lock_guard<std::mutex> lock(impl_->mutex);auto& s=*impl_;json txs=json::array(),ops=json::array();for(const auto& t:s.transactions)txs.push_back({{"tx",t.first},{"last_lsn",t.second.last},{"committed",t.second.committed},{"aborting",t.second.aborting},{"operations",t.second.operations.size()}});for(const auto& item:s.operations){auto& o=item.second;json tries=json::array();for(const auto& a:o.attempts)tries.push_back({{"mini_tx",a.tx},{"allocation_start",a.allocation_start},{"allocation_end",a.allocation_end}});ops.push_back({{"op_id",o.id},{"tx",o.tx},{"mini_tx",o.mini_tx},{"previous_operation",o.previous},{"complete",o.complete},{"compensated",o.compensated},{"attempts",tries},{"description",o.description}});}return {{"mode","ARIES_LOGICAL_EXPERIMENT"},{"wal_bytes",s.end},{"last_lsn",s.lsn},{"flushed_lsn",s.forced_lsn},{"recovery_required",s.failstop},{"tail_truncated",s.tail_truncated},{"max_id",s.maximum_id},{"allocation_high_water",s.allocation_end},{"remaining_physical_WAL_reservation",s.remaining_credit},{"transactions",txs},{"operations",ops},{"limits",{{"file_pages",Limits::file_pages},{"table_rows",Limits::table_rows},{"row_bytes",Limits::row_bytes},{"active_operations",Limits::active_operations},{"new_pages_per_attempt",Limits::new_pages_per_attempt},{"recovery_attempts",Limits::recovery_attempts},{"physical_inverse_credit_per_operation",Limits::operation_credit},{"new_page_reservation_per_operation",Limits::operation_pages},{"active_transactions",Limits::active_transactions},{"operation_history",Limits::operation_history},{"savepoints_per_transaction",Limits::savepoints},{"savepoint_name_bytes",128},{"single_row_workspace_bytes",Limits::file_pages*aries::page_size},{"logical_WAL_bytes",log_limit},{"descriptor_bytes",6144},{"columns",8},{"primary_plus_same_key_BPLUS_indexes",2},{"tree_height",16}}}};}
}
