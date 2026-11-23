#include "edudb.hpp"
#include "format_copy.hpp"
#include "mvto_control.hpp"
#include "aries_recovery.hpp"
#include "recovery_archive.hpp"
#include "fencing.hpp"
#include "windows_paths.hpp"
#include "aries_logical.hpp"
#include "access.hpp"
#include "io.hpp"
#include "buffer_pool.hpp"
#include "record_codec.hpp"
#include "cluster_heap.hpp"
#include "change_feed.hpp"
#include "page_version_graph.hpp"
#include "execution_progress.hpp"
#include "managed_block_device.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <set>
#include <map>
#include <queue>
#include <thread>
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

namespace edudb {
namespace {
namespace fs = std::filesystem;
// Trivial TLS, one translation unit; never inherited by another thread.
thread_local const void* verified_write_storage=nullptr;
thread_local const void* verified_write_state=nullptr;
constexpr std::size_t PAGE_SIZE = 8192, PAGE_HEADER = 64, SLOT_SIZE = 8;
constexpr std::uint32_t PAGE_MAGIC = 0x45445047, REC_MAGIC = 0x45445752;
constexpr std::uint16_t TYPE_SUPER = 1, TYPE_CATALOG = 2, TYPE_DESCRIPTOR = 3,
                        TYPE_HEAP = 4, TYPE_BTREE = 5, TYPE_RETIRED = 6, TYPE_ACCESS = 7;
using Page = std::array<unsigned char, PAGE_SIZE>;
using PageId = std::uint64_t;
void put16(unsigned char* p, std::uint16_t n) { p[0]=static_cast<unsigned char>(n); p[1]=static_cast<unsigned char>(n>>8); }
void put32(unsigned char* p, std::uint32_t n) { for (int i=0;i<4;++i) p[i]=static_cast<unsigned char>(n>>(i*8)); }
void put64(unsigned char* p, std::uint64_t n) { for (int i=0;i<8;++i) p[i]=static_cast<unsigned char>(n>>(i*8)); }
std::uint16_t get16(const unsigned char* p) { return std::uint16_t(p[0]) | (std::uint16_t(p[1])<<8); }
std::uint32_t get32(const unsigned char* p) { std::uint32_t n=0; for(int i=3;i>=0;--i)n=(n<<8)|p[i]; return n; }
std::uint64_t get64(const unsigned char* p) { std::uint64_t n=0; for(int i=7;i>=0;--i)n=(n<<8)|p[i]; return n; }
std::uint32_t crc32(const unsigned char* p, std::size_t size) {
    static const auto table=[] { std::array<std::uint32_t,256> a{}; for(std::uint32_t i=0;i<256;++i){auto c=i;for(int j=0;j<8;++j)c=(c&1)?0xedb88320u^(c>>1):(c>>1);a[i]=c;}return a;}();
    std::uint32_t c=0xffffffffu; for(std::size_t i=0;i<size;++i)c=table[(c^p[i])&255]^(c>>8); return c^0xffffffffu;
}
std::uint32_t text_crc(const std::string& s) { return crc32(reinterpret_cast<const unsigned char*>(s.data()),s.size()); }
[[noreturn]] void io_error(const std::string& what) { throw DbError("IO_ERROR",what); }
void crash_at(const char* point) { const char* p=std::getenv("EDUDB_CRASH_AT"); if(p && std::strcmp(p,point)==0) std::_Exit(86); }

class NativeFile : public DataDevice {
public:
    NativeFile(const fs::path& path, bool create, bool truncate=false, bool exclusive_lock=false, bool exclusive_create=false,bool read_only=false) : path_(media_detail::absolute_path(path)),read_only_(read_only) {
        if(read_only&&(create||truncate||exclusive_create))throw DbError("ARGUMENT","Read-only file cannot be created/truncated");
#ifdef _WIN32
        h_=CreateFileW(media_detail::native_path(path).c_str(),GENERIC_READ|(read_only?0:GENERIC_WRITE),exclusive_lock?0:(read_only?FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE:FILE_SHARE_READ),
                       nullptr,exclusive_create?CREATE_NEW:(truncate?CREATE_ALWAYS:(create?OPEN_ALWAYS:OPEN_EXISTING)),FILE_ATTRIBUTE_NORMAL,nullptr);
        if(h_==INVALID_HANDLE_VALUE) { if(exclusive_lock)throw DbError("DATABASE_LOCKED","数据库已由其他进程打开");io_error("无法打开文件: "+path.u8string()+" ("+std::to_string(GetLastError())+")"); }
#else
        fd_=::open(path.c_str(),(read_only?O_RDONLY:O_RDWR)|(create?O_CREAT:0)|(truncate?O_TRUNC:0)|(exclusive_create?O_EXCL:0),0600);
        if(fd_<0)io_error("无法打开文件: "+path.string());
        if(exclusive_lock && ::flock(fd_,LOCK_EX|LOCK_NB)!=0){::close(fd_);fd_=-1;throw DbError("DATABASE_LOCKED","数据库已由其他进程打开");}
#endif
    }
    ~NativeFile() {
#ifdef _WIN32
        if(h_!=INVALID_HANDLE_VALUE)CloseHandle(h_);
#else
        if(fd_>=0)::close(fd_);
#endif
    }
    std::uint64_t size() {
        std::lock_guard<std::mutex> g(mu_);
#ifdef _WIN32
        LARGE_INTEGER n{};if(!GetFileSizeEx(h_,&n))io_error("读取文件大小失败");return static_cast<std::uint64_t>(n.QuadPart);
#else
        struct stat s{};if(::fstat(fd_,&s)!=0)io_error("读取文件大小失败");return static_cast<std::uint64_t>(s.st_size);
#endif
    }
    void read(std::uint64_t offset, unsigned char* dst, std::size_t n) {
        std::lock_guard<std::mutex> g(mu_);
#ifdef _WIN32
        LARGE_INTEGER pos{};pos.QuadPart=static_cast<LONGLONG>(offset);if(!SetFilePointerEx(h_,pos,nullptr,FILE_BEGIN))io_error("文件定位失败");
        while(n){DWORD got=0;auto part=static_cast<DWORD>(std::min<std::size_t>(n,1u<<30));auto started=std::chrono::steady_clock::now();auto ok=ReadFile(h_,dst,part,&got,nullptr);io_record(path_,"read",got,io_elapsed(started),!ok||!got,offset);if(!ok||!got)io_error("文件读取不足");dst+=got;n-=got;offset+=got;}
#else
        while(n){auto started=std::chrono::steady_clock::now();auto r=::pread(fd_,dst,n,static_cast<off_t>(offset));auto err=errno;io_record(path_,"read",r>0?static_cast<std::uint64_t>(r):0,io_elapsed(started),r<=0,offset);if(r<0&&err==EINTR)continue;if(r<=0)io_error("文件读取不足");dst+=r;n-=static_cast<std::size_t>(r);offset+=static_cast<std::uint64_t>(r);}
#endif
    }
    void write(std::uint64_t offset, const unsigned char* src, std::size_t n) {
        if(read_only_)throw DbError("READ_ONLY_DATABASE","Read-only file cannot be written");
        std::lock_guard<std::mutex> g(mu_);
#ifdef _WIN32
        LARGE_INTEGER pos{};pos.QuadPart=static_cast<LONGLONG>(offset);if(!SetFilePointerEx(h_,pos,nullptr,FILE_BEGIN))io_error("文件定位失败");
        while(n){DWORD got=0;auto part=static_cast<DWORD>(std::min<std::size_t>(n,1u<<30));auto started=std::chrono::steady_clock::now();auto ok=WriteFile(h_,src,part,&got,nullptr);io_record(path_,"write",got,io_elapsed(started),!ok||!got,offset);if(!ok||!got)io_error("文件写入不足");src+=got;n-=got;offset+=got;}
#else
        while(n){auto started=std::chrono::steady_clock::now();auto r=::pwrite(fd_,src,n,static_cast<off_t>(offset));auto err=errno;io_record(path_,"write",r>0?static_cast<std::uint64_t>(r):0,io_elapsed(started),r<=0,offset);if(r<0&&err==EINTR)continue;if(r<=0)io_error("文件写入不足");src+=r;n-=static_cast<std::size_t>(r);offset+=static_cast<std::uint64_t>(r);}
#endif
    }
    void sync() {
        if(read_only_)return;
        std::lock_guard<std::mutex> g(mu_);
#ifdef _WIN32
        auto started=std::chrono::steady_clock::now();auto ok=FlushFileBuffers(h_);io_record(path_,"sync",0,io_elapsed(started),!ok);if(!ok)io_error("FlushFileBuffers失败");
#else
        auto started=std::chrono::steady_clock::now();auto r=::fsync(fd_);io_record(path_,"sync",0,io_elapsed(started),r!=0);if(r!=0)io_error("fsync失败");
#endif
    }
    void truncate(std::uint64_t n) {
        if(read_only_)throw DbError("READ_ONLY_DATABASE","Read-only file cannot be truncated");
        std::lock_guard<std::mutex> g(mu_);
#ifdef _WIN32
        auto started=std::chrono::steady_clock::now();LARGE_INTEGER pos{};pos.QuadPart=static_cast<LONGLONG>(n);auto ok=SetFilePointerEx(h_,pos,nullptr,FILE_BEGIN)&&SetEndOfFile(h_);io_record(path_,"truncate",0,io_elapsed(started),!ok,n);if(!ok)io_error("截断文件失败");
#else
        auto started=std::chrono::steady_clock::now();auto r=::ftruncate(fd_,static_cast<off_t>(n));io_record(path_,"truncate",0,io_elapsed(started),r!=0,n);if(r!=0)io_error("截断文件失败");
#endif
    }
private:
    fs::path path_;std::mutex mu_;bool read_only_=false;
#ifdef _WIN32
    HANDLE h_=INVALID_HANDLE_VALUE;
#else
    int fd_=-1;
#endif
};
void sync_directory(const fs::path& path) {
#ifndef _WIN32
    int f=::open(path.c_str(),O_RDONLY|O_DIRECTORY);if(f<0)io_error("打开目录以同步失败");int r=::fsync(f);::close(f);if(r!=0)io_error("同步目录失败");
#else
    (void)path; // Process-crash model. Windows manifest replacement uses WRITE_THROUGH.
#endif
}
void atomic_manifest(const fs::path& directory, json value) {
    value.erase("checksum");value["checksum"]=text_crc(value.dump());auto bytes=value.dump(2);
    auto temp=directory/"control.tmp";{NativeFile file(temp,true,true);file.write(0,reinterpret_cast<const unsigned char*>(bytes.data()),bytes.size());file.sync();}
#ifdef _WIN32
    if(!MoveFileExW(media_detail::native_path(temp).c_str(),media_detail::native_path(directory/"control.json").c_str(),MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH))io_error("切换控制清单失败");
#else
    fs::rename(temp,directory/"control.json");sync_directory(directory);
#endif
}
json read_manifest(const fs::path& path) {
    NativeFile f(path,false,false,false,false,true);auto size=f.size();if(size>1024u*1024u)throw DbError("CORRUPT_CONTROL","Control manifest too large");std::vector<unsigned char> bytes(static_cast<std::size_t>(size));if(size)f.read(0,bytes.data(),bytes.size());json result;
    try{result=json::parse(bytes.begin(),bytes.end());auto sum=result.at("checksum").get<std::uint32_t>();auto copy=result;copy.erase("checksum");if(sum!=text_crc(copy.dump()))throw DbError("CORRUPT_CONTROL","控制清单校验失败");}
    catch(const DbError&){throw;}catch(const std::exception&){throw DbError("CORRUPT_CONTROL","控制清单格式错误");}return result;
}
std::string uuid() {std::random_device r;std::ostringstream s;s<<std::hex<<std::setfill('0');for(int i=0;i<4;++i)s<<std::setw(8)<<r();return s.str().substr(0,32);}
Page empty_page(PageId id,std::uint16_t type) {
    Page p{};put32(p.data(),PAGE_MAGIC);put16(p.data()+4,3);put16(p.data()+6,type);put64(p.data()+8,id);put16(p.data()+34,PAGE_HEADER);put16(p.data()+36,PAGE_SIZE);return p;
}
void seal(Page& p,std::uint64_t lsn) {put64(p.data()+16,lsn);put32(p.data()+60,0);put32(p.data()+60,crc32(p.data(),p.size()));}
bool valid_layout(const Page& p) {
    const auto count=get16(p.data()+32),lower=get16(p.data()+34),upper=get16(p.data()+36),type=get16(p.data()+6);
    return type>=TYPE_SUPER&&type<=TYPE_ACCESS&&count<=(PAGE_SIZE-PAGE_HEADER)/SLOT_SIZE&&
           lower==PAGE_HEADER+std::size_t(count)*SLOT_SIZE&&lower<=upper&&upper<=PAGE_SIZE;
}
bool valid_page(Page p,PageId id) {auto c=get32(p.data()+60);put32(p.data()+60,0);return get32(p.data())==PAGE_MAGIC&&get16(p.data()+4)==3&&get64(p.data()+8)==id&&valid_layout(p)&&c==crc32(p.data(),p.size());}
std::size_t slot_offset(const Page& p,std::size_t i,bool stale=false) {
    if(!valid_layout(p))throw DbError("CORRUPT_PAGE","Invalid page slot-directory/free-space bounds");
    if(i>=get16(p.data()+32))throw DbError(stale?"STALE_ROW_ID":"CORRUPT_PAGE","Row slot does not exist");
    const auto offset=PAGE_HEADER+i*SLOT_SIZE;
    if(!get32(p.data()+offset+4))throw DbError("CORRUPT_PAGE","Slot generation must be nonzero");
    return offset;
}
bool append_slot(Page& p,const std::string& record) {
    auto count=get16(p.data()+32),start=get16(p.data()+34),end=get16(p.data()+36);
    if(record.size()>PAGE_SIZE || std::size_t(start)+SLOT_SIZE+record.size()>end)return false;
    end=static_cast<std::uint16_t>(end-record.size());std::memcpy(p.data()+end,record.data(),record.size());
    put16(p.data()+start,end);put16(p.data()+start+2,static_cast<std::uint16_t>(record.size()));put32(p.data()+start+4,1);
    put16(p.data()+32,static_cast<std::uint16_t>(count+1));put16(p.data()+34,static_cast<std::uint16_t>(start+SLOT_SIZE));put16(p.data()+36,end);return true;
}
std::string slot(const Page& p,std::size_t i) {
    auto start=slot_offset(p,i);
    auto offset=get16(p.data()+start),length=get16(p.data()+start+2);
    if(start+SLOT_SIZE>get16(p.data()+34)||offset<get16(p.data()+36)||std::size_t(offset)+length>PAGE_SIZE)throw DbError("CORRUPT_PAGE","槽目录越界");
    return std::string(reinterpret_cast<const char*>(p.data()+offset),length);
}
json parse_json(const std::string& s) {try{return json::parse(s);}catch(const std::exception&){throw DbError("CORRUPT_PAGE","持久化JSON记录损坏");}}
bool compare(std::int64_t a,const std::string& op,std::int64_t b) {
    if(op=="=")return a==b;
    if(op=="<")return a<b;
    if(op=="<=")return a<=b;
    if(op==">")return a>b;
    if(op==">=")return a>=b;
    if(op=="!="||op=="<>")return a!=b;
    throw DbError("BAD_OPERATOR","未知索引比较操作");
}
} // namespace

struct Transaction::State {
    void* owner=nullptr;bool active=true,aborted=false;std::recursive_mutex mutex;
    std::unordered_map<PageId,Page> pages;std::unordered_set<PageId> new_pages;
    std::uint64_t heap_reads=0,index_reads=0,wal_bytes=0,last_pages=0;
    std::array<std::uint64_t,8> private_page_types{},last_page_types{};
    void rebuild_private_page_types(){private_page_types.fill(0);for(const auto& page:pages){auto type=get16(page.second.data()+6);if(type>=private_page_types.size())throw DbError("INTERNAL_ERROR","Invalid private page type");++private_page_types[type];}}
    std::unordered_map<PageId,json> descriptors;
    std::unordered_map<std::string,PageId> descriptor_names;
    std::set<PageId> dirty_descriptors;
    std::uint64_t rows_fetched=0,cover_hits=0,bitmap_words=0;
    std::unordered_map<PageId,std::map<std::uint64_t,PageId>> moved_rows;
    std::set<PageId> ch10_layout_touched;
    std::unordered_map<PageId,json> ch10_physical_cache;
    TransactionChangeFeed changefeed;
    std::map<PageId,std::uint16_t> aries_pages;
    std::uint64_t aries_last_dirty=0;
    std::string logical_kind;
    bool logical_staging=false,logical_compensating=false,logical_scanning=false;
    std::uint64_t logical_inner=0,logical_allocation_start=0;
    #include "transaction_state_fields.inc"
};
namespace {
std::uint16_t slot_length(const Page& p,std::size_t i){return get16(p.data()+slot_offset(p,i,true)+2);}
std::uint32_t slot_generation(const Page& p,std::size_t i){return get32(p.data()+slot_offset(p,i,true)+4);}
bool repack(Page& p,std::size_t target,const std::string& bytes,bool append){
    auto old=get16(p.data()+32);std::size_t count=old+(append?1:0),total=0;std::vector<std::string> records;
    for(std::size_t i=0;i<count;++i){auto value=i==target?bytes:(i<old&&slot_length(p,i)?slot(p,i):std::string());total+=value.size();records.push_back(std::move(value));}
    if(PAGE_HEADER+count*SLOT_SIZE+total>PAGE_SIZE)return false;
    std::size_t end=PAGE_SIZE;for(std::size_t i=0;i<count;++i){end-=records[i].size();if(!records[i].empty())std::memcpy(p.data()+end,records[i].data(),records[i].size());put16(p.data()+PAGE_HEADER+i*SLOT_SIZE,static_cast<std::uint16_t>(end));put16(p.data()+PAGE_HEADER+i*SLOT_SIZE+2,static_cast<std::uint16_t>(records[i].size()));}
    put16(p.data()+32,static_cast<std::uint16_t>(count));put16(p.data()+34,static_cast<std::uint16_t>(PAGE_HEADER+count*SLOT_SIZE));put16(p.data()+36,static_cast<std::uint16_t>(end));return true;
}
std::optional<RowId> heap_append(Page& p,const std::string& bytes){auto count=get16(p.data()+32);std::size_t target=count;std::uint32_t gen=1;
    for(std::size_t i=0;i<count;++i)if(!slot_length(p,i)&&slot_generation(p,i)<std::numeric_limits<std::uint32_t>::max()){target=i;gen=slot_generation(p,i)+1;break;}
    if(!repack(p,target,bytes,target==count))return std::nullopt;
    put32(p.data()+PAGE_HEADER+target*SLOT_SIZE+4,gen);return RowId{get64(p.data()+8),static_cast<std::uint16_t>(target),gen};
}
class FunctionCursor final:public RowCursor {std::function<bool(StoredRow&)> callback_;public:explicit FunctionCursor(std::function<bool(StoredRow&)> f):callback_(std::move(f)){}bool next(StoredRow& row)override{return callback_(row);}};
}
struct Storage::Impl {
    // Declared first, destroyed last: recovery-copy cleanup must follow closure
    // of buffer, ARIES authority, WAL/data and process-lock handles.
    std::unique_ptr<format_copy::LegacySnapshot> legacy_snapshot;
    bool legacy_read_only=false;
    std::unique_ptr<buffer::BufferPool> buffer;
    std::unique_ptr<aries::Manager> aries_manager;
    bool aries_mode=false,logical_mode=false;
    std::unique_ptr<logical::Journal> logical_journal;
    std::recursive_mutex logical_mutex;
    std::uint64_t logical_provisional_pages=0;
    aries::Options aries_options;
    fs::path directory;std::unique_ptr<NativeFile> process_lock,wal;std::unique_ptr<DataDevice> data;std::string db_uuid;std::size_t frame_limit,private_limit;
    json data_layout={{"kind","single"}};
    std::atomic<PageId> next_page{2};std::atomic<std::uint64_t> next_tx{1};std::uint64_t generation=1,next_lsn=1,checkpoint_lsn=0,tick=0;std::atomic<std::uint64_t> durable_lsn{0};
    std::atomic<bool> failed{false};std::mutex wal_mutex,registry_mutex;std::condition_variable registry_cv;bool checkpoint_requested=false;
    std::unordered_map<std::uint64_t,std::shared_ptr<Transaction::State>> active;
    std::atomic<std::uint64_t> hits{0},misses{0},evictions{0},writes{0},commits{0},rollbacks{0},recovered{0},index_reads{0},heap_reads{0},wal_forces{0},splits{0},merges{0},redistributions{0},root_shrinks{0},retired{0},io_faults{0},wal_committers{0},failed_waiters{0},failed_pre_wal_commits{0};
    bool truncated_tail=false;std::string fault;int fault_delay=0;bool fault_pending=false;
    std::uint64_t stale_sort_files_removed=0,unrecognized_sort_files_retained=0;
    std::uint64_t stale_query_files_removed=0,unrecognized_query_files_retained=0;
    #include "storage_protocol_fields.inc"
    #include "storage_backup_fields.inc"
    Impl(const std::string& dir,std::size_t count,std::size_t limit,const StorageOptions& options):directory(media_detail::absolute_path(fs::u8path(dir))),frame_limit(count),private_limit(limit){
        if(count<2||limit<PAGE_SIZE)throw DbError("INVALID_CONFIG","At least two frames and one private page required");
        fs::create_directories(directory);bool exists=fs::exists(directory/"control.json");
        if(exists){auto probe=read_manifest(directory/"control.json");auto version=probe.value("version",0);if(version!=3&&version!=4)throw DbError("INCOMPATIBLE_FORMAT","EduDB requires control4/page3; legacy control3 is read-only");if(version==3){legacy_snapshot=std::make_unique<format_copy::LegacySnapshot>(directory);legacy_snapshot->promote_owned_copy();directory=legacy_snapshot->path();legacy_read_only=true;}}
        process_lock=std::make_unique<NativeFile>(directory/"database.lock",true,false,true);io_reset(directory);IoOwnerScope constructor_io_owner(directory);
        if(!exists&&fs::exists(directory/"data.pages")&&fs::file_size(directory/"data.pages"))throw DbError("CORRUPT_CONTROL","Data exists without manifest; refusing overwrite");
        auto c=exists?read_manifest(directory/"control.json"):json::object();
        const auto persistent_mode=exists?c.value("recovery_mode",std::string("REDO_PRIVATE")):(options.recovery_mode.empty()?std::string("REDO_PRIVATE"):options.recovery_mode);
        if(persistent_mode!="REDO_PRIVATE"&&persistent_mode!="ARIES_PAGE"&&persistent_mode!="ARIES_LOGICAL")throw DbError("RECOVERY_MODE","Recovery mode must be REDO_PRIVATE, ARIES_PAGE or ARIES_LOGICAL");
        if(exists&&!options.recovery_mode.empty()&&options.recovery_mode!=persistent_mode)throw DbError("RECOVERY_MODE","Changing recovery mode requires export/import into a new data directory");
        logical_mode=persistent_mode=="ARIES_LOGICAL";aries_mode=persistent_mode=="ARIES_PAGE"||logical_mode;aries_options.buffer_frames=count;
        aries_options.steal=exists?c.value("aries_steal",true):options.aries_steal.value_or(true);
        aries_options.force_at_commit=exists?c.value("aries_force_at_commit",false):options.aries_force_at_commit.value_or(false);
        if(logical_mode&&!aries_options.steal)throw DbError("LOGICAL_MODE_POLICY","ARIES_LOGICAL requires steal=true");
        if(logical_mode&&private_limit<logical::Limits::file_pages*PAGE_SIZE)throw DbError("LOGICAL_WORKSPACE_LIMIT","ARIES_LOGICAL requires at least8MiB bounded single-operation workspace");
        if(!aries_mode&&(options.aries_steal.has_value()||options.aries_force_at_commit.has_value()))throw DbError("RECOVERY_MODE","ARIES policies require ARIES_PAGE or ARIES_LOGICAL mode");
        if(exists&&((options.aries_steal&&*options.aries_steal!=aries_options.steal)||(options.aries_force_at_commit&&*options.aries_force_at_commit!=aries_options.force_at_commit)))throw DbError("RECOVERY_MODE","Changing persisted ARIES policies requires export/import");
        if((fs::exists(directory/"backup-incomplete.json")&&!fs::exists(directory/"backup-complete.json"))||(fs::exists(directory/"restore-incomplete.json")&&!fs::exists(directory/"restore-complete.json")))throw DbError("RECOVERY_INCOMPLETE","Backup/restore publication did not complete");
        if(exists&&(c.value("version",0)!=4||c.value("page_format",0)!=3||c.value("minimum_reader_format",0)!=4))throw DbError("INCOMPATIBLE_FORMAT","EduDB requires control4/page3 and minimum_reader_format4; use offline migration into a new directory");
        if(exists&&(c.value("format",std::string())!="EduDB"||c.value("page_size",0)!=PAGE_SIZE))throw DbError("INCOMPATIBLE_FORMAT","Unsupported database manifest");
        if(!exists)for(const auto& entry:fs::directory_iterator(directory)){auto name=entry.path().filename().u8string();if(name.rfind("data-member-",0)==0||name=="control-media.json"||name=="control-media-redo.log")throw DbError("CORRUPT_CONTROL","Managed media exists without database manifest; refusing overwrite");}
        db_uuid=exists?c.at("uuid").get<std::string>():uuid();backup_configure(c,options);if(legacy_read_only){backup_role="RESTORED_READ_ONLY";backup_identity_upgrade=false;}
        data_layout=exists?c.value("data_layout",json{{"kind","single"}}):json{{"kind","single"}};
        if(!exists&&!options.data_layout.empty()&&options.data_layout!="SINGLE")data_layout={{"kind","managed"},{"format","edudb-managed-media-v1"},{"layout",options.data_layout},{"members",options.media_members}};
        const auto kind=data_layout.value("kind",std::string());
        if(kind!="single"&&kind!="managed")throw DbError("INCOMPATIBLE_FORMAT","Unknown data device kind");
        auto selected=kind=="single"?std::string("SINGLE"):data_layout.at("layout").get<std::string>();
        if(!options.data_layout.empty()&&options.data_layout!=selected)throw DbError("MEDIA_OPTION","Requested layout differs from persistent database layout");
        if(kind=="single"){
            if(options.media_members)throw DbError("MEDIA_OPTION","Single file does not accept a member count");
            data=std::make_unique<NativeFile>(directory/"data.pages",!exists);
        }else{
            if(data_layout.value("format",std::string())!="edudb-managed-media-v1")throw DbError("INCOMPATIBLE_FORMAT","Unsupported managed media format");
            auto members=data_layout.at("members").get<std::size_t>();
            if(options.media_members&&options.media_members!=members)throw DbError("MEDIA_OPTION","Requested member count differs from persistent layout");
            if(fs::exists(directory/"data.pages"))throw DbError("MEDIA_OPTION","Ambiguous single-file and managed data layout");
            data=std::make_unique<ManagedBlockDevice>(directory.u8string(),db_uuid,ManagedMediaOptions{selected,members},!exists);
        }
        if(!exists){initialize();c=read_manifest(directory/"control.json");}
        if(c.value("format","")!="EduDB"||c.value("version",0)!=4||c.value("page_format",0)!=3||c.value("minimum_reader_format",0)!=4||c.value("page_size",0)!=PAGE_SIZE)throw DbError("INCOMPATIBLE_FORMAT","Unsupported control4/page3 database format");
        db_uuid=c.at("uuid");generation=c.at("wal_generation");checkpoint_lsn=c.at("checkpoint_lsn");durable_lsn=checkpoint_lsn;next_lsn=checkpoint_lsn+1;
        next_page=std::max(c.at("next_page_id").get<std::uint64_t>(),(data->size()+PAGE_SIZE-1)/PAGE_SIZE);next_tx=c.value("next_tx_id",std::uint64_t(1));Page super{};data->read(0,super.data(),super.size());
        if(!valid_page(super,0)||get16(super.data()+6)!=TYPE_SUPER||parse_json(slot(super,0)).value("uuid","")!=db_uuid)throw DbError("CORRUPT_PAGE","Superblock/UUID mismatch");
        if(logical_mode)logical_journal=std::make_unique<logical::Journal>(directory.u8string(),db_uuid);
        if(aries_mode)aries_setup();else {wal=std::make_unique<NativeFile>(wal_path(generation),false);recover();setup_buffer();}
        if(logical_mode)logical_setup();
        cleanup_sort_runs();cleanup_query_runs();backup_setup();if(legacy_snapshot)legacy_snapshot->verify_source_unchanged();
        if(const char* p=std::getenv("EDUDB_TEST_IO_FAULT")){fault=p;fault_pending=fault=="wal_append_once"||fault=="wal_sync_once"||fault=="aries_after_commit_force_once"||fault=="aries_after_manager_commit_once";}if(const char* p=std::getenv("EDUDB_TEST_IO_FAULT_DELAY_MS"))fault_delay=std::min(5000,std::max(0,std::atoi(p)));
    }
    void cleanup_sort_runs(){
        // Exclusive database lock is already held. A name alone is never
        // enough to delete a file: validate the reserved format, UUID and CRC.
        const std::string prefix="edudb-sort-";
        for(const auto& item:fs::directory_iterator(directory)){
            auto name=item.path().filename().u8string();if(name.rfind(prefix,0)!=0||name.size()<prefix.size()+32+1+1+4||name.substr(name.size()-4)!=".tmp")continue;
            bool valid=false;
            try{if(item.is_symlink()||!item.is_regular_file()){++unrecognized_sort_files_retained;continue;}auto tail=name.substr(prefix.size()+33,name.size()-4-prefix.size()-33);auto token=name.substr(prefix.size(),32);if(name[prefix.size()+32]!='-'||token.find_first_not_of("0123456789abcdef")!=std::string::npos||tail.empty()||tail.find_first_not_of("0123456789")!=std::string::npos){++unrecognized_sort_files_retained;continue;}auto run=std::stoull(tail);
                {NativeFile file(item.path(),false);if(file.size()>=64){std::array<unsigned char,64> h{};file.read(0,h.data(),h.size());auto c=get32(h.data()+60);put32(h.data()+60,0);valid=!std::memcmp(h.data(),"EDUSRT3",7)&&!std::memcmp(h.data()+8,db_uuid.data(),32)&&get64(h.data()+40)==run&&get32(h.data()+48)==3&&get32(h.data()+52)==64&&c==crc32(h.data(),h.size());}}
                if(valid){std::error_code error;if(fs::remove(item.path(),error)&&!error)++stale_sort_files_removed;else ++unrecognized_sort_files_retained;}else ++unrecognized_sort_files_retained;
            }catch(...){++unrecognized_sort_files_retained;}
        }
    }
    fs::path wal_path(std::uint64_t gen)const{return directory/("wal-"+std::to_string(gen)+".log");}
    std::array<unsigned char,64> wal_header(std::uint64_t gen,std::uint64_t base){std::array<unsigned char,64> h{};std::memcpy(h.data(),"EDUWAL3",7);std::memcpy(h.data()+8,db_uuid.data(),32);put64(h.data()+40,gen);put64(h.data()+48,base);put32(h.data()+60,crc32(h.data(),h.size()));return h;}
    json control(std::uint64_t gen,std::uint64_t lsn){return backup_control({{"format","EduDB"},{"version",4},{"page_format",3},{"minimum_reader_format",4},{"page_size",PAGE_SIZE},{"uuid",db_uuid},{"recovery_mode",logical_mode?"ARIES_LOGICAL":aries_mode?"ARIES_PAGE":"REDO_PRIVATE"},{"aries_steal",aries_options.steal},{"aries_force_at_commit",aries_options.force_at_commit},{"wal_generation",gen},{"checkpoint_lsn",lsn},{"next_page_id",next_page.load()},{"next_tx_id",next_tx.load()},{"data_layout",data_layout}});}
    void initialize(){auto p=empty_page(0,TYPE_SUPER);append_slot(p,json({{"uuid",db_uuid},{"page_size",PAGE_SIZE},{"format_version",3}}).dump());seal(p,0);data->write(0,p.data(),p.size());p=empty_page(1,TYPE_CATALOG);append_slot(p,"{}");seal(p,0);data->write(PAGE_SIZE,p.data(),p.size());data->sync();if(!aries_mode){auto h=wal_header(1,0);NativeFile f(wal_path(1),true,true);f.write(0,h.data(),h.size());f.sync();}sync_directory(directory);atomic_manifest(directory,control(1,0));}
    void require(const std::shared_ptr<Transaction>& tx){if(failed)throw DbError("RECOVERY_REQUIRED","Storage fail-stop; restart required");if(!tx||!tx->state||tx->state->owner!=this||!tx->state->active)throw DbError("NO_TRANSACTION","Inactive transaction/wrong storage owner");if(tx->state->aborted)throw DbError("TRANSACTION_ABORTED","Private write failed; rollback required");}
    void require_writable()const{if(legacy_read_only)throw DbError("READ_ONLY_DATABASE","Legacy control3 is mounted read-only; use migrate-v3 into a new directory");}
    void finish(const std::shared_ptr<Transaction>& tx){protocol_finish(tx);tx->state->last_pages=aries_mode?tx->state->aries_pages.size():tx->state->pages.size();tx->state->aries_pages.clear();tx->state->last_page_types=tx->state->private_page_types;tx->state->private_page_types.fill(0);tx->state->active=false;tx->state->pages.clear();tx->state->changefeed.clear();tx->state->new_pages.clear();tx->state->descriptors.clear();tx->state->descriptor_names.clear();tx->state->dirty_descriptors.clear();tx->state->ch10_layout_touched.clear();tx->state->ch10_physical_cache.clear();{std::lock_guard<std::mutex> g(registry_mutex);active.erase(tx->id);}registry_cv.notify_all();}
    void private_page(const std::shared_ptr<Transaction>& tx,PageId id,Page p){require_writable();if(!(logical_mode&&tx->state->logical_staging&&tx->state->logical_compensating))backup_private_write_guard(tx);if(logical_mode&&tx->state->logical_staging){logical_stage_page(tx,id,std::move(p));return;}if(logical_mode&&tx->state->logical_kind!="physical")throw DbError("LOGICAL_UNSUPPORTED","Unjournaled page write rejected");if(aries_mode){aries_write_page(tx,id,std::move(p));return;}if(!tx->state->pages.count(id)&&(tx->state->pages.size()+1)>private_limit/PAGE_SIZE){tx->state->aborted=true;throw DbError("TRANSACTION_TOO_LARGE","Private page-image limit exceeded");}protocol_record_private_write(tx,id);auto type=get16(p.data()+6);if(type>=tx->state->private_page_types.size())throw DbError("INTERNAL_ERROR","Invalid private page type");auto old=tx->state->pages.find(id);auto previous=old==tx->state->pages.end()?0:get16(old->second.data()+6);tx->state->pages[id]=std::move(p);if(previous)--tx->state->private_page_types[previous];++tx->state->private_page_types[type];}
    PageId allocate(const std::shared_ptr<Transaction>& tx,std::uint16_t type,PageId owner=0){require_writable();if(logical_mode)logical_allocation_check(tx);auto id=next_page.fetch_add(1);if(id>=(std::uint64_t(1)<<48))throw DbError("DATABASE_TOO_LARGE","Page id range exhausted");auto p=empty_page(id,type);put64(p.data()+40,owner);protocol_record_allocation(tx,id);private_page(tx,id,p);tx->state->new_pages.insert(id);ch10_touch(tx,owner);return id;}
    void retire(const std::shared_ptr<Transaction>& tx,PageId id){require_writable();auto previous=read_page(tx,id);ch10_touch(tx,get64(previous.data()+40));++retired;if(tx->state->new_pages.erase(id)){if(aries_mode){private_page(tx,id,empty_page(id,TYPE_RETIRED));protocol_forget_allocation(tx,id);return;}auto page=tx->state->pages.find(id);if(page!=tx->state->pages.end()){--tx->state->private_page_types[get16(page->second.data()+6)];tx->state->pages.erase(page);}protocol_forget_allocation(tx,id);return;}private_page(tx,id,empty_page(id,TYPE_RETIRED));}
    #include "storage_buffer.inc"
    Page read_page(const std::shared_ptr<Transaction>& tx,PageId id){page_progress();return protocol_read_page(tx,id);}
    Page heap_page(const std::shared_ptr<Transaction>& tx,PageId id,PageId owner){auto p=read_page(tx,id);++heap_reads;++tx->state->heap_reads;if(get16(p.data()+6)!=TYPE_HEAP||get64(p.data()+40)!=owner)throw DbError("CORRUPT_PAGE","Heap owner/type mismatch");return p;}
    std::string read_blob(const std::shared_ptr<Transaction>& tx,PageId id,std::uint16_t type){std::string out;std::unordered_set<PageId> seen;while(id){if(!seen.insert(id).second)throw DbError("CORRUPT_PAGE","Metadata chain cycle");auto p=read_page(tx,id);if(get16(p.data()+6)!=type||get16(p.data()+32)!=1)throw DbError("CORRUPT_PAGE","Metadata type/slots mismatch");out+=slot(p,0);id=get64(p.data()+24);if(out.size()>64u*1024u*1024u)throw DbError("CORRUPT_PAGE","Metadata exceeds format limit");}return out;}
    void write_blob(const std::shared_ptr<Transaction>& tx,PageId root,std::uint16_t type,const std::string& bytes){constexpr auto chunk=PAGE_SIZE-PAGE_HEADER-SLOT_SIZE;PageId id=root;for(std::size_t off=0;;off+=chunk){auto old=read_page(tx,id);auto next=get64(old.data()+24);auto p=empty_page(id,type);put64(p.data()+40,get64(old.data()+40));auto len=std::min(chunk,bytes.size()-off);append_slot(p,bytes.substr(off,len));bool more=off+len<bytes.size();if(more&&!next)next=allocate(tx,type,get64(old.data()+40));put64(p.data()+24,more?next:0);private_page(tx,id,p);if(!more){while(next){auto q=read_page(tx,next);auto after=get64(q.data()+24);retire(tx,next);next=after;}break;}id=next;}}
    json read_catalog(const std::shared_ptr<Transaction>& tx){return cluster_catalog(tx).tables;}
    std::pair<PageId,json> descriptor(const std::shared_ptr<Transaction>& tx,const std::string& name){auto n=tx->state->descriptor_names.find(name);if(n!=tx->state->descriptor_names.end())return {n->second,tx->state->descriptors.at(n->second)};auto cat=read_catalog(tx);if(!cat.contains(name))throw DbError("TABLE_NOT_FOUND","Table not found: "+name);auto id=cat.at(name).get<PageId>();auto j=parse_json(read_blob(tx,id,TYPE_DESCRIPTOR));tx->state->descriptor_names[name]=id;tx->state->descriptors[id]=j;return {id,j};}
    void save_desc(const std::shared_ptr<Transaction>& tx,PageId id,const json& desc){
        auto counts=[](const json& value){std::array<std::uint64_t,4> out{};std::function<void(const json&)> visit=[&](const json& j){if(j.is_object()){if(j.contains("stats")&&j["stats"].is_object()){const char* keys[]={"splits","merges","redistributions","root_shrinks"};for(int i=0;i<4;++i)out[i]+=j["stats"].value(keys[i],std::uint64_t(0));}for(auto it=j.begin();it!=j.end();++it)if(it.key()!="meta"&&it.key()!="stats")visit(it.value());}else if(j.is_array())for(const auto& v:j)visit(v);};visit(value);return out;};
        auto after=counts(desc);std::array<std::uint64_t,4> before{};auto old=tx->state->descriptors.find(id);if(old!=tx->state->descriptors.end())before=counts(old->second);std::atomic<std::uint64_t>* counters[]={&splits,&merges,&redistributions,&root_shrinks};for(int i=0;i<4;++i)if(after[i]>before[i])*counters[i]+=after[i]-before[i];
        auto next=desc;if(tx->state->ch10_layout_touched.erase(id))ch10_bump(next,"physical_version");tx->state->descriptors[id]=std::move(next);tx->state->dirty_descriptors.insert(id);tx->state->ch10_physical_cache.erase(id);}
    void flush_descriptors(const std::shared_ptr<Transaction>& tx){if(tx->state->dirty_descriptors.empty())return;std::optional<BackupWriteOperation> operation;if(!(logical_mode&&tx->state->logical_staging&&tx->state->logical_compensating))operation.emplace(*this,tx);for(auto id:tx->state->dirty_descriptors)write_blob(tx,id,TYPE_DESCRIPTOR,tx->state->descriptors.at(id).dump());tx->state->dirty_descriptors.clear();}
    #include "storage_cluster.inc"
    #include "storage_record.inc"
    #include "storage_v3.inc"
    #include "storage_changefeed.inc"
    #include "storage_sort.inc"
    #include "storage_ch10.inc"
    #include "storage_transactions.inc"
    #include "storage_aries.inc"
    #include "storage_backup.inc"
    #include "storage_logical.inc"
    void inject(const char* phase){if(fault_pending&&fault==phase){fault_pending=false;++io_faults;
        // Test-only bounded barrier: let a second writer pass its initial
        // failed check and block on this writer's WAL mutex before I/O fails.
        auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(fault_delay);
        while(fault_delay&&wal_committers.load()<2&&std::chrono::steady_clock::now()<deadline)std::this_thread::sleep_for(std::chrono::milliseconds(1));
        throw DbError("IO_ERROR","Injected one-shot "+fault);}}
    std::vector<unsigned char> record(std::uint32_t type,std::uint64_t lsn,std::uint64_t txid,const std::vector<unsigned char>& payload){std::vector<unsigned char> bytes(48+payload.size(),0);put32(bytes.data(),REC_MAGIC);put32(bytes.data()+4,static_cast<std::uint32_t>(payload.size()));put32(bytes.data()+8,type);put32(bytes.data()+12,3);put64(bytes.data()+16,lsn);put64(bytes.data()+24,txid);put32(bytes.data()+32,crc32(payload.data(),payload.size()));put32(bytes.data()+36,crc32(bytes.data(),48));std::copy(payload.begin(),payload.end(),bytes.begin()+48);return bytes;}
    void append_record(std::uint32_t type,std::uint64_t lsn,const std::shared_ptr<Transaction>& tx,const std::vector<unsigned char>& payload){if(type==2)inject("wal_append_once");auto bytes=record(type,lsn,tx->id,payload);wal->write(wal->size(),bytes.data(),bytes.size());tx->state->wal_bytes+=bytes.size();}
    static std::uint32_t digest_page(std::uint32_t digest,const Page& p){return ((digest<<1)|(digest>>31))^crc32(p.data(),p.size());}
    void recover(){std::array<unsigned char,64> h{};if(wal->size()<h.size())throw DbError("CORRUPT_WAL","Truncated generation header");wal->read(0,h.data(),h.size());auto sum=get32(h.data()+60);put32(h.data()+60,0);if(std::memcmp(h.data(),"EDUWAL3",7)||std::string(reinterpret_cast<char*>(h.data()+8),32)!=db_uuid||get64(h.data()+40)!=generation||get64(h.data()+48)!=checkpoint_lsn||sum!=crc32(h.data(),h.size()))throw DbError("CORRUPT_WAL","Generation header checksum/identity mismatch");
        auto size=wal->size();std::uint64_t off=64,last_commit=64,last_lsn=checkpoint_lsn,pending_id=0;std::vector<std::pair<PageId,Page>> pending;std::uint32_t digest=0;bool tail=false;
        while(off<size){if(size-off<48){tail=true;break;}std::array<unsigned char,48> header{};wal->read(off,header.data(),header.size());auto hc=get32(header.data()+36);put32(header.data()+36,0);if(hc!=crc32(header.data(),header.size())||get32(header.data())!=REC_MAGIC||get32(header.data()+12)!=3)throw DbError("CORRUPT_WAL","Record header checksum/version mismatch");auto len=get32(header.data()+4),type=get32(header.data()+8);if((type==1&&len!=0)||(type==2&&len!=PAGE_SIZE+8)||(type==3&&len!=8)||type<1||type>3)throw DbError("CORRUPT_WAL","Type has invalid fixed payload length");if(off+48+len>size){tail=true;break;}std::vector<unsigned char> payload(len);if(len)wal->read(off+48,payload.data(),len);if(get32(header.data()+32)!=crc32(payload.data(),payload.size()))throw DbError("CORRUPT_WAL","Complete payload checksum mismatch");auto lsn=get64(header.data()+16),txid=get64(header.data()+24);if(lsn<=last_lsn)throw DbError("CORRUPT_WAL","Non-increasing LSN");last_lsn=lsn;next_lsn=lsn+1;next_tx=std::max(next_tx.load(),txid+1);
            if(type==1){if(pending_id)throw DbError("CORRUPT_WAL","Nested transaction");pending_id=txid;digest=0;pending.clear();}
            else if(type==2){if(!pending_id||pending_id!=txid)throw DbError("CORRUPT_WAL","PAGE without BEGIN");auto id=get64(payload.data());Page p{};std::copy(payload.begin()+8,payload.end(),p.begin());if(id<1||id>=(std::uint64_t(1)<<48)||!valid_page(p,id)||get64(p.data()+16)!=lsn)throw DbError("CORRUPT_WAL","Invalid page image");pending.emplace_back(id,p);digest=digest_page(digest,p);next_page=std::max(next_page.load(),id+1);if(pending.size()>131072)throw DbError("CORRUPT_WAL","Transaction page limit exceeded");}
            else{if(!pending_id||pending_id!=txid||get32(payload.data())!=pending.size()||get32(payload.data()+4)!=digest)throw DbError("CORRUPT_WAL","COMMIT digest mismatch");for(const auto& pair:pending){Page old{};bool present=false;if(data->size()>=(pair.first+1)*PAGE_SIZE){data->read(pair.first*PAGE_SIZE,old.data(),old.size());present=valid_page(old,pair.first)&&get64(old.data()+16)>=get64(pair.second.data()+16);}if(!present){data->write(pair.first*PAGE_SIZE,pair.second.data(),pair.second.size());++recovered;}}durable_lsn=lsn;last_commit=off+48+len;pending_id=0;pending.clear();}off+=48+len;
        }if(recovered)data->sync();if(tail||pending_id){
            // A normal incomplete append cannot have reached data under no-steal/WAL-before-data.
            // If valid data pages are newer, acknowledged WAL has been removed or corrupted.
            // Preserve the log for diagnosis instead of discarding the only remaining evidence.
            const auto full_pages=data->size()/PAGE_SIZE;
            for(PageId id=0;id<full_pages;++id){Page p{};data->read(id*PAGE_SIZE,p.data(),p.size());if(valid_page(p,id)&&get64(p.data()+16)>durable_lsn.load())throw DbError("CORRUPT_WAL","Incomplete WAL tail conflicts with a newer durable data page");}
            wal->truncate(last_commit);wal->sync();truncated_tail=true;next_lsn=durable_lsn+1;
        }
    }
};
Storage::Storage(const std::string& d,std::size_t f,std::size_t p,const StorageOptions& options):impl_(new Impl(d,f,p,options)){}
bool Storage::recovery_required()const{return impl_->failed.load();}
bool Storage::logical_recovery_mode()const{return impl_->logical_mode;}
bool Storage::read_only()const{return impl_->legacy_read_only;}
void Storage::enable_changefeed(const std::shared_ptr<Transaction>& tx,const std::vector<std::string>& tables){impl_->require_writable();if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);tx->state->changefeed.enable(tables);}
json Storage::read_changefeed(const std::shared_ptr<Transaction>& tx,const std::string& table,std::uint64_t after){if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);impl_->require(tx);return tx->state->changefeed.read(table,after);}
void Storage::set_materialized_metadata(const std::shared_ptr<Transaction>& tx,const std::string& name,const json& state){impl_->require_writable();
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;s.require(tx);Impl::BackupWriteOperation operation(*impl_,tx);
    if(tx->state->protocol->options.read_only)throw DbError("READ_ONLY_TRANSACTION","Read-only transaction cannot update materialized refresh state");
    if(!state.is_object()||state.dump().size()>256u*1024u)throw DbError("MATERIALIZED_STATE_LIMIT","Refresh state must be an object within256KiB; group accumulators belong in their auxiliary table");
    auto pair=s.descriptor(tx,name);auto next=pair.second;if(!next.at("meta").contains("materialized_view")||!next["meta"]["materialized_view"].is_object())throw DbError("NOT_MATERIALIZED_VIEW","Refresh metadata update requires a materialized view");
    if(next["meta"].value("materialized_refresh_state",json::object())==state)return;
    next["meta"]["materialized_refresh_state"]=state;s.save_desc(tx,pair.first,next);
}
json Storage::media_scrub(){auto* device=dynamic_cast<ManagedBlockDevice*>(impl_->data.get());if(!device)throw DbError("MEDIA_OPTION","Scrub requires a managed member-file layout");IoScope scope("media integrity scrub");auto result=device->scrub();result["io"]=scope.snapshot();return result;}
json Storage::media_rebuild(std::size_t member){impl_->require_writable();auto backup_gate=impl_->backup_commit_gate(true);auto* device=dynamic_cast<ManagedBlockDevice*>(impl_->data.get());if(!device)throw DbError("MEDIA_OPTION","Rebuild requires a managed member-file layout");IoScope scope("explicit member rebuild");try{auto result=device->rebuild(member);result["io"]=scope.snapshot();result["restart_required"]=impl_->failed.load();return result;}catch(...){if(device->diagnostics().value("recovery_required",false)){impl_->failed=true;impl_->registry_cv.notify_all();}throw;}}
json Storage::media_maintenance(const std::string& directory,const json& request){
    if(!request.is_object())throw DbError("ARGUMENT","Media request must be an object");
    for(auto it=request.begin();it!=request.end();++it)if(it.key()!="action"&&it.key()!="member")throw DbError("ARGUMENT","Unknown media maintenance option");
    auto action=request.value("action",std::string("status"));if(action!="status"&&action!="scrub"&&action!="rebuild")throw DbError("ARGUMENT","Media action requires status, scrub or rebuild");
    auto path=fs::absolute(fs::u8path(directory)).lexically_normal();if(!fs::is_directory(path))throw DbError("ARGUMENT","Existing database directory required");NativeFile exclusive(path/"database.lock",false,false,true,false,true);IoScope scope("offline media maintenance");auto c=read_manifest(path/"control.json");
    if(c.value("version",0)==3)throw DbError("READ_ONLY_DATABASE","Legacy control3 requires offline migrate-v3 before media writes");if(c.value("format",std::string())!="EduDB"||c.value("version",0)!=4||c.value("page_format",0)!=3||c.value("minimum_reader_format",0)!=4)throw DbError("INCOMPATIBLE_FORMAT","Unsupported control4/page3 format");auto layout=c.value("data_layout",json{{"kind","single"}});if(layout.value("kind",std::string())!="managed")throw DbError("MEDIA_OPTION","Managed member-file database required");if(layout.value("format",std::string())!="edudb-managed-media-v1")throw DbError("INCOMPATIBLE_FORMAT","Unsupported media format");
    ManagedBlockDevice media(path.u8string(),c.at("uuid").get<std::string>(),ManagedMediaOptions{layout.at("layout").get<std::string>(),layout.at("members").get<std::size_t>()},false);json result={{"status","PASS"}};
    if(action=="scrub")result=media.scrub();else if(action=="rebuild"){if(!request.contains("member")||!request["member"].is_number_unsigned())throw DbError("ARGUMENT","Rebuild requires a nonnegative unsigned member number");result=media.rebuild(request["member"].get<std::size_t>());}else if(request.contains("member"))throw DbError("ARGUMENT","Member option only applies to rebuild");
    return {{"result",result},{"configuration",media.configuration()},{"diagnostics",media.diagnostics()},{"core_recovery","not performed; start database to replay database WAL"},{"io",scope.snapshot()}};
}
Storage::~Storage(){
    if(!impl_)return;
    if(impl_->logical_mode){
        try{
            // Native Storage users also get clean-close logical rollback.
            // Never call finish() while registry_mutex is held: it unregisters.
            std::vector<std::shared_ptr<Transaction>> pending;
            {std::lock_guard<std::mutex> guard(impl_->registry_mutex);for(const auto& item:impl_->active){auto tx=std::make_shared<Transaction>();tx->id=item.first;tx->state=item.second;pending.push_back(std::move(tx));}}
            for(const auto& tx:pending){std::lock_guard<std::recursive_mutex> guard(tx->state->mutex);if(tx->state->active&&tx->state->owner==impl_.get()){
                if(!impl_->failed){try{impl_->logical_abort(tx);}catch(...){impl_->logical_fail();}}
                impl_->finish(tx);tx->state->owner=nullptr;
            }}
        }catch(...){impl_->logical_fail();}
        // Persistence failure or allocation failure leaves durable loser
        // evidence for startup. Abandoning it is not a successful rollback.
        std::lock_guard<std::mutex> guard(impl_->registry_mutex);for(auto& item:impl_->active){item.second->active=false;item.second->owner=nullptr;item.second->pages.clear();}impl_->active.clear();return;
    }
    std::lock_guard<std::mutex> g(impl_->registry_mutex);for(auto& item:impl_->active){if(impl_->aries_mode){try{impl_->aries_manager->abort(item.first);}catch(...){impl_->failed=true;}}item.second->active=false;item.second->owner=nullptr;item.second->pages.clear();}impl_->active.clear();
}
std::shared_ptr<Transaction> Storage::begin(){return begin(TransactionOptions{});}
#define EDUDB_STORAGE_PROTOCOL_PUBLIC
#include "storage_transactions.inc"
#undef EDUDB_STORAGE_PROTOCOL_PUBLIC
#define EDUDB_STORAGE_BACKUP_PUBLIC
#include "storage_backup.inc"
#undef EDUDB_STORAGE_BACKUP_PUBLIC
void Storage::commit(const std::shared_ptr<Transaction>& tx){auto& s=*impl_;if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> tl(tx->state->mutex);try{s.require(tx);}catch(...){
        auto error=std::current_exception();
        if(tx->state->owner==&s&&tx->state->active){
            if(!s.aries_mode&&s.failed)++s.failed_pre_wal_commits;
            if(s.logical_mode&&!s.failed){try{s.logical_abort(tx);}catch(...){s.logical_fail();s.finish(tx);throw;}}
            else if(s.aries_mode&&!s.failed){try{s.aries_abort(tx);}catch(...){s.aries_note_failure();if(s.failed)s.finish(tx);throw;}}
            s.finish(tx);
        }
        std::rethrow_exception(error);
    }
    if(s.logical_mode){s.logical_commit(tx);return;}
    if(s.aries_mode){
        try{s.flush_descriptors(tx);}catch(...){auto error=std::current_exception();try{s.aries_abort(tx);}catch(...){s.aries_note_failure();if(s.failed)s.finish(tx);throw;}s.finish(tx);std::rethrow_exception(error);}
        auto backup_gate=s.backup_commit_gate(!tx->state->pages.empty()||!tx->state->aries_pages.empty()||(tx->state->protocol&&!tx->state->protocol->write_tables.empty())||(s.aries_mode&&s.aries_manager->transaction_metrics(tx->id).at("last_lsn").get<std::uint64_t>()!=0));
        s.aries_commit(tx);return;
    }
    std::optional<recovery::FencePermit> backup_gate;
    try{
        s.flush_descriptors(tx);
        backup_gate=s.backup_commit_gate(!tx->state->pages.empty()||!tx->state->aries_pages.empty()||(tx->state->protocol&&!tx->state->protocol->write_tables.empty()));
    }catch(...){
        // A preceding writer can fail while this commit waits for admission.
        // No page from this private transaction has reached WAL/publication.
        // Discard it and unregister; do not leave a failed commit active.
        if(s.failed){++s.failed_pre_wal_commits;s.finish(tx);}throw;
    }
    struct CommitterGuard{std::atomic<std::uint64_t>& count;explicit CommitterGuard(std::atomic<std::uint64_t>& c):count(c){++count;}~CommitterGuard(){--count;}} committer(s.wal_committers);
    std::lock_guard<std::mutex> wl(s.wal_mutex);
    // Recheck after lock acquisition: a preceding writer may have failed while we waited.
    if(s.failed){++s.failed_waiters;s.finish(tx);throw DbError("RECOVERY_REQUIRED","Earlier writer failed while waiting for WAL");}bool attempted=false;
    std::optional<Impl::ProtocolCommit> protocol_commit;
    try{protocol_commit.emplace(s.protocol_prepare_commit(tx));if(tx->state->pages.empty()){s.protocol_complete_commit(*protocol_commit);++s.commits;s.finish(tx);return;}
        std::optional<buffer::BufferPool::Publication> publication;try{publication.emplace(s.buffer->reserve_publication(5000));}catch(const buffer::Error& e){throw DbError(e.code,e.what());}
        std::vector<PageId> ids;for(const auto& p:tx->state->pages)ids.push_back(p.first);std::sort(ids.begin(),ids.end());attempted=true;s.append_record(1,s.next_lsn++,tx,{});std::uint32_t digest=0;for(auto id:ids){auto& p=tx->state->pages.at(id);auto lsn=s.next_lsn++;seal(p,lsn);digest=Impl::digest_page(digest,p);std::vector<unsigned char> bytes(PAGE_SIZE+8);put64(bytes.data(),id);std::copy(p.begin(),p.end(),bytes.begin()+8);s.append_record(2,lsn,tx,bytes);}std::vector<unsigned char> end(8);put32(end.data(),static_cast<std::uint32_t>(ids.size()));put32(end.data()+4,digest);auto lsn=s.next_lsn++;s.append_record(3,lsn,tx,end);crash_at("wal_before_sync");s.inject("wal_sync_once");s.wal->sync();++s.wal_forces;s.durable_lsn=lsn;crash_at("after_wal_sync");
        s.protocol_publish_versions(tx,*protocol_commit);
        for(auto id:ids)publication->publish(id,tx->state->pages.at(id));publication->finish();
        if(const char* p=std::getenv("EDUDB_CRASH_AT"))if(std::strcmp(p,"after_page_flush")==0)s.buffer->flush_one();
        s.protocol_complete_commit(*protocol_commit);++s.commits;s.finish(tx);
    }catch(...){if(attempted)s.failed=true;s.finish(tx);throw;}
}
void Storage::rollback(const std::shared_ptr<Transaction>& tx){if(!tx||!tx->state)return;auto& s=*impl_;std::lock_guard<std::recursive_mutex> l(tx->state->mutex);if(tx->state->owner==&s&&tx->state->active){++s.rollbacks;if(s.logical_mode){try{s.logical_abort(tx);}catch(...){s.logical_fail();s.finish(tx);throw;}}else if(s.aries_mode){try{s.aries_abort(tx);}catch(...){s.aries_note_failure();s.finish(tx);throw;}}s.finish(tx);}}
json Storage::catalog(const std::shared_ptr<Transaction>& tx){if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> l(tx->state->mutex);impl_->require(tx);std::unique_lock<std::recursive_mutex> structure;if(impl_->logical_mode)structure=std::unique_lock<std::recursive_mutex>(impl_->logical_mutex);return impl_->read_catalog(tx);}
json Storage::read_table(const std::shared_ptr<Transaction>& tx,const std::string& name,bool include_rows,bool include_physical){
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> l(tx->state->mutex);auto& s=*impl_;s.require(tx);
    std::unique_lock<std::recursive_mutex> structure;if(s.logical_mode){structure=std::unique_lock<std::recursive_mutex>(s.logical_mutex);if(!tx->state->logical_staging&&tx->state->logical_kind!="physical")s.logical_clear(tx);}
    auto pair=s.descriptor(tx,name);auto& d=pair.second;json t=d.at("meta");
    t["record_layout"]=s.cluster_record_layout(d);
    t["row_count"]=d.at("row_count");if(s.logical_mode){t["row_count_scope"]="physical mini-transaction metadata; may include outer-uncommitted insert/delete unless caller holds a coarse guard";t["logical_visibility"]="SQL rows require key/range or coarse locks; physical metadata is not a committed-row oracle";}t["heap_pages"]=d.at("heap").size();t["table_id"]=pair.first;
    if(s.clustered(d)){t["heap_pages"]=s.cluster_organization_stats(tx,d).at("pages");t["heap_pages_shared"]=true;}
    for(const char* field:{"data_version","schema_version","physical_version"})t[field]=s.ch10_version(d,field);
    t["statistics"]=s.cluster_statistics(tx,d);t["statistics_versions"]=d.value("statistics_versions",json::object());
    t["physical_stats"]=include_physical?s.ch10_physical(tx,pair.first,d):json{{"measurement_scope","not requested"}};t["index_stats"]=d.at("index_roots");
    for(auto& ix:t["index_stats"]){ix["entries"]=ix.value("count",std::uint64_t(0));ix["root_page"]=ix.value("root",std::uint64_t(0));ix["column"]=ix.value("column",std::string());}
    if(include_rows){t["rows"]=json::array();t["row_ids"]=json::array();auto c=scan(tx,name);StoredRow r;while(c->next(r)){t["rows"].push_back(r.values);t["row_ids"].push_back(r.id.logical_id);}}
    return t;
}
void Storage::set_statistics(const std::shared_ptr<Transaction>& tx,const std::string& name,const json& statistics){impl_->require_writable();
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");
    std::lock_guard<std::recursive_mutex> l(tx->state->mutex);auto& s=*impl_;s.require(tx);Impl::BackupWriteOperation operation(*impl_,tx);
    if(s.logical_mode)throw DbError("LOGICAL_UNSUPPORTED","ANALYZE statistics are unavailable in logical experiment mode");
    if(!statistics.is_object())throw DbError("INVALID_STATISTICS","Statistics must be a JSON object");
    if(statistics.dump().size()>16u*1024u*1024u)throw DbError("STATISTICS_LIMIT_EXCEEDED","Statistics exceed 16 MiB");
    auto pair=s.descriptor(tx,name);auto& d=pair.second;
    d["statistics"]=statistics;d["statistics_versions"]=s.cluster_stats_versions(tx,d);s.save_desc(tx,pair.first,d);
}
std::pair<std::string,std::string> Storage::query_workspace()const{
    return {fs::absolute(impl_->directory).lexically_normal().u8string(),impl_->db_uuid};
}
json Storage::configure_buffer(const json& request){return impl_->configure_buffer(request);}
RowId Storage::insert_row(const std::shared_ptr<Transaction>& tx,const std::string& name,const json& row){impl_->require_writable();if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> l(tx->state->mutex);impl_->require(tx);Impl::BackupWriteOperation operation(*impl_,tx);if(impl_->logical_mode)return impl_->logical_mutate(tx,name,{},row,"INSERT");auto id=impl_->v3_insert(tx,name,row);if(tx->state->changefeed.enabled(name))impl_->feed_emit(tx,name,"INSERT",id.logical_id,nullptr,impl_->feed_row(tx,name,id.logical_id));return id;}
RowId Storage::update_row(const std::shared_ptr<Transaction>& tx,const std::string& name,const RowId& id,const json& row){impl_->require_writable();if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> l(tx->state->mutex);impl_->require(tx);Impl::BackupWriteOperation operation(*impl_,tx);if(impl_->logical_mode)return impl_->logical_mutate(tx,name,id,row,"UPDATE");json before;if(tx->state->changefeed.enabled(name))before=impl_->feed_row(tx,name,id.logical_id);auto after=impl_->v3_update(tx,name,id,row);if(tx->state->changefeed.enabled(name))impl_->feed_emit(tx,name,"UPDATE",after.logical_id,before,impl_->feed_row(tx,name,after.logical_id));return after;}
void Storage::erase_row(const std::shared_ptr<Transaction>& tx,const std::string& name,const RowId& id){impl_->require_writable();if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> l(tx->state->mutex);impl_->require(tx);Impl::BackupWriteOperation operation(*impl_,tx);if(impl_->logical_mode){impl_->logical_mutate(tx,name,id,nullptr,"DELETE");return;}json before;if(tx->state->changefeed.enabled(name))before=impl_->feed_row(tx,name,id.logical_id);impl_->v3_erase(tx,name,id);if(tx->state->changefeed.enabled(name))impl_->feed_emit(tx,name,"DELETE",id.logical_id,before,nullptr);}
void Storage::drop_table(const std::shared_ptr<Transaction>& tx,const std::string& name){impl_->require_writable();
    if(!tx||!tx->state)throw DbError("NO_TRANSACTION","No transaction");std::lock_guard<std::recursive_mutex> lock(tx->state->mutex);auto& s=*impl_;s.require(tx);Impl::BackupWriteOperation operation(*impl_,tx);s.logical_prepare_ddl(tx);auto cat=s.read_catalog(tx);if(!cat.contains(name))throw DbError("TABLE_NOT_FOUND","Table not found");auto pair=s.descriptor(tx,name);auto owner=pair.first;auto& d=pair.second;
    {access::Index map(s.pager(tx,owner),d["row_map"]);for(const auto& e:map.all())s.retire_objects(tx,owner,s.stored_at(tx,owner,d,e.id));}
    for(auto& ix:d["index_roots"])if(!ix.value("organization_alias",false)){access::Index index(s.pager(tx,owner),ix);index.clear();}
    s.cluster_remove_member(tx,owner,d);
    {access::Index map(s.pager(tx,owner),d["row_map"]);map.clear();}if(s.org_method(d)!="HEAP"){access::Index index(s.pager(tx,owner,true),d["organization_index"]);index.clear();}else for(const auto& id:d["heap"])s.retire(tx,id.get<PageId>());
    std::vector<PageId> discard;for(auto id:tx->state->new_pages)if(id!=owner&&get64(s.read_page(tx,id).data()+40)==owner)discard.push_back(id);for(auto id:discard)s.retire(tx,id);s.retire(tx,owner);tx->state->descriptors.erase(owner);tx->state->dirty_descriptors.erase(owner);tx->state->descriptor_names.erase(name);cat.erase(name);s.save_catalog_tables(tx,cat);s.feed_emit(tx,name,"RESET",0,nullptr,nullptr);
}
json Storage::transaction_stats(const std::shared_ptr<Transaction>& tx){if(!tx||!tx->state||tx->state->owner!=impl_.get())throw DbError("NO_TRANSACTION","Unknown transaction");std::lock_guard<std::recursive_mutex> l(tx->state->mutex);if(impl_->logical_mode)return impl_->logical_stats(tx);if(impl_->aries_mode)return impl_->aries_transaction_stats(tx);auto n=tx->state->active?tx->state->pages.size():tx->state->last_pages;auto types=tx->state->active?tx->state->private_page_types:tx->state->last_page_types;return {{"scope","transaction"},{"active",tx->state->active},{"private_pages",n},{"private_dirty_pages",n},{"dirty_pages",n},{"private_bytes",n*PAGE_SIZE},{"private_heap_pages",types[TYPE_HEAP]},{"private_metadata_pages",types[TYPE_SUPER]+types[TYPE_CATALOG]+types[TYPE_DESCRIPTOR]},{"private_index_pages",types[TYPE_BTREE]+types[TYPE_ACCESS]},{"private_retired_pages",types[TYPE_RETIRED]},{"pending_descriptor_count",tx->state->dirty_descriptors.size()},{"heap_page_reads",tx->state->heap_reads},{"index_page_reads",tx->state->index_reads},{"wal_bytes",tx->state->wal_bytes},{"row_fetches",tx->state->rows_fetched},{"cover_hits",tx->state->cover_hits},{"bitmap_words",tx->state->bitmap_words},{"savepoint_bytes",tx->state->protocol_savepoint_bytes},{"savepoint_count",tx->state->protocol_savepoints.size()},{"savepoint_limit_bytes",impl_->protocol_savepoint_limit},{"protocol",tx->state->protocol?transaction_protocol_name(tx->state->protocol->options.protocol):std::string("uninitialized")},{"read_csn",tx->state->protocol?tx->state->protocol->read_csn:0}};}
#include "storage_v3_api.inc"
json Storage::wal_records(std::size_t limit){
    if(limit>65536)throw DbError("ARGUMENT","WAL inspection limit must be 0..65536");
    auto& s=*impl_;
    if(!s.aries_mode)throw DbError("WAL_INSPECTION_MODE","SHOW WAL RECORDS requires ARIES_PAGE mode; SHOW WAL is available in both modes");
    if(s.logical_mode){std::lock_guard<std::recursive_mutex> structure(s.logical_mutex);auto out=s.aries_manager->wal_records(std::min<std::size_t>(limit,4096));out["mode"]="ARIES_LOGICAL";out["logical_records"]=s.logical_journal->wal_records(limit);out["lsn_scopes"]="Independent physical and logical sequences; integer LSN values must not be compared across them";return out;}
    return s.aries_manager->wal_records(limit);
}
#include "storage_cluster_api.inc"
json Storage::diagnostics(){
    auto& s=*impl_;if(s.aries_mode){auto out=s.aries_diagnostics();out["format_version"]=s.legacy_read_only?3:4;out["page_format"]=3;out["read_only"]=s.legacy_read_only;if(s.legacy_snapshot){out["legacy_source"]=s.legacy_snapshot->source().u8string();out["recovery_scope"]="owned snapshot copy only; source locked and unchanged";}return out;}std::size_t count,dirty=0,active=0;json pages=json::array(),nodes=json::array();{std::lock_guard<std::mutex> l(s.registry_mutex);active=s.active.size();}
    // Maintenance replaces buffer while holding WAL; protect its object lifetime too.
    std::lock_guard<std::mutex> l(s.wal_mutex);
    const auto bs=s.buffer->statistics();count=bs.resident_frames;dirty=bs.dirty_frames;
    for(const auto& f:s.buffer->snapshot(64)){const auto& p=f.image;pages.push_back({{"id",f.id},{"type",get16(p.data()+6)},{"lsn",get64(p.data()+16)},{"dirty",f.dirty},{"pins",f.pins},{"prefetched",f.prefetched},{"reserved",f.reserved},{"slots",get16(p.data()+32)}});if(get16(p.data()+6)==TYPE_BTREE){auto n=parse_json(slot(p,0));n["page_id"]=f.id;n["owner"]=get64(p.data()+40);nodes.push_back(n);}}

    json media={{"configuration",{{"layout","SINGLE"}}},{"data_layout",s.data_layout}};if(auto* device=dynamic_cast<ManagedBlockDevice*>(s.data.get())){media["configuration"]=device->configuration();media["diagnostics"]=device->diagnostics();}
    return s.backup_diagnostics({{"media",media},{"transactions",s.protocol_diagnostics()},{"stale_query_files_removed",s.stale_query_files_removed},{"unrecognized_query_files_retained",s.unrecognized_query_files_retained},{"stale_sort_files_removed",s.stale_sort_files_removed},{"unrecognized_sort_files_retained",s.unrecognized_sort_files_retained},{"io",io_snapshot(s.directory)},{"format_version",s.legacy_read_only?3:4},{"page_format",3},{"read_only",s.legacy_read_only},{"legacy_source",s.legacy_snapshot?s.legacy_snapshot->source().u8string():std::string()},{"recovery_scope",s.legacy_read_only?"owned snapshot copy only; source locked and unchanged":"database directory"},{"page_size",PAGE_SIZE},{"buffer_pool_frames",s.frame_limit},{"resident_frames",count},{"dirty_frames",dirty},{"private_page_limit_bytes",s.private_limit},{"active_transactions",active},{"next_page_id",s.next_page.load()},{"wal_generation",s.generation},{"wal_bytes",s.wal->size()},{"durable_lsn",s.durable_lsn.load()},{"checkpoint_lsn",s.checkpoint_lsn},{"buffer_hits",bs.demand_hits},{"buffer_policy",buffer::policy_name(bs.config.policy)},{"buffer_live_guards",bs.live_guards},{"buffer_pinned_frames",bs.pinned_frames},{"buffer_guard_acquires",bs.guard_acquires},{"buffer_guard_releases",bs.guard_releases},{"buffer_pin_high_water",bs.pin_high_water},{"buffer_max_live_guards",bs.max_live_guards},{"buffer_busy_reads",bs.busy_reads},{"buffer_clock_scans",bs.clock_scans},{"buffer_clock_second_chances",bs.clock_second_chances},{"buffer_toss_immediate",bs.toss_immediate},{"buffer_toss_dirty_deferred",bs.toss_dirty_deferred},{"buffer_publication_reservations",bs.publication_reservations},{"buffer_publication_busy",bs.publication_busy},{"buffer_publication_pages",bs.publication_pages},{"buffer_publication_flushes",bs.publication_flushes},{"buffer_publication_active",bs.publication_active},{"buffer_prefetch_requested",bs.prefetch_requested},{"buffer_prefetch_loaded",bs.prefetch_loaded},{"buffer_prefetch_already_resident",bs.prefetch_already_resident},{"buffer_prefetch_useful",bs.prefetch_useful},{"buffer_prefetch_wasted",bs.prefetch_wasted},{"buffer_prefetch_skipped_busy",bs.prefetch_skipped_busy},{"buffer_read_ahead_pages",bs.config.read_ahead_pages},{"buffer_wait_ms",bs.config.wait_ms},{"buffer_io_failures",bs.io_failures},{"buffer_policy_changes",bs.policy_changes},{"buffer_misses",bs.demand_misses},{"evictions",bs.evictions},{"page_writes",s.writes.load()},{"commits",s.commits.load()},{"rollbacks",s.rollbacks.load()},{"wal_forces",s.wal_forces.load()},{"recovered_pages",s.recovered.load()},{"wal_tail_truncated",s.truncated_tail},{"index_page_reads",s.index_reads.load()},{"heap_page_reads",s.heap_reads.load()},{"splits",s.splits.load()},{"merges",s.merges.load()},{"redistributions",s.redistributions.load()},{"root_shrinks",s.root_shrinks.load()},{"retired_pages",s.retired.load()},{"test_io_faults",s.io_faults.load()},{"failed_waiting_writers",s.failed_waiters.load()},{"failed_pre_wal_commits",s.failed_pre_wal_commits.load()},{"recovery_required",s.failed.load()},{"allocator","monotonic; retired whole pages are not reused"},{"buffer",{{"frames",s.frame_limit},{"resident",count},{"dirty",dirty},{"hits",bs.demand_hits},{"misses",bs.demand_misses},{"evictions",bs.evictions}}},{"wal",{{"generation",s.generation},{"bytes",s.wal->size()},{"durable_lsn",s.durable_lsn.load()},{"checkpoint_lsn",s.checkpoint_lsn},{"forces",s.wal_forces.load()}}},{"pages",pages},{"index_nodes",nodes}});
}
json Storage::checkpoint(int timeout_ms){impl_->require_writable();auto& s=*impl_;if(s.aries_mode){auto backup_gate=s.backup_commit_gate(true);return s.aries_checkpoint();}std::unique_lock<std::mutex> r(s.registry_mutex);if(s.failed)throw DbError("RECOVERY_REQUIRED","Storage requires recovery");if(s.checkpoint_requested)throw DbError("CHECKPOINT_BUSY","Another checkpoint running");s.checkpoint_requested=true;
    if(!s.registry_cv.wait_for(r,std::chrono::milliseconds(std::max(0,timeout_ms)),[&]{return s.active.empty();})){s.checkpoint_requested=false;r.unlock();s.registry_cv.notify_all();throw DbError("CHECKPOINT_BUSY","Active transactions prevented checkpoint");}
    if(s.failed){s.checkpoint_requested=false;r.unlock();s.registry_cv.notify_all();throw DbError("RECOVERY_REQUIRED","Writer failed while checkpoint waited");}r.unlock();auto release=[&]{std::lock_guard<std::mutex> g(s.registry_mutex);s.checkpoint_requested=false;s.registry_cv.notify_all();};
    std::optional<recovery::FencePermit> backup_gate;try{backup_gate=s.backup_commit_gate(true);}catch(...){release();throw;}
    bool mutation_started=false;
    try{IoOwnerScope io_owner(s.directory);std::lock_guard<std::mutex> w(s.wal_mutex);s.backup_archive_before_checkpoint();mutation_started=true;s.buffer->flush_all();s.data->sync();crash_at("checkpoint_after_data");auto gen=s.generation+1;while(fs::exists(s.wal_path(gen)))++gen;auto next=std::make_unique<NativeFile>(s.wal_path(gen),true,true);auto h=s.wal_header(gen,s.durable_lsn);next->write(0,h.data(),h.size());next->sync();sync_directory(s.directory);crash_at("checkpoint_before_manifest");auto old=s.generation;atomic_manifest(s.directory,s.control(gen,s.durable_lsn));crash_at("checkpoint_after_manifest");s.wal=std::move(next);s.generation=gen;s.checkpoint_lsn=s.durable_lsn;std::error_code ec;fs::remove(s.wal_path(old),ec);sync_directory(s.directory);release();return {{"status","PASS"},{"wal_generation",gen},{"checkpoint_lsn",s.checkpoint_lsn},{"active_transactions",0}};}catch(const DbError& e){if(mutation_started||e.code=="ARCHIVE_WAL"||e.code=="IO_ERROR")s.failed=true;release();throw;}catch(...){if(mutation_started)s.failed=true;release();throw;}
}
#include "storage_migrate.inc"
} // namespace edudb
