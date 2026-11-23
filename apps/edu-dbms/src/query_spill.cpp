#include "query_spill.hpp"
#include "io.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <sstream>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace edudb { namespace {
namespace fs=std::filesystem;
constexpr std::size_t header_bytes=80, frame_bytes=24;
constexpr std::uint32_t frame_magic=0x51524631;
void p32(unsigned char* p,std::uint32_t n){for(unsigned i=0;i<4;++i)p[i]=static_cast<unsigned char>(n>>(8*i));}
void p64(unsigned char* p,std::uint64_t n){for(unsigned i=0;i<8;++i)p[i]=static_cast<unsigned char>(n>>(8*i));}
std::uint32_t g32(const unsigned char* p){std::uint32_t n=0;for(int i=3;i>=0;--i)n=(n<<8)|p[i];return n;}
std::uint64_t g64(const unsigned char* p){std::uint64_t n=0;for(int i=7;i>=0;--i)n=(n<<8)|p[i];return n;}
std::uint32_t checksum(const unsigned char* p,std::size_t n){
    static const auto table=[] {std::array<std::uint32_t,256> t{};for(std::uint32_t i=0;i<256;++i){auto v=i;for(int j=0;j<8;++j)v=(v&1)?(v>>1)^0xedb88320u:v>>1;t[i]=v;}return t;}();
    std::uint32_t c=0xffffffffu;for(std::size_t i=0;i<n;++i)c=table[(c^p[i])&255]^(c>>8);return c^0xffffffffu;
}
[[noreturn]] void corrupt(const char* message){throw DbError("QUERY_SPILL_CORRUPT",message);}
std::string nonce(){std::random_device r;std::ostringstream s;s<<std::hex;for(int i=0;i<8;++i){auto n=static_cast<std::uint32_t>(r());s.width(8);s.fill('0');s<<n;}return s.str();}

// Every counted transfer is a native file call; buffered iostream operations
// must not be presented as operating-system I/O measurements.
class TempFile {
public:
    TempFile(fs::path path,bool create):path_(std::move(path)){
#ifdef _WIN32
        handle_=CreateFileW(path_.c_str(),GENERIC_READ|(create?GENERIC_WRITE:0),FILE_SHARE_READ,nullptr,create?CREATE_NEW:OPEN_EXISTING,FILE_ATTRIBUTE_NORMAL,nullptr);
        if(handle_==INVALID_HANDLE_VALUE)throw DbError("IO_ERROR","Cannot open owned query run");
#else
        fd_=::open(path_.c_str(),create?(O_RDWR|O_CREAT|O_EXCL):O_RDONLY,0600);
        if(fd_<0)throw DbError("IO_ERROR","Cannot open owned query run");
#endif
    }
    ~TempFile(){
#ifdef _WIN32
        if(handle_!=INVALID_HANDLE_VALUE)CloseHandle(handle_);
#else
        if(fd_>=0)::close(fd_);
#endif
    }
    std::uint64_t size(){
#ifdef _WIN32
        LARGE_INTEGER value{};if(!GetFileSizeEx(handle_,&value))throw DbError("IO_ERROR","Cannot size query run");return static_cast<std::uint64_t>(value.QuadPart);
#else
        struct stat value{};if(::fstat(fd_,&value))throw DbError("IO_ERROR","Cannot size query run");return static_cast<std::uint64_t>(value.st_size);
#endif
    }
    void transfer(bool writing,std::uint64_t offset,unsigned char* data,std::size_t length){
#ifdef _WIN32
        LARGE_INTEGER position{};position.QuadPart=static_cast<LONGLONG>(offset);
        if(!SetFilePointerEx(handle_,position,nullptr,FILE_BEGIN))throw DbError("IO_ERROR","Cannot seek query run");
#endif
        while(length){auto started=std::chrono::steady_clock::now();std::size_t count=0;bool bad=false;
#ifdef _WIN32
            DWORD amount=0;auto chunk=static_cast<DWORD>(std::min<std::size_t>(length,1u<<30));
            auto ok=writing?WriteFile(handle_,data,chunk,&amount,nullptr):ReadFile(handle_,data,chunk,&amount,nullptr);
            count=amount;bad=!ok||!amount;
#else
            auto result=writing ? ::pwrite(fd_,data,length,static_cast<off_t>(offset)) : ::pread(fd_,data,length,static_cast<off_t>(offset));
            auto saved=errno;count=result>0?static_cast<std::size_t>(result):0;bad=result<=0;
#endif
            io_record(path_,writing?"write":"read",count,io_elapsed(started),bad,offset);
#ifndef _WIN32
            if(result<0&&saved==EINTR)continue;
#endif
            if(bad)throw DbError("IO_ERROR",writing?"Query run write failed":"Query run read failed or truncated");
            data+=count;length-=count;offset+=count;
        }
    }
    void read(std::uint64_t offset,unsigned char* data,std::size_t length){transfer(false,offset,data,length);}
    void write(std::uint64_t offset,const unsigned char* data,std::size_t length){transfer(true,offset,const_cast<unsigned char*>(data),length);}
private:
    fs::path path_;
#ifdef _WIN32
    HANDLE handle_=INVALID_HANDLE_VALUE;
#else
    int fd_=-1;
#endif
};
} // namespace

struct QuerySpillStore::Impl {
    struct Entry {Run run;fs::path path;bool writing=true,sealed=false;std::size_t readers=0;};
    fs::path directory;std::string identity,prefix;std::size_t quota,maximum;
    mutable std::mutex mutex;std::map<std::uint64_t,std::shared_ptr<Entry>> entries;
    std::uint64_t serial=0,live=0,peak=0,created=0,written=0,read=0,cleanup_failures=0;
    Impl(const std::string& path,const std::string& id,std::size_t limit,std::size_t frame)
        :directory(fs::absolute(fs::u8path(path)).lexically_normal()),identity(id),prefix("edudb-query-"+nonce()+"-"),quota(limit),maximum(frame){
        if(!fs::is_directory(directory)||identity.size()!=32||identity.find_first_not_of("0123456789abcdefABCDEF")!=std::string::npos)
            throw DbError("INVALID_CONFIG","Query spill requires an existing database directory and its 32-character identity");
        if(quota<header_bytes||maximum<16||maximum>64u*1024u*1024u)throw DbError("INVALID_CONFIG","Invalid query spill size limits");
    }
    ~Impl(){for(auto& pair:entries){std::error_code error;fs::remove(pair.second->path,error);}}
    void reserve(std::uint64_t bytes){if(bytes>quota-live)throw DbError("QUERY_LIMIT_EXCEEDED","Query temporary-file quota exceeded");live+=bytes;peak=std::max(peak,live);}
    std::array<unsigned char,header_bytes> header(const Entry& e,bool sealed)const{
        std::array<unsigned char,header_bytes> h{};std::memcpy(h.data(),"EDUQSP1",7);std::memcpy(h.data()+8,identity.data(),32);
        p64(h.data()+40,e.run.id);p64(h.data()+48,e.run.rows);p64(h.data()+56,e.run.bytes);p32(h.data()+64,static_cast<std::uint32_t>(maximum));p32(h.data()+68,sealed?1:0);p32(h.data()+76,checksum(h.data(),h.size()));return h;
    }
    std::shared_ptr<Entry> find(const Run& run){auto it=entries.find(run.id);if(it==entries.end()||!it->second->sealed||it->second->run.rows!=run.rows||it->second->run.bytes!=run.bytes)throw DbError("INVALID_QUERY_RUN","Unknown or unsealed query run");return it->second;}
    void remove(const std::shared_ptr<Entry>& e){
        if(e->path.parent_path()!=directory||e->path.filename().u8string().rfind(prefix,0)!=0)throw DbError("IO_ERROR","Query run ownership mismatch");
        std::error_code error;fs::remove(e->path,error);if(error)throw DbError("IO_ERROR","Cannot remove owned query run");
        live-=e->run.bytes;entries.erase(e->run.id);
    }
};

struct QuerySpillStore::Writer::Impl {
    std::shared_ptr<QuerySpillStore::Impl> owner;std::shared_ptr<QuerySpillStore::Impl::Entry> entry;std::unique_ptr<TempFile> file;bool finished=false,failed=false;
    ~Impl(){file.reset();if(!finished&&owner&&entry){std::lock_guard<std::mutex> lock(owner->mutex);entry->writing=false;try{owner->remove(entry);}catch(...){++owner->cleanup_failures;}}}
};
struct QuerySpillStore::Reader::Impl {
    std::shared_ptr<QuerySpillStore::Impl> owner;std::shared_ptr<QuerySpillStore::Impl::Entry> entry;std::unique_ptr<TempFile> file;std::uint64_t at=header_bytes,rows=0;
    ~Impl(){file.reset();if(owner&&entry){std::lock_guard<std::mutex> lock(owner->mutex);--entry->readers;}}
};

QuerySpillStore::QuerySpillStore(const std::string& directory,const std::string& identity,std::size_t limit,std::size_t frame)
    :impl_(std::make_shared<Impl>(directory,identity,limit,frame)){}
QuerySpillStore::~QuerySpillStore()=default;
QuerySpillStore::Writer::Writer(std::unique_ptr<Writer::Impl> impl):impl_(std::move(impl)){}
QuerySpillStore::Writer::~Writer()=default;
QuerySpillStore::Reader::Reader(std::unique_ptr<Reader::Impl> impl):impl_(std::move(impl)){}
QuerySpillStore::Reader::~Reader()=default;

std::unique_ptr<QuerySpillStore::Writer> QuerySpillStore::create(){
    auto value=std::make_unique<Writer::Impl>();value->owner=impl_;
    std::lock_guard<std::mutex> lock(impl_->mutex);auto e=std::make_shared<Impl::Entry>();e->run.id=++impl_->serial;e->path=impl_->directory/(impl_->prefix+std::to_string(e->run.id)+".tmp");
    impl_->reserve(header_bytes);e->run.bytes=header_bytes;impl_->entries[e->run.id]=e;value->entry=e;
    // On creation failure, destruction after releasing the mutex removes only
    // this run; create-new never replaces an existing file.
    try{value->file=std::make_unique<TempFile>(e->path,true);auto h=impl_->header(*e,false);value->file->write(0,h.data(),h.size());impl_->written+=h.size();++impl_->created;}
    catch(...){bool created=bool(value->file);value->owner.reset();value->file.reset();if(created){std::error_code error;fs::remove(e->path,error);}impl_->live-=header_bytes;impl_->entries.erase(e->run.id);throw;}
    return std::unique_ptr<Writer>(new Writer(std::move(value)));
}
void QuerySpillStore::Writer::append(const json& row){
    if(impl_->finished||impl_->failed)throw DbError("INVALID_QUERY_RUN","Query run is sealed or failed");
    auto text=row.dump();auto& owner=*impl_->owner;std::lock_guard<std::mutex> lock(owner.mutex);
    if(text.empty()||text.size()>owner.maximum)throw DbError("QUERY_ROW_TOO_LARGE","Query spill row exceeds framed-record limit");
    std::array<unsigned char,frame_bytes> frame{};p32(frame.data(),frame_magic);p32(frame.data()+4,static_cast<std::uint32_t>(text.size()));p64(frame.data()+8,impl_->entry->run.rows);
    p32(frame.data()+16,checksum(reinterpret_cast<const unsigned char*>(text.data()),text.size()));p32(frame.data()+20,checksum(frame.data(),frame.size()));
    auto bytes=frame.size()+text.size();owner.reserve(bytes);auto offset=impl_->entry->run.bytes;impl_->entry->run.bytes+=bytes;
    try{impl_->file->write(offset,frame.data(),frame.size());impl_->file->write(offset+frame.size(),reinterpret_cast<const unsigned char*>(text.data()),text.size());}catch(...){impl_->failed=true;throw;}
    owner.written+=bytes;++impl_->entry->run.rows;
}
QuerySpillStore::Run QuerySpillStore::Writer::finish(){
    if(impl_->finished||impl_->failed)throw DbError("INVALID_QUERY_RUN","Query run is sealed or failed");
    auto& owner=*impl_->owner;std::lock_guard<std::mutex> lock(owner.mutex);auto h=owner.header(*impl_->entry,true);try{impl_->file->write(0,h.data(),h.size());}catch(...){impl_->failed=true;throw;}owner.written+=h.size();
    impl_->file.reset();impl_->entry->writing=false;impl_->entry->sealed=true;impl_->finished=true;return impl_->entry->run;
}
std::unique_ptr<QuerySpillStore::Reader> QuerySpillStore::open(const Run& run){
    auto value=std::make_unique<Reader::Impl>();std::lock_guard<std::mutex> lock(impl_->mutex);auto e=impl_->find(run);
    auto file=std::make_unique<TempFile>(e->path,false);if(file->size()!=run.bytes||run.bytes<header_bytes)corrupt("Query run size mismatch");
    std::array<unsigned char,header_bytes> h{};file->read(0,h.data(),h.size());impl_->read+=h.size();auto sum=g32(h.data()+76);p32(h.data()+76,0);
    if(std::memcmp(h.data(),"EDUQSP1",7)||h[7]||std::memcmp(h.data()+8,impl_->identity.data(),32)||g64(h.data()+40)!=run.id||g64(h.data()+48)!=run.rows||g64(h.data()+56)!=run.bytes||g32(h.data()+64)!=impl_->maximum||g32(h.data()+68)!=1||g32(h.data()+72)!=0||sum!=checksum(h.data(),h.size()))corrupt("Query run header/identity/checksum mismatch");
    value->owner=impl_;value->entry=e;value->file=std::move(file);++e->readers;return std::unique_ptr<Reader>(new Reader(std::move(value)));
}
bool QuerySpillStore::Reader::next(json& row){
    auto& value=*impl_;auto& owner=*value.owner;
    if(value.at==value.entry->run.bytes){if(value.rows!=value.entry->run.rows)corrupt("Query run row count mismatch");return false;}
    if(value.rows>=value.entry->run.rows||value.at>value.entry->run.bytes||value.entry->run.bytes-value.at<frame_bytes)corrupt("Truncated query run frame");
    std::array<unsigned char,frame_bytes> f{};value.file->read(value.at,f.data(),f.size());{std::lock_guard<std::mutex> lock(owner.mutex);owner.read+=f.size();}
    auto sum=g32(f.data()+20);p32(f.data()+20,0);if(g32(f.data())!=frame_magic||g64(f.data()+8)!=value.rows||sum!=checksum(f.data(),f.size()))corrupt("Query frame header checksum/sequence mismatch");
    auto length=g32(f.data()+4);if(!length||length>owner.maximum||length>value.entry->run.bytes-value.at-frame_bytes)corrupt("Invalid query frame length");
    std::string text(length,'\0');value.file->read(value.at+frame_bytes,reinterpret_cast<unsigned char*>(text.data()),text.size());{std::lock_guard<std::mutex> lock(owner.mutex);owner.read+=text.size();}
    if(g32(f.data()+16)!=checksum(reinterpret_cast<const unsigned char*>(text.data()),text.size()))corrupt("Query frame payload checksum mismatch");
    try{row=json::parse(text);}catch(...){corrupt("Invalid query frame JSON");}
    value.at+=frame_bytes+length;++value.rows;return true;
}
void QuerySpillStore::erase(const Run& run){std::lock_guard<std::mutex> lock(impl_->mutex);auto e=impl_->find(run);if(e->writing||e->readers)throw DbError("QUERY_RUN_BUSY","Query run still has an active reader or writer");impl_->remove(e);}
json QuerySpillStore::diagnostics()const{std::lock_guard<std::mutex> lock(impl_->mutex);return {{"temporary_runs_created",impl_->created},{"live_runs",impl_->entries.size()},{"live_bytes",impl_->live},{"peak_live_bytes",impl_->peak},{"byte_limit",impl_->quota},{"bytes_written",impl_->written},{"bytes_read",impl_->read},{"frame_crc",true},{"header_crc",true},{"cleanup_failures",impl_->cleanup_failures},{"durability","uncommitted temporary rows; no fsync required"}};}
} // namespace edudb
