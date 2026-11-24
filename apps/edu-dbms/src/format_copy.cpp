#include "portable_sha256.hpp"
#include "format_copy.hpp"
#include "edudb.hpp"
#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
namespace edudb::format_copy {
namespace {
[[noreturn]]void fail(const std::string& c,const std::string& m){throw DbError(c,m);}
using Sha256 = PortableSha256;
std::uint32_t crc(const std::string& text){std::uint32_t c=0xffffffffu;for(unsigned char b:text){c^=b;for(int n=0;n<8;++n)c=(c>>1)^((c&1)?0xedb88320u:0);}return c^0xffffffffu;}
std::string nonce(){std::random_device random;std::ostringstream s;s<<std::hex<<std::setfill('0');for(unsigned i=0;i<4;++i)s<<std::setw(8)<<static_cast<std::uint32_t>(random());return s.str();}
void path_limit(const fs::path& path){
#ifdef _WIN32
    if(fs::absolute(path).native().size()>245)fail("MIGRATION_PATH_LIMIT","Resolved file path exceeds this Windows build's245 UTF16-unit safe limit");
#else
    if(fs::absolute(path).native().size()>4000)fail("MIGRATION_PATH_LIMIT","Resolved file path is too long");
#endif
}
bool reparse(const fs::path& p){
#ifdef _WIN32
    auto attributes=GetFileAttributesW(p.c_str());if(attributes==INVALID_FILE_ATTRIBUTES)fail("MIGRATION_PATH","Cannot inspect file attributes");return (attributes&FILE_ATTRIBUTE_REPARSE_POINT)!=0;
#else
    return fs::is_symlink(fs::symlink_status(p));
#endif
}
class DirectoryHandle {
public:fs::path resolved;
#ifdef _WIN32
    HANDLE handle=INVALID_HANDLE_VALUE;
#else
    int handle=-1;
#endif
    explicit DirectoryHandle(const fs::path& path){path_limit(path);
#ifdef _WIN32
        handle=CreateFileW(path.c_str(),FILE_READ_ATTRIBUTES,FILE_SHARE_READ|FILE_SHARE_WRITE,nullptr,OPEN_EXISTING,FILE_FLAG_BACKUP_SEMANTICS,nullptr);if(handle==INVALID_HANDLE_VALUE)fail("MIGRATION_PATH","Cannot open existing directory handle");
        std::vector<wchar_t> text(32768);auto n=GetFinalPathNameByHandleW(handle,text.data(),static_cast<DWORD>(text.size()),FILE_NAME_NORMALIZED|VOLUME_NAME_DOS);if(!n||n>=text.size()){CloseHandle(handle);handle=INVALID_HANDLE_VALUE;fail("MIGRATION_PATH","Cannot resolve actual directory target");}std::wstring final(text.data(),n);if(final.rfind(L"\\\\?\\UNC\\",0)==0){CloseHandle(handle);handle=INVALID_HANDLE_VALUE;fail("MIGRATION_PATH","Network paths are outside the atomic local-directory migration contract");}if(final.rfind(L"\\\\?\\",0)==0)final.erase(0,4);resolved=fs::path(final);BY_HANDLE_FILE_INFORMATION info{};if(!GetFileInformationByHandle(handle,&info)||!(info.dwFileAttributes&FILE_ATTRIBUTE_DIRECTORY)){CloseHandle(handle);handle=INVALID_HANDLE_VALUE;fail("MIGRATION_PATH","A real directory is required");}
#else
        handle=::open(path.c_str(),O_RDONLY|O_DIRECTORY);if(handle<0)fail("MIGRATION_PATH","Cannot open directory");resolved=fs::canonical(path);
#endif
        try{path_limit(resolved);}catch(...){
#ifdef _WIN32
            CloseHandle(handle);handle=INVALID_HANDLE_VALUE;
#else
            ::close(handle);handle=-1;
#endif
            throw;
        }
    }
    ~DirectoryHandle(){
#ifdef _WIN32
        if(handle!=INVALID_HANDLE_VALUE)CloseHandle(handle);
#else
        if(handle>=0)::close(handle);
#endif
    }
};
bool prefix(const fs::path& a,const fs::path& b){auto x=a.begin(),y=b.begin();for(;x!=a.end();++x,++y){if(y==b.end())return false;auto lhs=x->native(),rhs=y->native();
#ifdef _WIN32
        if(CompareStringOrdinal(lhs.c_str(),static_cast<int>(lhs.size()),rhs.c_str(),static_cast<int>(rhs.size()),TRUE)!=CSTR_EQUAL)return false;
#else
        if(lhs!=rhs)return false;
#endif
    }return true;}
class ReadLock {
public:
#ifdef _WIN32
    HANDLE handle=INVALID_HANDLE_VALUE;
#else
    int handle=-1;
#endif
    explicit ReadLock(const fs::path& path){path_limit(path);
#ifdef _WIN32
        handle=CreateFileW(path.c_str(),GENERIC_READ,0,nullptr,OPEN_EXISTING,FILE_ATTRIBUTE_NORMAL,nullptr);if(handle==INVALID_HANDLE_VALUE)fail(GetLastError()==ERROR_FILE_NOT_FOUND?"SOURCE_LOCK_MISSING":"DATABASE_LOCKED","Closed source with its existing database/media lock file is required");LARGE_INTEGER size{};if(!GetFileSizeEx(handle,&size)||size.QuadPart!=0){CloseHandle(handle);handle=INVALID_HANDLE_VALUE;fail("MIGRATION_LOCKFILE","Lock file must be empty");}
#else
        handle=::open(path.c_str(),O_RDONLY);if(handle<0)fail("SOURCE_LOCK_MISSING","Existing source lock required");if(::flock(handle,LOCK_EX|LOCK_NB)!=0){::close(handle);handle=-1;fail("DATABASE_LOCKED","Source is open");}struct stat s{};if(::fstat(handle,&s)||s.st_size){::close(handle);handle=-1;fail("MIGRATION_LOCKFILE","Lock file must be empty");}
#endif
    }
    ~ReadLock(){
#ifdef _WIN32
        if(handle!=INVALID_HANDLE_VALUE)CloseHandle(handle);
#else
        if(handle>=0)::close(handle);
#endif
    }
};
std::string hash_file(const fs::path& path,std::chrono::steady_clock::time_point deadline){std::ifstream in(path,std::ios::binary);if(!in)fail("MIGRATION_IO","Cannot open file for read-only SHA256");Sha256 hash;std::array<char,65536> buffer{};while(in){if(std::chrono::steady_clock::now()>deadline)fail("MIGRATION_LIMIT","SHA256 time budget exceeded");in.read(buffer.data(),buffer.size());hash.add(buffer.data(),static_cast<std::size_t>(in.gcount()));}if(in.bad())fail("MIGRATION_IO","Read-only SHA256 failed");return hash.finish();}
void write_new(const fs::path& path,const std::string& text){path_limit(path);
#ifdef _WIN32
    auto h=CreateFileW(path.c_str(),GENERIC_WRITE,0,nullptr,CREATE_NEW,FILE_ATTRIBUTE_NORMAL,nullptr);if(h==INVALID_HANDLE_VALUE)fail("MIGRATION_IO","Cannot create owned new file");bool valid=true;std::size_t off=0;while(off<text.size()){DWORD n=0;auto take=static_cast<DWORD>(std::min<std::size_t>(text.size()-off,65536));if(!WriteFile(h,text.data()+off,take,&n,nullptr)||n!=take){valid=false;break;}off+=n;}if(valid)valid=FlushFileBuffers(h)!=0;CloseHandle(h);if(!valid)fail("MIGRATION_IO","Owned file write/sync failed");
#else
    auto h=::open(path.c_str(),O_CREAT|O_EXCL|O_WRONLY,0600);if(h<0)fail("MIGRATION_IO","Cannot create owned new file");std::size_t off=0;while(off<text.size()){auto n=::write(h,text.data()+off,text.size()-off);if(n<=0){::close(h);fail("MIGRATION_IO","Owned write failed");}off+=static_cast<std::size_t>(n);}auto result=::fsync(h);::close(h);if(result)fail("MIGRATION_IO","Owned sync failed");
#endif
}
json manifest_files(const fs::path& source,const Limits& limits,std::chrono::steady_clock::time_point started,const fs::path* copy=nullptr){json files=json::object();std::uint64_t total=0;std::size_t count=0;for(const auto& entry:fs::directory_iterator(source)){if(std::chrono::steady_clock::now()-started>limits.time)fail("MIGRATION_LIMIT","Source copy/hash time budget exceeded");if(reparse(entry.path())||!entry.is_regular_file())fail("MIGRATION_PATH","Source entries must be regular files; nested directories/reparse points are refused");if(++count>limits.files)fail("MIGRATION_LIMIT","Source file count exceeds migration limit");auto size=entry.file_size();if(size>limits.bytes-total)fail("MIGRATION_LIMIT","Source bytes exceed migration limit");total+=size;auto name=entry.path().filename();if(name==".edudb-owned.json")fail("MIGRATION_PATH","Source contains a reserved ownership marker");path_limit(entry.path());if(name=="database.lock"||name=="control-media.lock"){if(size)fail("MIGRATION_LOCKFILE","Lock file must be empty");files[name.u8string()]={{"bytes",0},{"sha256",sha256("")}};continue;}auto digest=hash_file(entry.path(),started+limits.time);files[name.u8string()]={{"bytes",size},{"sha256",digest}};if(copy){auto target=*copy/name;path_limit(target);std::ifstream in(entry.path(),std::ios::binary);std::ofstream out(target,std::ios::binary|std::ios::out|std::ios::trunc);if(!in||!out)fail("MIGRATION_IO","Source read-only copy failed to open");std::array<char,65536> buffer{};std::uint64_t actual=0;while(in){if(std::chrono::steady_clock::now()-started>limits.time)fail("MIGRATION_LIMIT","Copy time budget exceeded");in.read(buffer.data(),buffer.size());auto n=static_cast<std::size_t>(in.gcount());if(n>size-actual)fail("SOURCE_CHANGED","Source grew while copied");out.write(buffer.data(),n);actual+=n;}out.flush();out.close();if(in.bad()||!out||actual!=size||hash_file(target,started+limits.time)!=digest)fail("SOURCE_CHANGED","Source copy differs from its pre-copy SHA");}}
    return files;}
}
std::string sha256(const std::string& value){Sha256 hash;hash.add(value.data(),value.size());return hash.finish();}
std::string file_sha256(const fs::path& path){return hash_file(path,std::chrono::steady_clock::time_point::max());}
json read_control(const fs::path& path){path_limit(path);if(fs::file_size(path)>1024u*1024u)fail("CORRUPT_CONTROL","Control manifest exceeds1MiB");std::ifstream in(path,std::ios::binary);if(!in)fail("CORRUPT_CONTROL","Cannot read control manifest");std::string bytes((std::istreambuf_iterator<char>(in)),std::istreambuf_iterator<char>());json value;try{value=json::parse(bytes);auto copy=value;copy.erase("checksum");if(value.at("checksum").get<std::uint32_t>()!=crc(copy.dump()))fail("CORRUPT_CONTROL","Control checksum mismatch");}catch(const DbError&){throw;}catch(...){fail("CORRUPT_CONTROL","Malformed control manifest");}return value;}
void write_control(const fs::path& directory,json value){value.erase("checksum");value["checksum"]=crc(value.dump());auto temporary=directory/(".edudb-control-"+nonce()+".tmp");write_new(temporary,value.dump(2));
#ifdef _WIN32
    if(!MoveFileExW(temporary.c_str(),(directory/"control.json").c_str(),MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH))fail("MIGRATION_IO","Cannot atomically replace owned copy control");
#else
    fs::rename(temporary,directory/"control.json");
#endif
}
struct Destination::Impl{DirectoryHandle parent;fs::path target;explicit Impl(const fs::path& requested):parent(fs::absolute(requested).parent_path()){auto leaf=requested.filename();if(leaf.empty()||leaf=="."||leaf=="..")fail("MIGRATION_PATH","New destination needs a filename component");target=parent.resolved/leaf;path_limit(target);if(fs::exists(target)||fs::is_symlink(fs::symlink_status(target)))fail("TARGET_EXISTS","Migration destination must not exist");}};
Destination::Destination(const fs::path& requested):impl_(new Impl(requested)){}Destination::~Destination()=default;const fs::path& Destination::path()const{return impl_->target;}const fs::path& Destination::parent()const{return impl_->parent.resolved;}
void Destination::separate_from(const fs::path& source)const{if(prefix(source,path())||prefix(path(),source))fail("MIGRATION_PATH","Resolved source and target overlap");}
struct OwnedDirectory::Impl{DirectoryHandle parent_handle;fs::path parent,working;std::string nonce_value;bool published=false;Impl(const fs::path& p,const std::string& purpose):parent_handle(p),parent(parent_handle.resolved),nonce_value(nonce()){if(purpose!="snapshot"&&purpose!="staging")fail("MIGRATION_ARGUMENT","Unknown owned directory kind");working=parent/(".edudb-v3-"+purpose+"-"+nonce_value);path_limit(working/"control-aries-durable.json");if(!fs::create_directory(working))fail("MIGRATION_IO","Owned directory already exists");try{write_new(working/".edudb-owned.json",json{{"format","EduDB-owned-migration"},{"token",nonce_value},{"kind",purpose}}.dump());}catch(...){std::error_code error;fs::remove(working/".edudb-owned.json",error);fs::remove(working,error);throw;}}
    ~Impl(){if(published||working.empty())return;try{if(reparse(working))return;auto resolved=DirectoryHandle(working).resolved;if(resolved.parent_path()!=parent||resolved.filename()!=working.filename())return;json marker;{std::ifstream in(working/".edudb-owned.json",std::ios::binary);in>>marker;}if(marker.value("format",std::string())!="EduDB-owned-migration"||marker.value("token",std::string())!=nonce_value)return;for(const auto& entry:fs::directory_iterator(working))if(reparse(entry.path())||!entry.is_regular_file())return;fs::remove_all(working);}catch(...){/* Preserve unrecognized/changed material rather than deleting it. */}}
};
OwnedDirectory::OwnedDirectory(const fs::path& parent,const std::string& purpose):impl_(new Impl(parent,purpose)){}OwnedDirectory::~OwnedDirectory()=default;const fs::path& OwnedDirectory::path()const{return impl_->working;}const std::string& OwnedDirectory::token()const{return impl_->nonce_value;}
void OwnedDirectory::publish(const Destination& destination){if(impl_->published||path().parent_path()!=destination.parent()||fs::exists(destination.path()))fail("TARGET_EXISTS","Atomic publication requires an absent destination in the same actual parent");
#ifdef _WIN32
    if(!MoveFileExW(path().c_str(),destination.path().c_str(),MOVEFILE_WRITE_THROUGH))fail("MIGRATION_PUBLISH","Atomic no-replace directory publication failed");
#else
    // Windows is the validated platform. rename on another platform must not
    // be advertised as a raced no-replace primitive without separate tests.
    if(fs::exists(destination.path()))fail("TARGET_EXISTS","Destination appeared");fs::rename(path(),destination.path());
#endif
    impl_->published=true;
}
struct LegacySnapshot::Impl{DirectoryHandle source_handle;ReadLock source_lock;std::unique_ptr<ReadLock> media_lock;std::unique_ptr<OwnedDirectory> copy;Limits limits;std::chrono::steady_clock::time_point started=std::chrono::steady_clock::now();json manifest,files;
    Impl(const fs::path& source,const fs::path& parent,Limits bounds,const Destination* destination,int expected_control):source_handle(source),source_lock(source_handle.resolved/"database.lock"),limits(bounds){if(destination)destination->separate_from(source_handle.resolved);if(fs::exists(source_handle.resolved/"control-media.lock"))media_lock=std::make_unique<ReadLock>(source_handle.resolved/"control-media.lock");manifest=read_control(source_handle.resolved/"control.json");if(manifest.value("format",std::string())!="EduDB"||manifest.value("version",0)!=expected_control||manifest.value("page_size",0)!=8192||(expected_control==4&&(manifest.value("minimum_reader_format",0)!=4||manifest.value("sql_catalog_revision",0)!=0||manifest.value("recovery_role",std::string("PRIMARY"))!="PRIMARY")))fail("INCOMPATIBLE_FORMAT","Unsupported source database version, catalog revision, or recovery role for this migration");copy=std::make_unique<OwnedDirectory>(parent,"snapshot");files=manifest_files(source_handle.resolved,limits,started,&copy->path());if(files!=manifest_files(source_handle.resolved,limits,started))fail("SOURCE_CHANGED","Source changed during snapshot copy");}
};
LegacySnapshot::LegacySnapshot(const fs::path& source,const fs::path& parent,Limits limits,const Destination* destination,int expected_control):impl_(new Impl(source,parent,limits,destination,expected_control)){}LegacySnapshot::~LegacySnapshot()=default;const fs::path& LegacySnapshot::source()const{return impl_->source_handle.resolved;}const fs::path& LegacySnapshot::path()const{return impl_->copy->path();}const json& LegacySnapshot::control()const{return impl_->manifest;}const json& LegacySnapshot::source_files()const{return impl_->files;}
void LegacySnapshot::promote_owned_copy(){auto c=read_control(path()/"control.json");if(c.value("version",0)!=3)fail("INCOMPATIBLE_FORMAT","Only a freshly owned format3 copy can be promoted for isolated recovery");c["version"]=4;c["page_format"]=3;c["minimum_reader_format"]=4;c.erase("sql_catalog_revision");write_control(path(),c);}
void LegacySnapshot::verify_source_unchanged()const{if(impl_->files!=manifest_files(source(),impl_->limits,impl_->started))fail("SOURCE_CHANGED","Original source SHA set changed before completion");}
}
