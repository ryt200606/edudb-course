// Included in recovery's anonymous namespace after media_native.hpp.
void regular_input(const fs::path& path){
    if(native_reparse(path)||!native_regular(path))fail("BACKUP_PATH","Expected a regular immutable input file: "+path.u8string());
#ifdef _WIN32
    const auto attributes=GetFileAttributesW(native_path(path).c_str());
    if(attributes==INVALID_FILE_ATTRIBUTES||(attributes&FILE_ATTRIBUTE_REPARSE_POINT))fail("BACKUP_PATH","Reparse-point input is forbidden");
#endif
}
class ReadOnlyFile final:public DataDevice {
    fs::path path_;std::mutex mutex_;
#ifdef _WIN32
    HANDLE handle_=INVALID_HANDLE_VALUE;
#else
    int fd_=-1;
#endif
public:
    explicit ReadOnlyFile(fs::path path,bool share_delete=false):path_(absolute_path(path)){
        regular_input(path_);
#ifdef _WIN32
        handle_=CreateFileW(native_path(path_).c_str(),GENERIC_READ,FILE_SHARE_READ|(share_delete?FILE_SHARE_DELETE:0),nullptr,OPEN_EXISTING,FILE_ATTRIBUTE_NORMAL,nullptr);
        if(handle_==INVALID_HANDLE_VALUE)fail("BACKUP_IO","Cannot open immutable input read-only: "+path_.u8string());
#else
        (void)share_delete;
        fd_=::open(path_.c_str(),O_RDONLY);if(fd_<0)fail("BACKUP_IO","Cannot open immutable input read-only");
#endif
    }
    ~ReadOnlyFile()override{
#ifdef _WIN32
        if(handle_!=INVALID_HANDLE_VALUE)CloseHandle(handle_);
#else
        if(fd_>=0)::close(fd_);
#endif
    }
    std::uint64_t size()override{std::lock_guard<std::mutex> guard(mutex_);
#ifdef _WIN32
        LARGE_INTEGER result{};if(!GetFileSizeEx(handle_,&result))fail("BACKUP_IO","Input size failed");return static_cast<std::uint64_t>(result.QuadPart);
#else
        struct stat s{};if(::fstat(fd_,&s))fail("BACKUP_IO","Input size failed");return static_cast<std::uint64_t>(s.st_size);
#endif
    }
    void read(std::uint64_t at,unsigned char* out,std::size_t length)override{
        if(at>static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())-length)fail("BACKUP_LIMIT","Read offset overflow");
        std::lock_guard<std::mutex> guard(mutex_);
#ifdef _WIN32
        LARGE_INTEGER offset{};offset.QuadPart=static_cast<LONGLONG>(at);if(!SetFilePointerEx(handle_,offset,nullptr,FILE_BEGIN))fail("BACKUP_IO","Input seek failed");
#endif
        while(length){auto started=std::chrono::steady_clock::now();auto part=std::min<std::size_t>(length,1u<<28);std::size_t actual=0;bool okay=false;
#ifdef _WIN32
            DWORD got=0;okay=ReadFile(handle_,out,static_cast<DWORD>(part),&got,nullptr)!=0;actual=got;
#else
            auto got=::pread(fd_,out,part,static_cast<off_t>(at));if(got<0&&errno==EINTR)continue;okay=got>=0;actual=got>0?static_cast<std::size_t>(got):0;
#endif
            io_record(path_,"read",actual,io_elapsed(started),!okay||!actual,at);if(!okay||!actual)fail("BACKUP_IO","Immutable input short read");out+=actual;at+=actual;length-=actual;
        }
    }
    void write(std::uint64_t,const unsigned char*,std::size_t)override{fail("READ_ONLY","Immutable backup/archive input");}
    void sync()override{fail("READ_ONLY","Immutable backup/archive input");}
};
class Sha256 {
#ifdef _WIN32
    HCRYPTPROV provider_=0;HCRYPTHASH hash_=0;
#else
    PortableSha256 hash_;
#endif
public:
    Sha256(){
#ifdef _WIN32
        if(!CryptAcquireContextW(&provider_,nullptr,nullptr,PROV_RSA_AES,CRYPT_VERIFYCONTEXT))fail("BACKUP_HASH","SHA256 provider unavailable");
        if(!CryptCreateHash(provider_,CALG_SHA_256,0,0,&hash_)){CryptReleaseContext(provider_,0);provider_=0;fail("BACKUP_HASH","SHA256 context unavailable");}
#endif
    }
    ~Sha256(){
#ifdef _WIN32
        if(hash_)CryptDestroyHash(hash_);
        if(provider_)CryptReleaseContext(provider_,0);
#endif
    }
    void add(const unsigned char* p,std::size_t size){
#ifdef _WIN32
        while(size){auto n=static_cast<DWORD>(std::min<std::size_t>(size,1u<<28));if(!CryptHashData(hash_,p,n,0))fail("BACKUP_HASH","SHA256 update failed");p+=n;size-=n;}
#else
        hash_.add(reinterpret_cast<const char*>(p),size);
#endif
    }
    std::string finish(){
#ifdef _WIN32
        unsigned char bytes[32];DWORD size=32;if(!CryptGetHashParam(hash_,HP_HASHVAL,bytes,&size,0)||size!=32)fail("BACKUP_HASH","SHA256 finish failed");static constexpr char digits[]="0123456789abcdef";std::string text;for(auto b:bytes){text+=digits[b>>4];text+=digits[b&15];}return text;
#else
        return hash_.finish();
#endif
    }
};
std::string new_identity(){std::random_device r;std::ostringstream text;text<<std::hex<<std::setfill('0');for(unsigned n=0;n<4;++n)text<<std::setw(8)<<r();return text.str().substr(0,32);}
void replace_atomic(const fs::path& from,const fs::path& to,bool overwrite){
#ifdef _WIN32
    const auto start=std::chrono::steady_clock::now();
    for(;;){auto call=std::chrono::steady_clock::now();const bool okay=MoveFileExW(native_path(from).c_str(),native_path(to).c_str(),MOVEFILE_WRITE_THROUGH|(overwrite?MOVEFILE_REPLACE_EXISTING:0))!=0;const auto error=okay?0:GetLastError();io_record(to,"atomic_replace",0,io_elapsed(call),!okay);if(okay)return;const bool transient=error==ERROR_ACCESS_DENIED||error==ERROR_SHARING_VIOLATION||error==ERROR_LOCK_VIOLATION||error==ERROR_USER_MAPPED_FILE;if(!transient||io_elapsed(start)/1000000>=500)fail("BACKUP_IO","Atomic publication failed with Win32 error "+std::to_string(error));std::this_thread::sleep_for(std::chrono::milliseconds(2));}
#else
    if(!overwrite&&native_exists(to))fail("BACKUP_EXISTS","Publication target exists");media_detail::replace(from,to);
#endif
}
