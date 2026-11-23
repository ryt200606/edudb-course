// Native persistence for the independent ARIES module. Every file operation is
// reported through the existing IoScope/role accounting boundary.
class NativeFile final:public DataDevice {
    fs::path path_;
    std::mutex mutex_;
#ifdef _WIN32
    HANDLE handle_=INVALID_HANDLE_VALUE;
#else
    int fd_=-1;
#endif
public:
    NativeFile(fs::path path,bool create,bool truncate=false,bool exclusive=false,bool create_new=false,bool read_only=false):path_(std::move(path)) {
#ifdef _WIN32
        handle_=CreateFileW(path_.c_str(),read_only?GENERIC_READ:GENERIC_READ|GENERIC_WRITE,exclusive?0:read_only?FILE_SHARE_READ|FILE_SHARE_DELETE:FILE_SHARE_READ,nullptr,create_new?CREATE_NEW:truncate?CREATE_ALWAYS:create?OPEN_ALWAYS:OPEN_EXISTING,FILE_ATTRIBUTE_NORMAL,nullptr);
        if(handle_==INVALID_HANDLE_VALUE)throw DbError(exclusive?"DATABASE_LOCKED":"IO_ERROR","Cannot open ARIES file: "+path_.u8string()+" ("+std::to_string(GetLastError())+")");
#else
        fd_=::open(path_.c_str(),(read_only?O_RDONLY:O_RDWR)|(create?O_CREAT:0)|(truncate?O_TRUNC:0)|(create_new?O_EXCL:0),0600);
        if(fd_<0)throw DbError("IO_ERROR","Cannot open ARIES file");
        if(exclusive&&::flock(fd_,LOCK_EX|LOCK_NB)!=0){::close(fd_);fd_=-1;throw DbError("DATABASE_LOCKED","ARIES directory is already open");}
#endif
    }
    ~NativeFile()override {
#ifdef _WIN32
        if(handle_!=INVALID_HANDLE_VALUE)CloseHandle(handle_);
#else
        if(fd_>=0)::close(fd_);
#endif
    }
    std::uint64_t size()override {
        std::lock_guard<std::mutex> lock(mutex_);
#ifdef _WIN32
        LARGE_INTEGER n{};if(!GetFileSizeEx(handle_,&n))throw DbError("IO_ERROR","ARIES GetFileSizeEx failed");return static_cast<std::uint64_t>(n.QuadPart);
#else
        struct stat st{};if(::fstat(fd_,&st)!=0)throw DbError("IO_ERROR","ARIES fstat failed");return static_cast<std::uint64_t>(st.st_size);
#endif
    }
    void read(std::uint64_t offset,unsigned char* destination,std::size_t length)override {
        std::lock_guard<std::mutex> lock(mutex_);
#ifdef _WIN32
        LARGE_INTEGER pos{};pos.QuadPart=static_cast<LONGLONG>(offset);if(!SetFilePointerEx(handle_,pos,nullptr,FILE_BEGIN))throw DbError("IO_ERROR","ARIES read seek failed");
        while(length){DWORD actual=0;auto amount=static_cast<DWORD>(std::min<std::size_t>(length,1u<<30));auto start=Clock::now();auto okay=ReadFile(handle_,destination,amount,&actual,nullptr);io_record(path_,"read",actual,io_elapsed(start),!okay||!actual,offset);if(!okay||!actual)throw DbError("IO_ERROR","Short ARIES file read");destination+=actual;offset+=actual;length-=actual;}
#else
        while(length){auto start=Clock::now();auto actual=::pread(fd_,destination,length,static_cast<off_t>(offset));auto saved=errno;io_record(path_,"read",actual>0?actual:0,io_elapsed(start),actual<=0,offset);if(actual<0&&saved==EINTR)continue;if(actual<=0)throw DbError("IO_ERROR","Short ARIES read");destination+=actual;offset+=actual;length-=actual;}
#endif
    }
    void write(std::uint64_t offset,const unsigned char* source,std::size_t length)override {
        std::lock_guard<std::mutex> lock(mutex_);
#ifdef _WIN32
        LARGE_INTEGER pos{};pos.QuadPart=static_cast<LONGLONG>(offset);if(!SetFilePointerEx(handle_,pos,nullptr,FILE_BEGIN))throw DbError("IO_ERROR","ARIES write seek failed");
        while(length){DWORD actual=0;auto amount=static_cast<DWORD>(std::min<std::size_t>(length,1u<<30));auto start=Clock::now();auto okay=WriteFile(handle_,source,amount,&actual,nullptr);io_record(path_,"write",actual,io_elapsed(start),!okay||!actual,offset);if(!okay||!actual)throw DbError("IO_ERROR","Short ARIES file write");source+=actual;offset+=actual;length-=actual;}
#else
        while(length){auto start=Clock::now();auto actual=::pwrite(fd_,source,length,static_cast<off_t>(offset));auto saved=errno;io_record(path_,"write",actual>0?actual:0,io_elapsed(start),actual<=0,offset);if(actual<0&&saved==EINTR)continue;if(actual<=0)throw DbError("IO_ERROR","Short ARIES write");source+=actual;offset+=actual;length-=actual;}
#endif
    }
    void sync()override {
        std::lock_guard<std::mutex> lock(mutex_);auto start=Clock::now();
#ifdef _WIN32
        auto okay=FlushFileBuffers(handle_)!=0;
#else
        auto okay=::fsync(fd_)==0;
#endif
        io_record(path_,"sync",0,io_elapsed(start),!okay);if(!okay)throw DbError("IO_ERROR","ARIES file synchronization failed");
    }
    void truncate(std::uint64_t length) {
        std::lock_guard<std::mutex> lock(mutex_);auto start=Clock::now();
#ifdef _WIN32
        LARGE_INTEGER pos{};pos.QuadPart=static_cast<LONGLONG>(length);auto okay=SetFilePointerEx(handle_,pos,nullptr,FILE_BEGIN)&&SetEndOfFile(handle_);
#else
        auto okay=::ftruncate(fd_,static_cast<off_t>(length))==0;
#endif
        io_record(path_,"truncate",0,io_elapsed(start),!okay);if(!okay)throw DbError("IO_ERROR","ARIES truncate failed");
    }
};
void atomic_control(const fs::path& path,json body) {
    auto bytes=body.dump();body["crc"]=crc(reinterpret_cast<const unsigned char*>(bytes.data()),bytes.size());bytes=body.dump();static std::atomic<std::uint64_t> names{0};auto temp=path;temp+=".tmp-"+std::to_string(Clock::now().time_since_epoch().count())+"-"+std::to_string(++names);
    bool owned=false;
    try{{NativeFile file(temp,true,false,false,true);owned=true;file.write(0,reinterpret_cast<const unsigned char*>(bytes.data()),bytes.size());file.sync();}
#ifdef _WIN32
        const auto deadline=GetTickCount64()+500;
        while(!MoveFileExW(temp.c_str(),path.c_str(),MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH)){
            const auto error=GetLastError();
            if((error!=ERROR_SHARING_VIOLATION&&error!=ERROR_LOCK_VIOLATION&&error!=ERROR_ACCESS_DENIED)||GetTickCount64()>=deadline)
                throw DbError("IO_ERROR","ARIES atomic control replacement failed: Win32 "+std::to_string(error));
            Sleep(5);
        }
#else
        if(::rename(temp.c_str(),path.c_str())!=0)throw DbError("IO_ERROR","ARIES atomic control rename failed");int fd=::open(path.parent_path().c_str(),O_RDONLY|O_DIRECTORY);if(fd<0||::fsync(fd)!=0){if(fd>=0)::close(fd);throw DbError("IO_ERROR","ARIES directory sync failed");}::close(fd);
#endif
        owned=false;
    }catch(...){if(owned){std::error_code ec;fs::remove(temp,ec);}throw;}
}
