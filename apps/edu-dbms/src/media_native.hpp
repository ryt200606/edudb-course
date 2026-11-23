#pragma once
#include "io.hpp"
#include "windows_paths.hpp"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace edudb::media_detail {
namespace fs=std::filesystem;
[[noreturn]] inline void fail(const std::string& code,const std::string& reason){throw DbError(code,reason);}
inline void put32(unsigned char* p,std::uint32_t n){for(unsigned i=0;i<4;++i)p[i]=static_cast<unsigned char>(n>>(8*i));}
inline void put64(unsigned char* p,std::uint64_t n){for(unsigned i=0;i<8;++i)p[i]=static_cast<unsigned char>(n>>(8*i));}
inline std::uint32_t get32(const unsigned char* p){std::uint32_t n=0;for(int i=3;i>=0;--i)n=(n<<8)|p[i];return n;}
inline std::uint64_t get64(const unsigned char* p){std::uint64_t n=0;for(int i=7;i>=0;--i)n=(n<<8)|p[i];return n;}
inline std::uint32_t crc(const unsigned char* p,std::size_t n){
    static const auto table=[] {std::array<std::uint32_t,256> a{};for(std::uint32_t i=0;i<256;++i){auto c=i;for(unsigned j=0;j<8;++j)c=(c&1)?0xedb88320u^(c>>1):(c>>1);a[i]=c;}return a;}();
    std::uint32_t c=0xffffffffu;for(std::size_t i=0;i<n;++i)c=table[(c^p[i])&255]^(c>>8);return c^0xffffffffu;
}
inline std::uint32_t crc(const std::string& s){return crc(reinterpret_cast<const unsigned char*>(s.data()),s.size());}
class File {
public:
    explicit File(fs::path path,bool create=false,bool create_new=false,bool exclusive=false):path_(absolute_path(path)){
#ifdef _WIN32
        handle_=CreateFileW(native_path(path_).c_str(),GENERIC_READ|GENERIC_WRITE,exclusive?0:FILE_SHARE_READ,nullptr,create_new?CREATE_NEW:create?OPEN_ALWAYS:OPEN_EXISTING,FILE_ATTRIBUTE_NORMAL,nullptr);
        if(handle_==INVALID_HANDLE_VALUE)fail(exclusive?"MEDIA_LOCKED":"MEDIA_IO","Open member/control file failed: "+path_.u8string());
#else
        fd_=::open(path_.c_str(),O_RDWR|(create?O_CREAT:0)|(create_new?(O_EXCL|O_CREAT):0),0600);
        if(fd_<0)fail("MEDIA_IO","Open member/control file failed: "+path_.u8string());
        (void)exclusive;
#endif
    }
    ~File(){
#ifdef _WIN32
        if(handle_!=INVALID_HANDLE_VALUE)CloseHandle(handle_);
#else
        if(fd_>=0)::close(fd_);
#endif
    }
    File(const File&)=delete;File& operator=(const File&)=delete;
    std::uint64_t size(){
#ifdef _WIN32
        LARGE_INTEGER n{};if(!GetFileSizeEx(handle_,&n))fail("MEDIA_IO","Get file size failed");return static_cast<std::uint64_t>(n.QuadPart);
#else
        struct stat s{};if(::fstat(fd_,&s))fail("MEDIA_IO","Get file size failed");return static_cast<std::uint64_t>(s.st_size);
#endif
    }
    void read(std::uint64_t offset,unsigned char* dst,std::size_t length){transfer(offset,dst,nullptr,length);}
    void write(std::uint64_t offset,const unsigned char* src,std::size_t length){transfer(offset,nullptr,src,length);}
    void truncate(std::uint64_t n){auto started=std::chrono::steady_clock::now();bool ok=false;
#ifdef _WIN32
        LARGE_INTEGER p{};p.QuadPart=static_cast<LONGLONG>(n);ok=SetFilePointerEx(handle_,p,nullptr,FILE_BEGIN)&&SetEndOfFile(handle_);
#else
        ok=::ftruncate(fd_,static_cast<off_t>(n))==0;
#endif
        io_record(path_,"truncate",0,io_elapsed(started),!ok,n);if(!ok)fail("MEDIA_IO","Truncate failed");
    }
    void sync(){auto started=std::chrono::steady_clock::now();bool ok=false;
#ifdef _WIN32
        ok=FlushFileBuffers(handle_)!=0;
#else
        ok=::fsync(fd_)==0;
#endif
        io_record(path_,"sync",0,io_elapsed(started),!ok);if(!ok)fail("MEDIA_IO","Member/control sync failed");
    }
private:
    void transfer(std::uint64_t offset,unsigned char* dst,const unsigned char* src,std::size_t length){
        if(offset>static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())-length)fail("MEDIA_LIMIT","File offset too large");
#ifdef _WIN32
        LARGE_INTEGER p{};p.QuadPart=static_cast<LONGLONG>(offset);if(!SetFilePointerEx(handle_,p,nullptr,FILE_BEGIN))fail("MEDIA_IO","Seek failed");
#endif
        while(length){const auto part=std::min<std::size_t>(length,1u<<28);auto started=std::chrono::steady_clock::now();std::uint64_t got=0;bool ok=false;
#ifdef _WIN32
            DWORD transferred=0;ok=src?WriteFile(handle_,src,static_cast<DWORD>(part),&transferred,nullptr)!=0:ReadFile(handle_,dst,static_cast<DWORD>(part),&transferred,nullptr)!=0;got=transferred;
#else
            auto n=src ? ::pwrite(fd_,src,part,static_cast<off_t>(offset)) : ::pread(fd_,dst,part,static_cast<off_t>(offset));auto err=errno;ok=n>=0;got=n>0?static_cast<std::uint64_t>(n):0;
            if(n<0&&err==EINTR){io_record(path_,src?"write":"read",0,io_elapsed(started),true,offset);continue;}
#endif
            io_record(path_,src?"write":"read",got,io_elapsed(started),!ok||got==0,offset);if(!ok||got==0)fail("MEDIA_IO","Short/failed member I/O");
            if(src)src+=got;else dst+=got;offset+=got;length-=static_cast<std::size_t>(got);
        }
    }
    fs::path path_;
#ifdef _WIN32
    HANDLE handle_=INVALID_HANDLE_VALUE;
#else
    int fd_=-1;
#endif
};
inline void replace(const fs::path& from,const fs::path& to){
#ifdef _WIN32
    if(!MoveFileExW(native_path(from).c_str(),native_path(to).c_str(),MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH))fail("MEDIA_IO","Atomic manifest/member switch failed");
#else
    fs::rename(from,to);auto parent=to.parent_path();int d=::open(parent.c_str(),O_RDONLY|O_DIRECTORY);if(d<0)fail("MEDIA_IO","Open directory failed");auto result=::fsync(d);::close(d);if(result)fail("MEDIA_IO","Directory sync failed");
#endif
}
} // namespace edudb::media_detail
