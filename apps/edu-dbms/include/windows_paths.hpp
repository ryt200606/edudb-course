#pragma once
#include "edudb.hpp"
#include <filesystem>
#include <vector>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif
namespace edudb::media_detail {
inline std::filesystem::path presentation_path(const std::filesystem::path& path){
#ifdef _WIN32
    auto copy=path;auto value=copy.make_preferred().wstring();
    if(value.rfind(L"\\\\?\\UNC\\",0)==0)value=L"\\\\"+value.substr(8);
    else if(value.rfind(L"\\\\?\\",0)==0)value=value.substr(4);
    return std::filesystem::path(value);
#else
    return path;
#endif
}
inline std::filesystem::path absolute_path(const std::filesystem::path& path){return std::filesystem::absolute(presentation_path(path)).lexically_normal().make_preferred();}
inline std::filesystem::path native_path(const std::filesystem::path& path){
#ifdef _WIN32
    auto value=absolute_path(path).wstring();
    if(value.rfind(L"\\\\",0)==0)return std::filesystem::path(L"\\\\?\\UNC\\"+value.substr(2));
    return std::filesystem::path(L"\\\\?\\"+value);
#else
    return path;
#endif
}
inline bool native_exists(const std::filesystem::path& path){
#ifdef _WIN32
    auto attributes=GetFileAttributesW(native_path(path).c_str());
    if(attributes!=INVALID_FILE_ATTRIBUTES)return true;
    auto error=GetLastError();if(error==ERROR_FILE_NOT_FOUND||error==ERROR_PATH_NOT_FOUND)return false;
    throw DbError("MEDIA_IO","Cannot inspect native path "+native_path(path).u8string()+" (Win32 "+std::to_string(error)+")");
#else
    return std::filesystem::exists(path);
#endif
}
inline bool native_reparse(const std::filesystem::path& path){
#ifdef _WIN32
    auto attributes=GetFileAttributesW(native_path(path).c_str());return attributes!=INVALID_FILE_ATTRIBUTES&&(attributes&FILE_ATTRIBUTE_REPARSE_POINT);
#else
    return std::filesystem::is_symlink(path);
#endif
}
inline bool native_directory(const std::filesystem::path& path){
#ifdef _WIN32
    auto attributes=GetFileAttributesW(native_path(path).c_str());return attributes!=INVALID_FILE_ATTRIBUTES&&(attributes&FILE_ATTRIBUTE_DIRECTORY);
#else
    return std::filesystem::is_directory(path);
#endif
}
inline bool native_regular(const std::filesystem::path& path){
#ifdef _WIN32
    auto attributes=GetFileAttributesW(native_path(path).c_str());return attributes!=INVALID_FILE_ATTRIBUTES&&!(attributes&(FILE_ATTRIBUTE_DIRECTORY|FILE_ATTRIBUTE_REPARSE_POINT));
#else
    return std::filesystem::is_regular_file(path);
#endif
}
// Resolve existing ancestors through the OS (including junctions), then append
// an absent destination tail. MinGW weakly_canonical rejects some extended
// paths, so containment checks cannot rely on it for native long paths.
inline std::filesystem::path canonical_path(const std::filesystem::path& path){
#ifdef _WIN32
    auto ancestor=absolute_path(path);std::vector<std::filesystem::path> tail;
    while(!native_exists(ancestor)){auto parent=ancestor.parent_path();if(parent.empty()||parent==ancestor)throw DbError("BACKUP_PATH","No existing ancestor for destination");tail.push_back(ancestor.filename());ancestor=parent;}
    HANDLE file=CreateFileW(native_path(ancestor).c_str(),FILE_READ_ATTRIBUTES,FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,nullptr,OPEN_EXISTING,FILE_FLAG_BACKUP_SEMANTICS,nullptr);
    if(file==INVALID_HANDLE_VALUE)throw DbError("BACKUP_PATH","Cannot resolve existing path ancestor (Win32 "+std::to_string(GetLastError())+")");
    auto size=GetFinalPathNameByHandleW(file,nullptr,0,FILE_NAME_NORMALIZED|VOLUME_NAME_DOS);
    if(!size){auto error=GetLastError();CloseHandle(file);throw DbError("BACKUP_PATH","Cannot resolve final native path (Win32 "+std::to_string(error)+")");}
    std::wstring value(size,L'\0');auto actual=GetFinalPathNameByHandleW(file,value.data(),size,FILE_NAME_NORMALIZED|VOLUME_NAME_DOS);auto error=GetLastError();CloseHandle(file);
    if(!actual||actual>=size)throw DbError("BACKUP_PATH","Final path changed during resolution (Win32 "+std::to_string(error)+")");
    value.resize(actual);
    auto resolved=presentation_path(std::filesystem::path(value));for(auto it=tail.rbegin();it!=tail.rend();++it)resolved/=*it;return resolved.lexically_normal().make_preferred();
#else
    return std::filesystem::weakly_canonical(path);
#endif
}
inline std::vector<std::filesystem::path> native_entries(const std::filesystem::path& directory,std::size_t limit=8192){
    std::vector<std::filesystem::path> entries;
#ifdef _WIN32
    const auto base=absolute_path(directory);WIN32_FIND_DATAW data{};
    HANDLE handle=FindFirstFileW(native_path(base/L"*").c_str(),&data);
    if(handle==INVALID_HANDLE_VALUE){auto error=GetLastError();if(error==ERROR_FILE_NOT_FOUND)return entries;throw DbError("MEDIA_IO","Cannot enumerate native directory (Win32 "+std::to_string(error)+")");}
    struct Guard{HANDLE handle;~Guard(){FindClose(handle);}} guard{handle};
    for(;;){std::wstring name=data.cFileName;if(name!=L"."&&name!=L".."){if(entries.size()>=limit)throw DbError("BACKUP_LIMIT","Directory entry count exceeds bound");entries.push_back(base/name);}if(!FindNextFileW(handle,&data)){auto error=GetLastError();if(error!=ERROR_NO_MORE_FILES)throw DbError("MEDIA_IO","Native directory enumeration failed (Win32 "+std::to_string(error)+")");break;}}
#else
    for(const auto& item:std::filesystem::directory_iterator(directory)){if(entries.size()>=limit)throw DbError("BACKUP_LIMIT","Directory entry count exceeds bound");entries.push_back(item.path());}
#endif
    return entries;
}
inline bool native_remove(const std::filesystem::path& path){
#ifdef _WIN32
    auto p=native_path(path);auto attrs=GetFileAttributesW(p.c_str());if(attrs==INVALID_FILE_ATTRIBUTES){auto error=GetLastError();if(error==ERROR_FILE_NOT_FOUND||error==ERROR_PATH_NOT_FOUND)return false;throw DbError("MEDIA_IO","Cannot inspect removal target");}
    const bool okay=(attrs&FILE_ATTRIBUTE_DIRECTORY)&&!(attrs&FILE_ATTRIBUTE_REPARSE_POINT)?RemoveDirectoryW(p.c_str())!=0:DeleteFileW(p.c_str())!=0;
    if(!okay)throw DbError("MEDIA_IO","Cannot remove owned native path (Win32 "+std::to_string(GetLastError())+")");
    return true;
#else
    return std::filesystem::remove(path);
#endif
}
}
