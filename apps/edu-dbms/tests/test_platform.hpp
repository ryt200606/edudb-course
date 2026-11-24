#pragma once
#include <cstdlib>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <unistd.h>
#endif
namespace edudb_test {
inline unsigned long process_id() {
#ifdef _WIN32
    return GetCurrentProcessId();
#else
    return static_cast<unsigned long>(::getpid());
#endif
}
inline int set_env(const char* name,const char* value) {
#ifdef _WIN32
    return _putenv_s(name,value);
#else
    return *value ? ::setenv(name,value,1) : ::unsetenv(name);
#endif
}
}
