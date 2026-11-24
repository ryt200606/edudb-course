#pragma once
#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <array>
namespace edudb {
struct IoCounter { std::uint64_t reads=0,writes=0,read_bytes=0,write_bytes=0,read_ns=0,write_ns=0,syncs=0,sync_ns=0,failures=0,truncates=0; };
class IoScope {
public:
    explicit IoScope(std::string purpose="request");
    ~IoScope();
    json snapshot() const;
    IoScope(const IoScope&)=delete;
    IoScope& operator=(const IoScope&)=delete;
    void record(std::size_t role,const std::string& operation,std::uint64_t bytes,std::uint64_t ns,bool failed,const std::filesystem::path& path,std::uint64_t offset);
private:
    IoScope* previous_; std::string purpose_; std::array<IoCounter,4> counters_{}; json trace_=json::array(); std::uint64_t trace_total_=0;
};
// While the originating request is blocked in commit, the batch leader performs
// its page I/O. Attribute that work to the originating request exactly once.
class IoRequestBinding {
public:
    static IoScope* current() noexcept;
    explicit IoRequestBinding(IoScope* target) noexcept;
    ~IoRequestBinding() noexcept;
    IoRequestBinding(const IoRequestBinding&)=delete;
    IoRequestBinding& operator=(const IoRequestBinding&)=delete;
private:
    IoScope* previous_;
};
// Explicit owner affects instance totals only; request trace keeps real paths.
// Thread-local nesting is implemented once in io.cpp, never inline dynamic TLS.
class IoOwnerScope {
public:
    explicit IoOwnerScope(const std::filesystem::path& directory);
    ~IoOwnerScope() noexcept;
    IoOwnerScope(const IoOwnerScope&)=delete;
    IoOwnerScope& operator=(const IoOwnerScope&)=delete;
    IoOwnerScope(IoOwnerScope&&)=delete;
    IoOwnerScope& operator=(IoOwnerScope&&)=delete;
private:
    std::string owner_;
    const IoOwnerScope* previous_=nullptr;
    friend void io_record(const std::filesystem::path&,const std::string&,
                          std::uint64_t,std::uint64_t,bool,std::uint64_t);
};
void io_record(const std::filesystem::path& path,const std::string& operation,std::uint64_t bytes,std::uint64_t ns,bool failed=false,std::uint64_t offset=0);
json io_snapshot(const std::filesystem::path& directory);
void io_reset(const std::filesystem::path& directory);
inline std::uint64_t io_elapsed(std::chrono::steady_clock::time_point start){return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-start).count());}
}
