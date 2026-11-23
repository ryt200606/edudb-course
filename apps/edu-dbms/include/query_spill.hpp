#pragma once
#include "edudb.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace edudb {
// Temporary row streams used by actual query operators. Runs belong to one
// store and cannot be opened by path supplied in SQL or an HTTP request.
class QuerySpillStore {
public:
    struct Run { std::uint64_t id=0, rows=0, bytes=0; };
    class Writer {
    public:
        ~Writer();
        void append(const json& row);
        Run finish();
        Writer(const Writer&)=delete;
        Writer& operator=(const Writer&)=delete;
    private:
        friend class QuerySpillStore;
        struct Impl;
        explicit Writer(std::unique_ptr<Impl> impl);
        std::unique_ptr<Impl> impl_;
    };
    class Reader {
    public:
        ~Reader();
        bool next(json& row);
        Reader(const Reader&)=delete;
        Reader& operator=(const Reader&)=delete;
    private:
        friend class QuerySpillStore;
        struct Impl;
        explicit Reader(std::unique_ptr<Impl> impl);
        std::unique_ptr<Impl> impl_;
    };
    QuerySpillStore(const std::string& directory, const std::string& database_identity,
                    std::size_t live_byte_limit=512u*1024u*1024u,
                    std::size_t maximum_frame=32u*1024u*1024u);
    ~QuerySpillStore();
    std::unique_ptr<Writer> create();
    std::unique_ptr<Reader> open(const Run& run);
    void erase(const Run& run);
    json diagnostics() const;
    QuerySpillStore(const QuerySpillStore&)=delete;
    QuerySpillStore& operator=(const QuerySpillStore&)=delete;
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};
} // namespace edudb
