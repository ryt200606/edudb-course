#pragma once
#include "aries_recovery.hpp"
#include <memory>
#include <string>
#include <vector>
namespace edudb::logical {
using json=nlohmann::json;
struct Limits {
    static constexpr std::uint64_t file_pages=1024,row_bytes=1024,table_rows=128;
    static constexpr std::uint64_t new_pages_per_attempt=64,recovery_attempts=2;
    static constexpr std::size_t active_operations=3,active_transactions=64,savepoints=64;
    static constexpr std::size_t operation_history=256;
    static constexpr std::uint64_t update_bytes=80+file_pages*(80+2*aries::page_size);
    static constexpr std::uint64_t clr_bytes=file_pages*(80+aries::page_size);
    static constexpr std::uint64_t attempt_credit=update_bytes+clr_bytes+640;
    static constexpr std::uint64_t operation_credit=recovery_attempts*attempt_credit;
    static constexpr std::uint64_t operation_pages=recovery_attempts*new_pages_per_attempt;
};
struct Operation {
    std::uint64_t id=0,tx=0,mini_tx=0,previous=0,undo_next=0;
    json description;
    bool complete=false,compensated=false;
    struct Attempt {std::uint64_t tx=0,allocation_start=0,allocation_end=0;};
    std::vector<Attempt> attempts;
};
class Journal {
public:
    // Analysis only. No callback into Storage or physical Manager is made.
    Journal(const std::string& directory,const std::string& uuid);
    ~Journal();
    void begin(std::uint64_t tx);
    void admit(std::uint64_t tx,std::uint64_t planned_forward_pages,aries::Manager& physical);
    std::uint64_t start_operation(std::uint64_t tx,std::uint64_t mini_tx,json description,
                                  std::uint64_t allocation_start,std::uint64_t allocation_end);
    void complete_operation(std::uint64_t operation);
    void abort_begin(std::uint64_t tx);
    std::vector<Operation> pending(std::uint64_t tx)const;
    void start_undo(std::uint64_t operation,std::uint64_t mini_tx,
                    std::uint64_t allocation_start,std::uint64_t allocation_end);
    void compensated(std::uint64_t operation);
    void commit(std::uint64_t tx);
    void finish_abort(std::uint64_t tx);
    void savepoint(std::uint64_t tx,const std::string& name);
    std::vector<Operation> after_savepoint(std::uint64_t tx,const std::string& name)const;
    void keep_savepoint(std::uint64_t tx,const std::string& name);
    void release_savepoint(std::uint64_t tx,const std::string& name);
    json savepoint_stats(std::uint64_t tx)const;
    // Reconstruct actual remaining credits after physical recovery has appended
    // any interrupted-mini-transaction CLRs. No global counter difference.
    void synchronize_reservations(aries::Manager& physical);
    std::uint64_t remaining_pages()const;
    std::vector<std::uint64_t> loser_transactions()const;
    std::uint64_t max_id()const;
    std::uint64_t allocation_high_water()const;
    json diagnostics()const;
    json wal_records(std::size_t limit=1024)const;
    bool failed()const;
    Journal(const Journal&)=delete;Journal& operator=(const Journal&)=delete;
private:struct Impl;std::unique_ptr<Impl> impl_;
};
}
