#include "edudb.hpp"
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <thread>
using namespace edudb;
namespace fs=std::filesystem;

static json sql(Engine& db,const std::string& sid,const std::string& text) {
    auto r=db.execute(sid,text);
    if(!r.at("ok").get<bool>())throw std::runtime_error(text+" => "+r.at("error").dump());
    return r;
}
static void inject(const char* fault) {
#ifdef _WIN32
    _putenv_s("EDUDB_TEST_IO_FAULT",fault);
#else
    if(*fault)setenv("EDUDB_TEST_IO_FAULT",fault,1);else unsetenv("EDUDB_TEST_IO_FAULT");
#endif
}
static void check(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}

static json run(const fs::path& dir,const std::string& fault,unsigned limit) {
    constexpr int clients=4;
    std::array<std::string,clients> table,sid;
    StorageOptions opts;opts.wal_group_size=limit;opts.wal_group_wait_us=10000;
    {
        Engine db(dir.u8string(),16,1u<<22,opts);auto s=db.open_session();
        for(int i=0;i<clients;++i) {
            table[i]="writers"+std::to_string(i);
            sql(db,s,"CREATE TABLE "+table[i]+"(id INTEGER PRIMARY KEY)");
        }
    }
    json stats;
    {
        inject(fault.c_str());
        Engine db(dir.u8string(),16,1u<<22,opts);inject("");
        for(int i=0;i<clients;++i){
            sid[i]=db.open_session();sql(db,sid[i],"BEGIN");
            sql(db,sid[i],"INSERT INTO "+table[i]+" VALUES (1),(2)");
        }
        std::mutex mutex;std::condition_variable cv;int ready=0;
        std::array<json,clients> results;std::vector<std::thread> threads;
        for(int i=0;i<clients;++i)threads.emplace_back([&,i] {
            std::unique_lock<std::mutex> guard(mutex);++ready;cv.notify_all();
            cv.wait(guard,[&]{return ready==clients;});guard.unlock();
            results[i]=db.execute(sid[i],"COMMIT");
        });
        for(auto& thread:threads)thread.join();
        auto storage=db.diagnostics().at("storage");stats=storage.at("group_commit");
        for(auto& r:results)check(r.at("ok").get<bool>()==fault.empty(),"incorrect client confirmation");
        if(fault.empty()&&limit>1){
            check(stats["largest_batch"].get<unsigned>()>=2,"no shared force was actually observed");
            check(stats["transactions"]==clients,"missed a transaction in diagnostics");
            check(storage.at("wal_forces").get<unsigned>()<clients,"writes were individually synced");
        }
        if(!fault.empty())check(storage["recovery_required"].get<bool>(),"failed batch remained writable");
    }
    json counts;
    for(int restart=0;restart<2;++restart){
        Engine db(dir.u8string(),16,1u<<22,opts);auto s=db.open_session();json observed=json::array();
        for(auto& name:table){
            auto r=sql(db,s,"SELECT id FROM "+name+" ORDER BY id").at("rows");
            if(!r.empty() && r!=json::parse("[[1],[2]]"))throw std::runtime_error("partial transaction in "+name+": "+r.dump());
            if(fault.empty())check(r.size()==2,"acknowledged transaction lost");
            observed.push_back(r.size());
        }
        if(restart)check(observed==counts,"non-idempotent recovery");else counts=observed;
    }
    return {{"group",stats},{"fault",fault},{"recovered_row_counts",counts}};
}
int main(){try{
    auto root=fs::temp_directory_path()/("edudb-gc-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    json tests=json::array();for(auto fault:{"","wal_append_once","wal_sync_once"})tests.push_back(run(root/("group-"+std::string(fault)),fault,4));
    tests.push_back(run(root/"individual","",1));std::cout<<tests.dump()<<std::endl;return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<std::endl;return 1;}}
