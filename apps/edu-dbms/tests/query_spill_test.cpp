#include "query_spill.hpp"
#include "io.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <functional>
#include <stdexcept>

using namespace edudb;
namespace fs=std::filesystem;
static int checks=0;
static void require(bool value,const char* message){++checks;if(!value)throw std::runtime_error(message);}
static void rejects(const std::function<void()>& action,const std::string& code){bool caught=false;try{action();}catch(const DbError& e){caught=e.code==code;}require(caught,"expected bounded spill error");}
static fs::path run_path(const fs::path& directory){for(auto& e:fs::directory_iterator(directory))if(e.path().extension()==".tmp")return e.path();throw std::runtime_error("temporary run missing");}
static void flip(const fs::path& path,std::streamoff offset){std::fstream f(path,std::ios::binary|std::ios::in|std::ios::out);char byte=0;f.seekg(offset);f.read(&byte,1);require(bool(f),"read byte to corrupt");byte^=0x40;f.seekp(offset);f.write(&byte,1);require(bool(f),"write corruption byte");}
int main(){
    const auto root=fs::current_path()/"query-spill-owned-test";
    try{
        require(!fs::exists(root),"test directory must be new");fs::create_directories(root);
        const std::string identity="0123456789abcdef0123456789abcdef";
        {std::ofstream marker(root/"must-survive.txt");marker<<"unrelated";}
        {
            IoScope scope("spill-correctness");QuerySpillStore store(root.u8string(),identity,1024*1024,65536);
            auto writer=store.create();
            for(int i=0;i<200;++i)writer->append(json::array({i,i%3?json("中文,\n引号\""):json(nullptr),9223372036854775807LL}));
            auto run=writer->finish();require(run.rows==200,"sealed row count");writer.reset();
            auto reader=store.open(run);json row;int i=0;while(reader->next(row)){require(row[0]==i,"ordered row stream");require(row[1]==(i%3?json("中文,\n引号\""):json(nullptr)),"UTF8 NULL and quoted value");require(row[2]==9223372036854775807LL,"int64 precise");++i;}
            require(i==200,"all rows read");rejects([&]{store.erase(run);},"QUERY_RUN_BUSY");reader.reset();
            auto forged=run;++forged.rows;rejects([&]{store.open(forged);},"INVALID_QUERY_RUN");
            require(store.diagnostics()["live_runs"]==1,"one live run");store.erase(run);require(store.diagnostics()["live_bytes"]==0,"quota returned on erase");
            auto metrics=scope.snapshot();require(metrics["files"]["temp"]["read_bytes"].get<std::uint64_t>()>0,"native temp reads recorded");require(metrics["files"]["temp"]["write_bytes"].get<std::uint64_t>()>0,"native temp writes recorded");
        }
        for(int kind=0;kind<3;++kind){
            QuerySpillStore store(root.u8string(),identity);auto writer=store.create();writer->append(json::array({42,"payload"}));auto run=writer->finish();writer.reset();auto path=run_path(root);
            if(kind==0){flip(path,40);rejects([&]{store.open(run);},"QUERY_SPILL_CORRUPT");}
            else if(kind==1){flip(path,84);auto reader=store.open(run);json row;rejects([&]{reader->next(row);},"QUERY_SPILL_CORRUPT");}
            else{flip(path,104);auto reader=store.open(run);json row;rejects([&]{reader->next(row);},"QUERY_SPILL_CORRUPT");}
        }
        {
            QuerySpillStore store(root.u8string(),identity,120,128);auto writer=store.create();rejects([&]{writer->append(json::array({std::string(100,'x')}));},"QUERY_LIMIT_EXCEEDED");writer.reset();require(store.diagnostics()["live_bytes"]==0,"failed writer cleanup");
            auto empty=store.create();auto run=empty->finish();empty.reset();auto reader=store.open(run);json row;require(!reader->next(row),"empty run");
        }
        {
            QuerySpillStore store(root.u8string(),identity,4096,16);auto writer=store.create();rejects([&]{writer->append(json::array({std::string(50,'x')}));},"QUERY_ROW_TOO_LARGE");
        }
        // Readers retain their owned context even if the facade has gone.
        std::unique_ptr<QuerySpillStore::Reader> held;
        {auto store=std::make_unique<QuerySpillStore>(root.u8string(),identity);auto writer=store->create();writer->append(json::array({7}));auto run=writer->finish();writer.reset();held=store->open(run);store.reset();}
        json row;require(held->next(row)&&row[0]==7,"reader owns context lifetime");held.reset();
        require(fs::exists(root/"must-survive.txt"),"foreign file untouched");
        std::size_t remaining=0;for(auto& entry:fs::directory_iterator(root))if(entry.path().extension()==".tmp")++remaining;
        require(remaining==0,"all owned runs cleaned");fs::remove(root/"must-survive.txt");fs::remove(root);
        std::cout<<"PASS query_spill "<<checks<<" checks\n";return 0;
    }catch(const std::exception& error){std::cerr<<"FAIL query_spill: "<<error.what()<<"\n";return 1;}
}
