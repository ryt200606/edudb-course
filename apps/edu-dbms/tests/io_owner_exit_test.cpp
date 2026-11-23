#include "io.hpp"
#include <filesystem>
#include <future>
#include <iostream>
namespace {struct ControlledUnwind {};}
int main(){try{
    // The previous dynamic-TLS owner string failed this small worker-exit
    // reproducer with Windows STATUS_HEAP_CORRUPTION on the release toolchain.
    for(unsigned i=0;i<64;++i){
        auto worker=std::async(std::launch::async,[i]{
            try{
                edudb::IoOwnerScope owner(std::filesystem::path(std::string("C:/edudb-owner-")+std::string(160,'x')+std::to_string(i)));
                throw ControlledUnwind{};
            }catch(const ControlledUnwind&){}
        });
        worker.get();
    }
    std::cout<<"PASS 64 short async I/O-owner scope exits\n";return 0;
}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
