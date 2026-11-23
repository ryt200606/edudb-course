#include <chrono>
#include "io.hpp"
#include "../src/media_native.hpp"
#include <filesystem>
#include <iostream>
#include <thread>
using namespace edudb;namespace fs=std::filesystem;
void check(bool yes,const std::string& text){if(!yes)throw std::runtime_error(text);}
void native(const fs::path& path){media_detail::File file(path,true,true);const unsigned char b=7;file.write(0,&b,1);file.sync();}
int main(int argc,char** argv){try{check(argc==1||argc==2,"usage [NEW_DIRECTORY]");auto root=argc==1?fs::absolute(fs::path("io-owner-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))):fs::absolute(fs::u8path(argv[1]));auto a=root/"db-a",b=root/"db-b",shared=root/"shared";fs::create_directories(a);fs::create_directories(b);fs::create_directories(shared);io_reset(a);io_reset(b);io_reset(shared);IoScope request("actual I/O owner nesting/thread isolation");{
 IoOwnerScope first(a/"child/..");native(shared/"control-a-first.json");
 try{IoOwnerScope second(b);native(shared/"control-b.json");throw std::runtime_error("controlled unwind");}catch(const std::runtime_error&){}
 native(shared/"control-a-restored.json");
 std::thread worker([&]{native(shared/"control-worker.json");});worker.join();
 try{IoOwnerScope invalid(fs::path{});throw std::runtime_error("empty owner unexpectedly accepted");}catch(const DbError& e){check(e.code=="ARGUMENT","wrong empty owner code");}
 native(shared/"control-a-after-rejection.json");
 }native(shared/"control-without-owner.json");
 auto av=io_snapshot(a),bv=io_snapshot(b),sv=io_snapshot(shared),trace=request.snapshot();
 check(av["files"]["control"]["write_calls"]==3&&av["files"]["control"]["sync_calls"]==3,"owner a totals");
 check(bv["files"]["control"]["write_calls"]==1&&bv["files"]["control"]["sync_calls"]==1,"nested owner totals");
 check(sv["files"]["control"]["write_calls"]==2&&sv["files"]["control"]["sync_calls"]==2,"owner leaked across thread or past scope");
 check(trace["trace_total"]==10,"request scope captured another thread or lost operations");
 bool actual=false,worker=false;for(const auto& item:trace["trace"]){actual=actual||item["file"]=="control-a-first.json";worker=worker||item["file"]=="control-worker.json";check(item["role"]=="control","actual role changed by owner");}check(actual&&!worker,"trace path rewritten or wrong thread included");
 std::cout<<json{{"status","PASS"},{"checks",json::array({"actual_native_cross_directory_owner","nested_owner_unwind_restoration","normalization_matches_io_snapshot","thread_local_no_cross_thread_inheritance","scope_exit_and_invalid_constructor_preserve_prior_owner","request_trace_keeps_real_file_names_and_roles"})},{"owner_a",av},{"owner_b",bv},{"physical_directory",sv},{"request",trace}}.dump(2)<<"\n";return 0;
 }catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 1;}}
