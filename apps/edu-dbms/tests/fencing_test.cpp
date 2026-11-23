#include <chrono>
#include "fencing.hpp"
#include "edudb.hpp"
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
using namespace edudb;namespace r=edudb::recovery;namespace fs=std::filesystem;
namespace {
void check(bool ok,const std::string& text){if(!ok)throw std::runtime_error(text);}
template<class F>void error(const std::string& code,F callback){try{callback();}catch(const DbError& e){check(e.code==code,"expected "+code+", got "+e.code+": "+e.what());return;}throw std::runtime_error("missing "+code);}
const std::string uuid="31313131313131313131313131313131",source="abababababababababababababababab",original="12121212121212121212121212121212",replacement="34343434343434343434343434343434";
json units(const fs::path& dir){fs::create_directories(dir);r::FenceDomain first(uuid,dir.u8string()),second(uuid,dir.u8string());r::WriterIdentity owner{original,source,0};auto state=first.register_primary(owner);owner.epoch=state["epoch"];check(owner.epoch==1,"first owner epoch");
 {auto held=first.acquire_writer(owner);auto pending=std::async(std::launch::async,[&]{error("FENCE_BUSY",[&]{second.promote(replacement,source,100,50);});});check(pending.wait_for(std::chrono::seconds(2))==std::future_status::ready,"promotion did not honor bounded lease wait");pending.get();}
 check(first.snapshot()["epoch"]==1,"blocked promotion changed epoch");json promoted;{auto lease=second.promote(replacement,source,100);promoted=lease.state();check(promoted["epoch"]==2&&promoted["parent_timeline"]==source&&promoted["fork_lsn"]==100,"promotion did not durably advance lineage");error("FENCE_BUSY",[&]{first.acquire_writer(owner,20);});}
 error("FENCED",[&]{first.acquire_writer(owner);});error("FENCED",[&]{first.register_primary(owner);});r::WriterIdentity next{replacement,promoted["timeline"],2};{auto permit=second.acquire_writer(next);check(permit.state()["owner_instance_id"]==replacement,"new owner missing");}
 {auto retry=second.promote(replacement,source,100);check(retry.state()==promoted,"promotion crash retry changed timeline/epoch twice");}
 error("FENCE_STALE",[&]{second.promote(original,source,101);});
 auto state_path=dir/("control-fence-"+uuid+".json");auto before=std::ifstream(state_path,std::ios::binary);std::string bytes{std::istreambuf_iterator<char>(before),std::istreambuf_iterator<char>()};before.close();{std::ofstream out(state_path,std::ios::binary|std::ios::trunc);out<<bytes.substr(0,bytes.size()/2);}error("FENCE_CORRUPT",[&]{second.acquire_writer(next);});
 fs::remove(state_path);error("FENCE_MISSING",[&]{second.register_primary(next);});
 return {{"status","PASS"},{"checks",json::array({"two_handles_share_actual_OS_exclusive_commit_lease","promotion_cannot_pass_inflight_commit","promotion_forces_epoch_owner_timeline_lineage","old_primary_and_restart_fenced","new_owner_and_idempotent_promotion_retry","stale_source_corrupt_missing_state_fail_closed"})},{"scope","Native same-machine fencing primitive; actual SQL integration tested separately"}};
}
}
int main(int argc,char** argv){try{if(argc==1){auto root=fs::absolute(fs::path("fencing-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));std::cout<<units(root).dump(2)<<"\n";return 0;}if(argc==3&&std::string(argv[1])=="--unit"){std::cout<<units(fs::u8path(argv[2])).dump(2)<<"\n";return 0;}throw std::runtime_error("usage --unit NEW_DIRECTORY");}catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 5;}}
