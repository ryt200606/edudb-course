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
void independent_domains(const fs::path& dir){
    r::FenceDomain a(uuid,(dir/"os-a").u8string()),b(uuid,(dir/"os-b").u8string()),c(uuid,(dir/"os-c").u8string());
    const std::string key(64,'2');
    r::WriterIdentity a0{original,source,0},b0{replacement,source,0};
    a0.epoch=a.register_primary(a0)["epoch"];
    auto stale=b.offer(b0,9,key), offer=b.offer(b0,10,key);
    error("HANDOFF_NOT_CAUGHT_UP",[&]{a.surrender(a0,10,stale,key);});
    auto changed=offer;changed["target"]=original;
    error("HANDOFF_PROOF",[&]{a.surrender(a0,10,changed,key);});
    {auto lease=a.acquire_writer(a0);}
    auto ticket=a.surrender(a0,10,offer,key);
    check(ticket==a.surrender(a0,10,offer,key),"Surrender retry changed the certificate");
    error("FENCED",[&]{a.acquire_writer(a0);});
    error("FENCED",[&]{a.promote(replacement,source,10);});
    c.offer(b0,10,key);
    error("HANDOFF_TARGET",[&]{c.accept(b0,10,ticket,key);});
    error("HANDOFF_TARGET",[&]{b.accept(b0,9,ticket,key);});
    error("HANDOFF_PROOF",[&]{b.accept(b0,10,ticket,std::string(64,'3'));});
    {auto permit=b.accept(b0,10,ticket,key);check(permit.state()["epoch"]==2,"Bad successor epoch");}
    {auto permit=b.accept(b0,10,ticket,key);check(permit.state()["timeline"]==ticket["timeline"],"Fork advanced twice");}
    r::WriterIdentity b1{replacement,ticket.at("timeline"),2};
    {auto permit=b.acquire_writer(b1);}
    // A second candidate on the former primary OS receives a fresh restore with
    // a new instance id. Transferring back invalidates every previous ticket.
    r::WriterIdentity a1{std::string(32,'a'),b1.timeline,0};
    auto return_offer=a.offer(a1,20,key), back=b.surrender(b1,20,return_offer,key);
    {auto permit=a.accept(a1,20,back,key);check(permit.state()["epoch"]==3,"Return transfer failed");}
    error("FENCE_STALE",[&]{b.accept(b0,10,ticket,key);});
    error("FENCED",[&]{b.accept(b1,20,ticket,key,true);});
    error("FENCED",[&]{b.acquire_writer(b1);});
    {auto local=a.promote(std::string(32,'7'),back.at("timeline"),20);}
    error("FENCE_STALE",[&]{a.accept(a1,20,back,key);});
}
json units(const fs::path& dir){fs::create_directories(dir);r::FenceDomain first(uuid,dir.u8string()),second(uuid,dir.u8string());r::WriterIdentity owner{original,source,0};auto state=first.register_primary(owner);owner.epoch=state["epoch"];check(owner.epoch==1,"first owner epoch");
 {auto held=first.acquire_writer(owner);auto pending=std::async(std::launch::async,[&]{error("FENCE_BUSY",[&]{second.promote(replacement,source,100,50);});});check(pending.wait_for(std::chrono::seconds(2))==std::future_status::ready,"promotion did not honor bounded lease wait");pending.get();}
 check(first.snapshot()["epoch"]==1,"blocked promotion changed epoch");json promoted;{auto lease=second.promote(replacement,source,100);promoted=lease.state();check(promoted["epoch"]==2&&promoted["parent_timeline"]==source&&promoted["fork_lsn"]==100,"promotion did not durably advance lineage");error("FENCE_BUSY",[&]{first.acquire_writer(owner,20);});}
 error("FENCED",[&]{first.acquire_writer(owner);});error("FENCED",[&]{first.register_primary(owner);});r::WriterIdentity next{replacement,promoted["timeline"],2};{auto permit=second.acquire_writer(next);check(permit.state()["owner_instance_id"]==replacement,"new owner missing");}
 {auto retry=second.promote(replacement,source,100);check(retry.state()==promoted,"promotion crash retry changed timeline/epoch twice");}
 error("FENCE_STALE",[&]{second.promote(original,source,101);});
 auto state_path=dir/("control-fence-"+uuid+".json");auto before=std::ifstream(state_path,std::ios::binary);std::string bytes{std::istreambuf_iterator<char>(before),std::istreambuf_iterator<char>()};before.close();{std::ofstream out(state_path,std::ios::binary|std::ios::trunc);out<<bytes.substr(0,bytes.size()/2);}error("FENCE_CORRUPT",[&]{second.acquire_writer(next);});
 fs::remove(state_path);error("FENCE_MISSING",[&]{second.register_primary(next);});
 independent_domains(dir);
 return {{"status","PASS"},{"checks",json::array({"two_handles_share_actual_OS_exclusive_commit_lease","promotion_cannot_pass_inflight_commit","promotion_forces_epoch_owner_timeline_lineage","old_primary_and_restart_fenced","new_owner_and_idempotent_promotion_retry","stale_source_corrupt_missing_state_fail_closed","two_cross_domain_transfers_preserve_ownership_and_reject_old_or_forged_proofs"})},{"scope","Native fencing domains; cross-OS SQL and WAL tested separately"}};
}
}
int main(int argc,char** argv){try{if(argc==1){auto root=fs::absolute(fs::path("fencing-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));std::cout<<units(root).dump(2)<<"\n";return 0;}if(argc==3&&std::string(argv[1])=="--unit"){std::cout<<units(fs::u8path(argv[2])).dump(2)<<"\n";return 0;}throw std::runtime_error("usage --unit NEW_DIRECTORY");}catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 5;}}
