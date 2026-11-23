#include "edudb.hpp"
#include "fencing.hpp"
#include "io.hpp"
#include "execution_progress.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
using namespace edudb;
namespace fs=std::filesystem;
namespace {
int checks=0;
void check(bool b,const std::string& why){++checks;if(!b)throw std::runtime_error(why);}
template<class F>void error(const char* code,F f){try{f();}catch(const DbError& e){check(e.code==code,std::string("expected ")+code+", actual "+e.code);return;}throw std::runtime_error(std::string("missing ")+code);}
json definition(int n){json t={{"columns",json::array({{{"name","id"},{"type","INTEGER"}},{{"name","v"},{"type","VARCHAR"},{"length",1000}}})},{"indexes",json::array({{{"name","ix"},{"column","id"},{"unique",true},{"options",{{"max_keys",5}}}}})},{"rows",json::array()}};for(int i=1;i<=n;++i)t["rows"].push_back(json::array({i,std::string(500,'x')}));return t;}
StorageOptions options(const fs::path& root,const std::string& mode){StorageOptions o;o.recovery_mode=mode;o.fencing_directory=(root/"fence").u8string();return o;}
void takeover(const fs::path& root,const json& id){recovery::FenceDomain domain(id.at("uuid"),(root/"fence").u8string());auto permit=domain.promote(std::string(32,'b'),id.at("timeline"),id.at("durable_lsn"));(void)permit;}
void probe_count(const fs::path& root){Storage db((root/"db").u8string(),4,32u*1024u*1024u,options(root,"REDO_PRIVATE"));auto tx=db.begin();db.write_table(tx,"t",definition(0));db.commit(tx);tx=db.begin();json observed;
 {IoScope measured("actual private write operation admission");db.write_table(tx,"t",definition(300));observed=measured.snapshot();}
 auto stats=db.transaction_stats(tx);check(stats["private_pages"].get<std::size_t>()>=20,"fixture did not modify many real private pages");const auto reads=observed["files"]["control"]["read_calls"].get<std::uint64_t>();check(reads>=1&&reads<8,"private pages repeated native ownership probes instead of reusing one operation admission");check(db.read_table(tx,"t")["rows"].size()==300,"actual bulk row values");db.commit(tx);auto r=db.begin();check(db.verify(r)["ok"],"bulk operation verification");db.rollback(r);std::cout<<json{{"case","actual_native_control_IO_not_timing"},{"private_pages",stats["private_pages"]},{"control_read_calls",reads}}.dump()<<'\n';}
void unwind_and_future(const fs::path& root,const std::string& mode){Storage db((root/"db").u8string(),4,32u*1024u*1024u,options(root,mode));auto tx=db.begin();db.write_table(tx,"t",definition(0));db.commit(tx);auto id=db.recovery_identity();tx=db.begin();error("INVALID_TABLE",[&]{db.write_table(tx,"t",json::object());});db.write_table(tx,"t",definition(1));db.rollback(tx);tx=db.begin();check(db.read_table(tx,"t")["rows"].empty(),"unwind/rollback leaked a partial row");takeover(root,id);error("FENCED",[&]{db.insert_row(tx,"t",json::array({99,"future native write"}));});db.rollback(tx);check(!db.recovery_required(),"ordinary ownership loss poisoned local read/recovery state");}
void nested_fresh(const fs::path& root,const std::string& mode){Storage db((root/"db").u8string(),4,32u*1024u*1024u,options(root,mode));auto tx=db.begin();db.write_table(tx,"t",definition(0));db.commit(tx);auto id=db.recovery_identity();tx=db.begin();bool entered=false,nested_rejected=false;
 {PageProgressScope callback([&]{if(entered)return;entered=true;takeover(root,id);try{db.insert_row(tx,"t",json::array({99,"nested future write"}));}catch(const DbError& e){nested_rejected=e.code=="FENCED";}});error("FENCED",[&]{db.write_table(tx,"t",definition(1));});}
 check(entered&&nested_rejected,"new nested public mutation reused outer admission after promotion");db.rollback(tx);auto r=db.begin();check(db.read_table(r,"t")["rows"].empty(),"fenced in-flight private work became committed");db.rollback(r);std::cout<<json{{"case","actual_promotion_during_public_operation_and_nested_API"},{"mode",mode},{"nested_rejected",nested_rejected}}.dump()<<'\n';}
}
int main(){try{auto root=fs::absolute(fs::path("write-scope-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));probe_count(root/"io");for(const auto& mode:{std::string("REDO_PRIVATE"),std::string("ARIES_PAGE")}){unwind_and_future(root/(mode+"-unwind"),mode);nested_fresh(root/(mode+"-nested"),mode);}std::cout<<"PASS native operation admission scopes "<<checks<<" checks\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
