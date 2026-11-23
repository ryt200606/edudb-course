#include "aries_recovery.hpp"
#include "managed_block_device.hpp"
#include "edudb.hpp"
#include <filesystem>
#include <chrono>
#include <iostream>
using namespace edudb;namespace a=edudb::aries;namespace fs=std::filesystem;
namespace {
void check(bool yes,const std::string& message){if(!yes)throw std::runtime_error(message);}
template<class F>void error(const std::string& code,F f){try{f();}catch(const DbError& e){check(e.code==code,"expected "+code+", got "+e.code+": "+e.what());return;}throw std::runtime_error("missing "+code);}
a::Page image(std::uint64_t id,unsigned value){auto c=a::default_page_codec();auto p=c.empty(id);p[64]=static_cast<unsigned char>(value);c.stamp(p,0);return p;}
}
int main(int argc,char** argv){try{check(argc==1||argc==2,"usage [NEWROOT]");auto root=argc==2?fs::u8path(argv[1]):fs::temp_directory_path()/("edudb-logical-reservation-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));check(!fs::exists(root),"new isolated test root required");fs::create_directories(root);auto data=a::open_native_data_device((root/"data.pages").u8string());const std::string uuid="0123456789abcdef0123456789abcdef";json checks=json::array();std::uint64_t spent=0;
 {a::Options o;o.wal_limit_bytes=131072;a::Manager m(root.u8string(),*data,uuid,a::default_page_codec(),o);
  m.set_external_reservation(65536);auto before=m.log_space();m.begin(1);m.update(1,1,image(1,10));auto lsn=m.diagnostics()["last_lsn"];
  error("ARIES_LOG_LIMIT",[&]{m.update(1,2,image(2,20));});check(m.diagnostics()["last_lsn"]==lsn,"rejected normal update appended records");error("ARIES_BUSY",[&]{m.set_external_reservation(0);});m.abort(1);check(m.read(0,1)[64]==0&&m.log_space().reserved==before.reserved,"normal abort consumed logical reservation");checks.push_back("normal_update_admission_and_its_real_CLR_preserve_external_credit");
  error("COMPENSATION_CREDIT",[&]{m.begin_compensation(2,70000,16544);});error("COMPENSATION_CREDIT",[&]{m.begin_compensation(2,32768,1);});auto start=m.log_space();m.begin_compensation(2,32768,16544);m.update(2,1,image(1,99));auto after_update=m.diagnostics()["last_lsn"];error("COMPENSATION_CREDIT",[&]{m.update(2,2,image(2,44));});check(m.diagnostics()["last_lsn"]==after_update,"compensation bound error modified WAL");m.abort(2);spent=m.transaction_record_bytes(2);auto end=m.log_space();check(end.used-start.used==spent&&start.reserved-end.reserved==spent,"credit accounting not bound to actual BEGIN/UPDATE/ABORT/CLR/END bytes");check(end.used+end.reserved==start.used+start.reserved,"WAL reservation invariant changed");check(m.read(0,1)[64]==0&&!m.transaction_committed(2),"aborted compensation left row or false COMMIT evidence");checks.push_back("real_compensation_uses_only_reserved_credit_with_update_and_CLR_bounds");
  m.begin_compensation(3,32768,16544);m.update(3,1,image(1,33));auto result=m.commit(3);check(result["status"]=="COMMITTED"&&m.transaction_committed(3),"durable opId mini-transaction COMMIT evidence missing");check(m.transaction_record_bytes(3)==16544+160,"COMMIT evidence byte accounting");checks.push_back("durable_COMMIT_evidence_and_transaction_byte_query");
  error("UNDO_RESERVATION",[&]{m.set_external_reservation(131072);});m.set_external_reservation(0);check(m.log_space().reserved==0,"reservation release failed");checks.push_back("overreservation_rejected_and_unused_credit_released");
 }
 {a::Options o;o.wal_limit_bytes=131072;a::Manager reopened(root.u8string(),*data,uuid,a::default_page_codec(),o);check(reopened.transaction_committed(3)&&reopened.transaction_record_bytes(2)==spent,"restart lost persisted mini-transaction evidence");check(reopened.read(0,1)[64]==33,"restart lost committed compensation page");auto remaining=65536-reopened.transaction_record_bytes(2)-reopened.transaction_record_bytes(3);reopened.set_external_reservation(remaining);check(reopened.log_space().reserved==remaining,"reservation cannot be reconstructed from physical WAL evidence");checks.push_back("native_restart_reconstructs_spent_credit_and_committed_opId");}
 std::cout<<json{{"status","PASS"},{"checks",checks},{"scope","Windows native physical ARIES reservation primitives; actual logical SQL is validated by separate tests"}}.dump(2)<<"\n";return 0;
 }catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 5;}}
