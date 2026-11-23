#include "edudb.hpp"

#include <chrono>

#include <filesystem>

#include <fstream>

#include <iostream>

#include <vector>

using namespace edudb;namespace fs=std::filesystem;

namespace {

json events=json::array();

void need(bool value,const std::string& message){if(!value)throw std::runtime_error(message);}

json sql(Engine& db,const std::string& s,const std::string& text){auto value=db.execute(s,text);events.push_back({{"sql",text},{"response",value}});need(value.value("ok",false),text+": "+value.dump());return value;}

json denied(Engine& db,const std::string& s,const std::string& text,const std::string& code){auto value=db.execute(s,text);events.push_back({{"sql",text},{"response",value}});need(!value.value("ok",true)&&value.at("error").at("code")==code,text+": expected "+code+", actual "+value.dump());return value;}

template<class F>void error(const std::string& code,F fn){try{fn();}catch(const DbError& e){need(e.code==code,"native expected "+code+", actual "+e.code+": "+e.what());return;}throw std::runtime_error("native missing "+code);}

StorageOptions options(const fs::path& path,const std::string& mode){StorageOptions o;o.recovery_mode=mode;o.fencing_directory=(path/"fence").u8string();return o;}

std::string contents(const fs::path& path){std::ifstream stream(path,std::ios::binary);need(bool(stream),"cannot read owned control");return {std::istreambuf_iterator<char>(stream),std::istreambuf_iterator<char>()};}

json wanted(){return json::array({json::array({1,10}),json::array({2,20})});}

json data(Engine& db,const std::string& session){return sql(db,session,"SELECT id,v FROM t ORDER BY id").at("rows");}

void seed(const fs::path& path,const std::string& mode){Engine db((path/"db").u8string(),4,32u*1024u*1024u,options(path,mode));auto s=db.open_session();sql(db,s,"CREATE TABLE t(id INTEGER PRIMARY KEY,parent INTEGER,v INTEGER)");sql(db,s,"INSERT INTO t VALUES(1,NULL,10),(2,1,20)");need(data(db,s)==wanted(),"seed rows");}

void begin_read(Engine& db,const std::string& s,const std::string& protocol){sql(db,s,"BEGIN PROTOCOL "+protocol);if(protocol=="TREE"){sql(db,s,"TREE OPEN t PARENT parent");sql(db,s,"TREE LOCK 1");}need(sql(db,s,"SELECT v FROM t WHERE id=1").at("rows")==json::array({json::array({10})}),"protocol read actual row");}

json units(const fs::path& root){json checks=json::array();

 const auto redo=root/"redo",page=root/"page",logical=root/"logical";seed(redo,"REDO_PRIVATE");seed(page,"ARIES_PAGE");seed(logical,"ARIES_LOGICAL");

 {Engine db((redo/"db").u8string(),4,32u*1024u*1024u,options(redo,""));auto a=db.open_session(),b=db.open_session();const std::vector<std::string> protocols={"MVTO","MV2PL","TREE"};

  for(std::size_t i=0;i<protocols.size();++i){begin_read(db,a,protocols[i]);need(db.diagnostics().at("storage").at("transactions").at("protocol_epoch")==protocols[i],"actual protocol epoch selected");auto next=protocols[(i+1)%protocols.size()];auto refusal=denied(db,b,"BEGIN PROTOCOL "+next,"PROTOCOL_MODE_BUSY");need(refusal.at("transaction_state")=="idle","failed incompatible BEGIN must not keep new transaction");sql(db,a,"COMMIT");sql(db,b,"BEGIN PROTOCOL "+next);sql(db,b,"ROLLBACK");}

  need(data(db,a)==wanted(),"protocol epoch transitions changed committed data");checks.push_back({{"case","REDO_PRIVATE_actual_MVTO_MV2PL_TREE_epochs_and_incompatible_BEGIN_preserve_data"},{"status","PASS"}});}

 {Engine db((page/"db").u8string(),4,32u*1024u*1024u,options(page,""));auto s=db.open_session();for(const std::string p:{"MVTO","MV2PL","TREE"})denied(db,s,"BEGIN PROTOCOL "+p,"ARIES_PROTOCOL");denied(db,s,"SET LOCK GRANULARITY RECORD","ARIES_RECORD_UNSUPPORTED");sql(db,s,"BEGIN PROTOCOL STRICT_2PL");sql(db,s,"UPDATE t SET v=11 WHERE id=1");sql(db,s,"ROLLBACK");need(data(db,s)==wanted(),"PAGE Strict rollback after rejected combinations");checks.push_back({{"case","ARIES_PAGE_rejects_MVTO_MV2PL_TREE_RECORD_then_Strict_real_DML_undo"},{"status","PASS"}});}

 {Engine db((logical/"db").u8string(),4,32u*1024u*1024u,options(logical,""));auto s=db.open_session();sql(db,s,"SET LOCK GRANULARITY TABLE");for(const std::string p:{"MVTO","MV2PL","TREE"})denied(db,s,"BEGIN PROTOCOL "+p,"ARIES_PROTOCOL");

  // TABLE makes the explicit Logical-mode guard observable, not just RECORD rejection.

  sql(db,s,"SET LOCK GRANULARITY TABLE");auto before=db.diagnostics().at("storage").at("active_transactions");auto rejected=db.execute_shared_batch(s,{"SELECT id,v FROM t ORDER BY id"});need(!rejected.value("ok",true)&&rejected.at("error").at("code")=="BATCH_TRANSACTION_UNSUPPORTED"&&rejected.at("transaction_state")=="idle","Logical idle batch must fail before owning transaction");need(db.diagnostics().at("storage").at("active_transactions")==before,"rejected idle Logical batch leaked transaction");

  sql(db,s,"SET LOCK GRANULARITY RECORD");sql(db,s,"BEGIN PROTOCOL STRICT_2PL");sql(db,s,"UPDATE t SET v=99 WHERE id=1");auto active=db.execute_shared_batch(s,{"SELECT id,v FROM t"});need(!active.value("ok",true)&&active.at("error").at("code")=="TRANSACTION_ACTIVE"&&active.at("transaction_state")=="active","Logical active caller rejected without rollback");need(sql(db,s,"SELECT v FROM t WHERE id=1").at("rows")==json::array({json::array({99})}),"batch rejection destroyed caller's real pending update");sql(db,s,"ROLLBACK");need(data(db,s)==wanted(),"Logical caller rollback restored original rows");need(db.diagnostics().at("storage").at("transactions").at("page_version_graph").at("disabled"),"Logical mode must not advertise committed physical-CSN graph");checks.push_back({{"case","ARIES_LOGICAL_Strict_only_and_batch_precheck_preserves_idle_and_active_caller"},{"status","PASS"}});}

 for(const auto& pair:std::vector<std::pair<fs::path,std::string>>{{redo,"REDO_PRIVATE"},{page,"ARIES_PAGE"}}){Engine db((pair.first/"db").u8string(),4,32u*1024u*1024u,options(pair.first,""));auto s=db.open_session();const std::vector<std::string> queries={"SELECT id,v FROM t ORDER BY id","SELECT COUNT(*),SUM(v) FROM t"};json standalone=json::array();for(const auto& q:queries)standalone.push_back(sql(db,s,q).at("rows"));auto batch=db.execute_shared_batch(s,queries);need(batch.value("ok",false)&&batch.at("transaction_state")=="idle"&&batch.at("results").size()==queries.size(),"allowed batch did not complete idle: "+batch.dump());need(batch.at("transaction_options").at("read_only")==true&&batch.at("transaction_options").at("protocol")=="STRICT_2PL","batch effective readonly Strict");for(std::size_t i=0;i<queries.size();++i)need(batch.at("results").at(i).at("rows")==standalone[i],"batch and ordinary SQL actual typed rows differ");need(batch.at("metrics").at("shared_scan").at("base_scan_opens")==1,"batch did not use one actual base cursor");auto mutation=db.execute_shared_batch(s,{"SELECT id FROM t","INSERT INTO t VALUES(3,1,30)"});need(!mutation.value("ok",true)&&mutation.at("error").at("code")=="BATCH_QUERY_UNSUPPORTED"&&mutation.at("transaction_state")=="idle","batch readonly API accepted mutation");need(data(db,s)==wanted(),"refused batch mutation changed rows");checks.push_back({{"case",pair.second+"_shared_batch_actual_readonly_results_match_ordinary_SQL"},{"status","PASS"}});}

 for(const auto& pair:std::vector<std::pair<fs::path,std::string>>{{page,"ARIES_PAGE"},{logical,"ARIES_LOGICAL"}}){Engine db((pair.first/"db").u8string(),4,32u*1024u*1024u,options(pair.first,""));auto s=db.open_session();error("BACKUP_MODE",[&]{db.backup("denied",20);});error("BACKUP_MODE",[&]{db.archive_wal();});error("BACKUP_MODE",[&]{db.set_archiving(true,20);});need(!fs::exists(pair.first/"db/backups/denied"),"rejected backup created a partial destination");need(data(db,s)==wanted(),"ARIES backup/archive rejections disturbed live data");checks.push_back({{"case",pair.second+"_backup_archive_configuration_explicitly_rejected_without_data_change"},{"status","PASS"}});}

 {for(const auto& pair:std::vector<std::pair<fs::path,std::string>>{{redo,"REDO_PRIVATE"},{page,"ARIES_PAGE"},{logical,"ARIES_LOGICAL"}}){const auto path=pair.first/"db/control.json";auto saved=contents(path);auto control=json::parse(saved);need(control.at("version")==4&&control.at("page_format")==3&&control.at("minimum_reader_format")==4&&control.at("recovery_mode")==pair.second,"exact control4/page3 durable mode boundary");auto conflicting=pair.second=="REDO_PRIVATE"?"ARIES_PAGE":"REDO_PRIVATE";error("RECOVERY_MODE",[&]{Engine bad((pair.first/"db").u8string(),4,32u*1024u*1024u,options(pair.first,conflicting));});need(contents(path)==saved,"mode conflict rewrote source control");Engine db((pair.first/"db").u8string(),4,32u*1024u*1024u,options(pair.first,""));auto s=db.open_session();need(db.diagnostics().at("storage").value("recovery_mode",std::string("REDO_PRIVATE"))==pair.second,"empty recovery option did not inherit durable mode");need(data(db,s)==wanted(),"reopen or rejected mode switch changed row fingerprint");need(sql(db,s,"VERIFY").at("verification").at("ok"),"mode reopen VERIFY");}checks.push_back({{"case","format4_three_persistent_recovery_modes_inherit_and_conflicting_inplace_switch_refused"},{"status","PASS"}});}

 need(checks.size()==8,"matrix expected8 observable composition cases");return {{"status","PASS"},{"checks",checks},{"events",events},{"scope","Small actual Engine SQL protocol/recovery/batch composition matrix, isolated native files; no runtime performance, power-loss or Linux claim"}};

}

}

int main(int argc,char** argv){try{need(argc==1||argc==2,"usage [NEWROOT]");auto root=argc==2?fs::u8path(argv[1]):fs::absolute(fs::path("protocol-recovery-matrix-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));need(!fs::exists(root),"matrix root must be new");std::cout<<units(root).dump(2)<<'\n';return 0;}catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()},{"events",events}}.dump()<<'\n';return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()},{"events",events}}.dump()<<'\n';return 5;}}

