// Windows SQL/recovery worker; the parent runner binds current source and linked core SHA.
// All data changes below use actual Engine SQL; native Storage is read-only verification.
#include "edudb.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <thread>
#ifdef _WIN32
#include <windows.h>
#endif
using namespace edudb;
namespace fs=std::filesystem;
using Model=std::map<std::string,std::map<long long,json>>;
static json events=json::array();
static unsigned checks=0;
static void require(bool b,const std::string& m){++checks;if(!b)throw std::runtime_error(m);}
static void write(const fs::path& p,const json& j){std::ofstream f(p,std::ios::binary|std::ios::trunc);f<<j.dump(2);f.flush();if(!f)throw std::runtime_error("receipt write failed");}
static std::string quote(const std::string& s){std::string r="'";for(char c:s){r+=c;if(c=='\'')r+='\'';}return r+"'";}
static std::string scalar(const json& j){if(j.is_null())return "NULL";if(j.is_string())return quote(j.get<std::string>());if(j.is_object()&&j.contains("$blob"))return "X'"+j.at("$blob").get<std::string>()+"'";return j.dump();}
static std::string values(const json& row){std::string s="(";for(std::size_t i=0;i<row.size();++i){if(i)s+=",";s+=scalar(row[i]);}return s+")";}
static std::string blob(unsigned n,unsigned salt=0){static const char* h="0123456789ABCDEF";std::string s;for(unsigned i=0;i<n;++i){unsigned b=(i*73+salt*29)%256;s+=h[b>>4];s+=h[b&15];}return s;}
static json row(long long id,bool large=false){return json::array({id,id%11==0?json(nullptr):json(id%7),id%5==0?json(nullptr):json(id%2!=0),std::string(u8"共同前缀_数据库_中文_")+std::string(100,'p')+std::to_string(id),id==1?json(std::numeric_limits<long long>::min()):id==2?json(std::numeric_limits<long long>::max()):json(id*10000000001LL),large?json{{"$blob",blob(16385)}}:json{{"$blob",""}}});}
static std::vector<std::string> tables(){return {"all_am","org_flat","org_multi","org_bplus","org_static","org_extend","org_linear","rec_fixed","rec_variable"};}
static Model model(bool changed=false){Model m;for(auto& t:tables())for(int i=1;i<=24;++i)m[t][i]=row(i,i==1);if(changed){for(auto& pair:m){auto& rs=pair.second;auto r=rs.at(2);rs.erase(2);r[0]=102;r[1]=99;r[3]=std::string(u8"已提交_组织键与索引键更新");rs[102]=r;for(int i=20;i<=24;++i)rs.erase(i);rs[90]=row(90,true);rs[90][3]=std::string(u8"已提交_新增大对象");}}return m;}
static StorageOptions opts(){StorageOptions o;o.recovery_mode="ARIES_PAGE";o.aries_steal=true;o.aries_force_at_commit=false;return o;}
static json sql(Engine& e,const std::string& sid,const std::string& q){auto r=e.execute(sid,q);events.push_back({{"sql",q},{"response",r}});if(!r.value("ok",false)){const auto& err=r.at("error");throw DbError(err.at("code").get<std::string>(),err.at("message").get<std::string>());}return r;}
struct IndexCase{std::string table,name,method,condition;};
static std::vector<IndexCase> cases(){return {
 {"all_am","ix_bp","BPLUS","k=3"},{"all_am","ix_bstar","BPLUS","id=7"},{"all_am","ix_post","BPLUS","k=3"},
 {"all_am","ix_bt","BTREE","k=3"},{"all_am","ix_dense","ORDERED_DENSE","k=3"},
 {"all_am","ix_static","HASH_STATIC","k=3"},{"all_am","ix_probe","HASH_STATIC","id=7"},
 {"all_am","ix_extend","HASH_EXTENDIBLE","id=7"},{"all_am","ix_linear","HASH_LINEAR","id=7"},
 {"all_am","ix_bitmap","BITMAP","k=3"},{"all_am","ix_hybrid","BITMAP","flag=TRUE"},
 {"all_am","ix_compound","BPLUS","k=3 AND flag=TRUE"},{"all_am","ix_string","BPLUS","tag="+quote(row(7)[3].get<std::string>())},
 {"org_flat","ix_sparse","ORDERED_SPARSE","id>=7 AND id<12"},{"org_multi","ix_multi","ORDERED_SPARSE","id>=7 AND id<12"}
};}
static bool matches(const IndexCase& c,const json& r){if(c.name=="ix_bstar"||c.name=="ix_probe"||c.name=="ix_extend"||c.name=="ix_linear")return r[0]==7;if(c.name=="ix_string")return r[3]==row(7)[3];if(c.name=="ix_sparse"||c.name=="ix_multi")return r[0].get<long long>()>=7&&r[0].get<long long>()<12;if(c.name=="ix_hybrid")return r[2]==true;return r[1]==3&&(c.name!="ix_compound"||r[2]==true);}
static json rows_of(const std::map<long long,json>& m){json j=json::array();for(auto& p:m)j.push_back(p.second);return j;}
static json schema_view(const json& r){return {{"columns",r.at("rows")},{"organization",r.at("organization")},{"record_layout",r.at("record_layout")}};}
static json inspect_index(Engine& e,const std::string& sid,const IndexCase& c){auto r=e.inspect(sid,{{"table",c.table},{"index",c.name},{"max_pages",2048}});events.push_back({{"inspect",{{"table",c.table},{"index",c.name}}},{"response",r}});auto i=r.at("inspection");require(i.at("status")=="PASS","inspection status "+c.name);require(i.at("structure").at("method")==c.method,"physical method "+c.name);require(i.at("structure").value("truncated",false)==false,"inspect all pages "+c.name);require(i.at("structure").at("nodes").get<unsigned>()>0,"actual persistent nodes "+c.name);return i;}
static json check_all(Engine& e,const std::string& sid,const Model& m){json out={{"tables",json::object()},{"indexes",json::object()},{"organizations",json::object()}};
 for(auto& t:tables()){
  auto r=sql(e,sid,"SELECT id,k,flag,tag,big,payload FROM "+t+" USE SCAN ORDER BY id");require(r.at("rows")==rows_of(m.at(t)),"complete typed row bag "+t);auto d=sql(e,sid,"DESCRIBE "+t);out["tables"][t]={{"rows",r.at("rows")},{"schema",schema_view(d)},{"indexes",sql(e,sid,"SHOW INDEXES FROM "+t).at("rows")}};
  auto ins=e.inspect(sid,{{"table",t},{"index","__organization"},{"max_pages",2048}});events.push_back({{"inspect",{{"table",t},{"index","__organization"}}},{"response",ins}});auto view=ins.at("inspection");require(view.at("status")=="PASS","organization inspection "+t);require(view.at("row_count").get<unsigned>()==m.at(t).size(),"org row count "+t);out["organizations"][t]=view;
 }
 for(auto& c:cases()){
  json expected=json::array();for(auto& p:m.at(c.table))if(matches(c,p.second))expected.push_back(json::array({p.second[0],p.second[1],p.second[2],p.second[3],p.second[4]}));
  auto q="SELECT id,k,flag,tag,big FROM "+c.table+" USE INDEX("+c.name+") WHERE "+c.condition+" ORDER BY id";auto r=sql(e,sid,q);require(r.at("rows")==expected,"forced index result "+c.name);auto plan=r.at("plan").at("sources").at(0);require(plan.at("index")==c.name,"actual selected index "+c.name);require(plan.value("index_page_reads",0u)>0||c.method=="ORDERED_SPARSE","actual index access counter "+c.name);out["indexes"][c.name]={{"rows",r.at("rows")},{"plan",plan},{"physical",inspect_index(e,sid,c)}};
 }
 auto nullable=sql(e,sid,"SELECT id FROM all_am USE INDEX(ix_bitmap) WHERE k IS NULL ORDER BY id");json nullids=json::array();for(auto& p:m.at("all_am"))if(p.second[1].is_null())nullids.push_back(json::array({p.first}));require(nullable.at("rows")==nullids,"bitmap NULL result");out["null_bitmap"]=nullable;
 auto v=sql(e,sid,"VERIFY");require(v.at("verification").at("ok")==true,"SQL VERIFY full storage/index/LOB");out["verification"]=v.at("verification");return out;
}
static void mutate(Engine& e,const std::string& sid){for(auto& t:tables()){
 sql(e,sid,"UPDATE "+t+" SET id=102,k=99,tag="+quote(u8"已提交_组织键与索引键更新")+" WHERE id=2");
 sql(e,sid,"DELETE FROM "+t+" WHERE id>=20 AND id<=24");auto r=row(90,true);r[3]=std::string(u8"已提交_新增大对象");sql(e,sid,"INSERT INTO "+t+" VALUES "+values(r));}}
static void seed(Engine& e,const std::string& sid){
 const std::string columns="(id INTEGER PRIMARY KEY,k INTEGER,flag BOOLEAN,tag VARCHAR(512),big INTEGER,payload BLOB(65536))";
 const std::map<std::string,std::string> org={{"all_am",""},{"org_flat"," ORGANIZATION ORDERED(id) WITH(multilevel=FALSE,max_keys=5)"},{"org_multi"," ORGANIZATION ORDERED(id) WITH(multilevel=TRUE,max_keys=5)"},{"org_bplus"," ORGANIZATION BPLUS(id) WITH(max_keys=5)"},{"org_static"," ORGANIZATION HASH_STATIC(id) WITH(buckets=8,max_keys=7)"},{"org_extend"," ORGANIZATION HASH_EXTENDIBLE(id) WITH(max_keys=5,max_depth=12)"},{"org_linear"," ORGANIZATION HASH_LINEAR(id) WITH(buckets=2,max_keys=5)"},{"rec_fixed"," RECORD FORMAT FIXED WITH(blob_ref_bytes=2048)"},{"rec_variable"," RECORD FORMAT VARIABLE"}};
 for(auto& t:tables())sql(e,sid,"CREATE TABLE "+t+columns+org.at(t));
 const std::vector<std::string> defs={"ix_bp ON all_am USING BPLUS(k) WITH(max_keys=5)","ix_bstar ON all_am USING BPLUS(id) WITH(max_keys=5,bstar=TRUE)","ix_post ON all_am USING BPLUS(k) WITH(max_keys=5,dup_layout='posting')","ix_bt ON all_am USING BTREE(k) WITH(max_keys=5)","ix_dense ON all_am USING DENSE(k) WITH(max_keys=5)","ix_static ON all_am USING HASH_STATIC(k) WITH(buckets=8,max_keys=15)","ix_probe ON all_am USING HASH_STATIC(id) WITH(buckets=16,max_keys=7,probing=TRUE)","ix_extend ON all_am USING HASH_EXTENDIBLE(id) WITH(max_keys=5,max_depth=12)","ix_linear ON all_am USING HASH_LINEAR(id) WITH(buckets=2,max_keys=5)","ix_bitmap ON all_am USING BITMAP(k)","ix_hybrid ON all_am USING BITMAP_HYBRID(flag)","ix_compound ON all_am USING BPLUS(k,flag) INCLUDE(tag,big) WITH(max_keys=5)","ix_string ON all_am USING BPLUS(tag) WITH(max_keys=5,prefix_compression=TRUE)","ix_sparse ON org_flat USING SPARSE(id)","ix_multi ON org_multi USING MULTILEVEL(id)"};
 for(auto& t:tables()){sql(e,sid,"BEGIN");for(int i=1;i<=24;++i)sql(e,sid,"INSERT INTO "+t+" VALUES "+values(row(i,i==1)));sql(e,sid,"COMMIT");}
 // Real CREATE INDEX bottom-up paths, followed by actual SQL incremental maintenance.
 for(auto& d:defs)sql(e,sid,"CREATE INDEX "+d);
 // Deliberate supported-boundary checks: errors, no hidden fallback to JSON.
 for(auto& method:std::vector<std::string>{"BPLUS","ORDERED","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR"})for(auto& fmt:std::vector<std::string>{"FIXED","VARIABLE"}){
  auto name="bad_"+method+"_"+fmt;std::transform(name.begin(),name.end(),name.begin(),[](unsigned char c){return char(std::tolower(c));});bool rejected=false;try{sql(e,sid,"CREATE TABLE "+name+"(id INTEGER) ORGANIZATION "+method+"(id) RECORD FORMAT "+fmt);}catch(const DbError& ex){rejected=ex.code=="RECORD_LAYOUT";}require(rejected,"nonHEAP binary combination explicitly rejected "+name);
 }
 bool bad=false;try{sql(e,sid,"CREATE INDEX bad_blob ON all_am USING BPLUS(payload)");}catch(const DbError& ex){bad=ex.code=="UNSUPPORTED_SQL"&&std::string(ex.what())=="BLOB cannot be index key";}require(bad,"BLOB search key rejected by SQL layer");
 auto shown=sql(e,sid,"SHOW TABLES");require(shown.at("rows").size()==tables().size(),"rejected DDL left no table");
}
static json native_lobs(const std::string& dir,bool changed){Storage db(dir,16,32u*1024u*1024u,opts());auto tx=db.begin();json found=json::array();for(auto& t:tables()){
 ScanSpec s;s.lazy_blobs=true;auto c=db.scan(tx,t,s);StoredRow r;unsigned n=0;while(c->next(r)){auto ref=r.values.at(5);if(ref.is_object()&&ref.contains("$lob")){
  if(ref.at("bytes")==0){require(ref.at("$lob").at("count")==0,"empty blob has no chunk "+t);require(db.blob_value(tx,ref)==json{{"$blob",""}},"empty physical blob hydrate "+t);continue;}
  require(ref.at("bytes")==16385,"actual >12KiB LOB length "+t);require(ref.at("$lob").at("count").get<unsigned>()>=6,"actual chunk count >=6 "+t);auto full=db.blob_value(tx,ref);require(full==json{{"$blob",blob(16385)}},"full physical blob hydrate "+t);auto partial=db.blob_value(tx,ref,3070,12);require(partial==json{{"$blob",blob(16385).substr(3069*2,24)}},"cross3072 chunk substring "+t);found.push_back({{"table",t},{"id",r.values.at(0)},{"reference",ref}});++n;
 }}require(n==(changed?2u:1u),"LOB count "+t);
 }db.rollback(tx);return found;}
int main(int argc,char** argv){std::string mode=argc>1?argv[1]:"",dir=argc>2?argv[2]:"",receipt=argc>3?argv[3]:"result.json";try{
 require(!dir.empty(),"directory supplied");json result={{"status","RUNNING"},{"mode",mode},{"directory",dir},{"build_binding","exact current core SHA is recorded by the parent runner"},{"scope","same-source integrated SQL access/organization/ARIES worker; parent receipt defines acceptance scope"}};
 if(mode=="seed"||mode=="seed-crash"){
  require(!fs::exists(fs::u8path(dir)),"seed directory must not exist");{Engine e(dir,16,32u*1024u*1024u,opts());auto sid=e.open_session();seed(e,sid);result["baseline"]=check_all(e,sid,model());
   if(mode=="seed"){
    sql(e,sid,"BEGIN");sql(e,sid,"SAVEPOINT original");mutate(e,sid);result["savepoint_changed"]=check_all(e,sid,model(true));sql(e,sid,"ROLLBACK TO original");result["savepoint_restored"]=check_all(e,sid,model());sql(e,sid,"RELEASE SAVEPOINT original");sql(e,sid,"COMMIT");
    sql(e,sid,"BEGIN");mutate(e,sid);sql(e,sid,"ROLLBACK");result["full_rollback"]=check_all(e,sid,model());
   }result["diagnostics"]=e.diagnostics();e.close_session(sid);
  }result["physical_lobs"]=native_lobs(dir,false);
 }else if(mode=="recover-base"||mode=="recover-commit"){
  bool changed=mode=="recover-commit";{Engine e(dir,16,32u*1024u*1024u,opts());result["startup_diagnostics"]=e.diagnostics();auto sid=e.open_session();result["snapshot"]=check_all(e,sid,model(changed));result["diagnostics"]=e.diagnostics();e.close_session(sid);}result["physical_lobs"]=native_lobs(dir,changed);
 }else if(mode=="hold-commit"||mode=="hold-loser"){
  Engine e(dir,16,32u*1024u*1024u,opts());auto sid=e.open_session();auto before=e.diagnostics();sql(e,sid,"BEGIN");mutate(e,sid);if(mode=="hold-commit")sql(e,sid,"COMMIT");auto diag=e.diagnostics();require(diag.at("storage").at("steal_flushes").get<unsigned>()>before.at("storage").at("steal_flushes").get<unsigned>(),"actual STEAL dirty writes before crash");result["diagnostics"]=diag;result["pid"]=GetCurrentProcessId();result["checks"]=checks;result["events"]=events;result["status"]="READY_FOR_EXTERNAL_TERMINATE";write(fs::u8path(receipt),result);for(int n=0;n<120;++n)std::this_thread::sleep_for(std::chrono::seconds(1));throw std::runtime_error("external kill did not arrive within120s");
 }else throw std::runtime_error("unknown mode");
 result["checks"]=checks;result["events"]=events;result["status"]="PASS";write(fs::u8path(receipt),result);std::cout<<"PASS "<<mode<<" checks="<<checks<<"\n";return 0;
 }catch(const std::exception& e){json fail={{"status","FAIL"},{"mode",mode},{"error",e.what()},{"checks",checks},{"events",events}};if(auto p=dynamic_cast<const DbError*>(&e))fail["code"]=p->code;try{write(fs::u8path(receipt),fail);}catch(...){}std::cerr<<"FAIL "<<mode<<" "<<e.what()<<"\n";return 1;}}
