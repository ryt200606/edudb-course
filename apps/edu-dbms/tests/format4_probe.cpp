#include "edudb.hpp"
#include "format_copy.hpp"
#include "interchange.hpp"
#include <filesystem>
#include <iostream>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#include <shellapi.h>
#endif
using namespace edudb;namespace fs=std::filesystem;
int main(int argc,char** argv){std::vector<std::string> utf8;std::vector<char*> arguments;
#ifdef _WIN32
int count=0;auto wide=CommandLineToArgvW(GetCommandLineW(),&count);if(wide){for(int i=0;i<count;++i){auto n=WideCharToMultiByte(CP_UTF8,0,wide[i],-1,nullptr,0,nullptr,nullptr);std::string text(static_cast<std::size_t>(n),0);WideCharToMultiByte(CP_UTF8,0,wide[i],-1,text.data(),n,nullptr,nullptr);text.pop_back();utf8.push_back(std::move(text));}LocalFree(wide);for(auto& a:utf8)arguments.push_back(a.data());argc=count;argv=arguments.data();}SetConsoleOutputCP(CP_UTF8);
#endif
try{if(argc!=3)throw DbError("ARGUMENT","probe --inspect|--seed-enhanced DIR");const auto path=fs::u8path(argv[2]);json out={{"status","PASS"}};
{Engine e(path.u8string(),3);auto sid=e.open_session();auto run=[&](const std::string& sql){auto r=e.execute(sid,sql);if(!r.value("ok",false))throw DbError("PROBE_SQL",r.dump());return r;};
if(std::string(argv[1])=="--seed-enhanced"){run("CREATE CLUSTER c");run("CREATE TABLE typed(id INTEGER PRIMARY KEY,k INTEGER,n VARCHAR(16)) RECORD FORMAT FIXED");run("INSERT INTO typed VALUES(1,7,'新格式')");run("ALTER TABLE typed CLUSTER c(k)");out["inspection"]=run("INSPECT CLUSTER c")["inspection"];}
else if(std::string(argv[1])=="--inspect"){out["tables"]=json::object();auto names=run("SHOW TABLES");for(const auto& row:names["rows"]){auto name=row[0].get<std::string>();auto quoted=interchange::quote_identifier(name);out["tables"][name]={{"query",run("SELECT * FROM "+quoted)},{"describe",run("DESCRIBE "+quoted)},{"indexes",run("SHOW INDEXES FROM "+quoted)}};}auto d=e.diagnostics();out["storage"]=d["storage"];if(d["storage"].value("read_only",false)){out["write_rejections"]=json::array();for(const auto* text:{"CREATE TABLE forbidden(k INTEGER)","CREATE CLUSTER forbidden","CHECKPOINT","ANALYZE \"演示表\"","DELETE FROM \"演示表\" WHERE id=-1","UPDATE \"演示表\" SET k=0 WHERE id=-1"}){auto r=e.execute(sid,text);if(r.value("ok",true)||r["error"]["code"]!="READ_ONLY_DATABASE")throw DbError("PROBE_READONLY",r.dump());out["write_rejections"].push_back(r["error"]);}auto exported=e.export_data(sid,{{"format","sql"}});if(!exported.value("ok",false))throw DbError("PROBE_EXPORT",exported.dump());out["export_sql_sha256"]=format_copy::sha256(exported["content"].get<std::string>());out["export_sql_bytes"]=exported["bytes"];}}
else throw DbError("ARGUMENT","Unknown probe command");e.close_session(sid);}
out["control"]=format_copy::read_control(path/"control.json");std::cout<<out.dump()<<"\n";return 0;
}catch(const DbError& e){std::cerr<<e.code<<": "<<e.what()<<"\n";return 1;}catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 2;}}
