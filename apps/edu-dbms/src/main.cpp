#include "edudb.hpp"
#include "input_limits.hpp"
#include "interchange.hpp"
#include "recovery_archive.hpp"
#include "standby_transport.hpp"
#include "input_limits.hpp"
#include "httplib.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <csignal>
#include <cctype>
#include <algorithm>
#include <cerrno>
#include <limits>
#ifdef _WIN32
#include <windows.h>
#include <shellapi.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif
using namespace edudb;
namespace fs = std::filesystem;
static httplib::Server* running = nullptr;
static void stop_server(int) { if (running) running->stop(); }
static json wire_result(json r) {
    if (r.contains("rows")) for (auto& row : r["rows"]) for (auto& v : row)
        if (v.is_number_integer() || v.is_number_unsigned()) v = v.dump();
    return r;
}
static void send(httplib::Response& res, const json& data, int status = 200) {
    res.status = status; res.set_content(data.dump(), "application/json; charset=utf-8");
    res.set_header("Cache-Control", "no-store");
    res.set_header("X-Content-Type-Options", "nosniff");
}
static json request_json(const httplib::Request& r) {
    return input::parse_json(r.body);
}
static constexpr std::size_t transfer_limit=64u*1024u*1024u;
static std::string read_transfer_file(const std::string& file) {
    auto path=fs::u8path(file);
    if(!fs::is_regular_file(path))throw DbError("ARGUMENT","Input must be a regular file");
    if(fs::file_size(path)>transfer_limit)throw DbError("REQUEST_TOO_LARGE","Input exceeds 64 MiB");
    std::ifstream in(path,std::ios::binary);if(!in)throw DbError("IO_ERROR","Cannot open input file");
    std::string content;char buffer[65536];
    while(in){in.read(buffer,sizeof(buffer));const auto count=static_cast<std::size_t>(in.gcount());if(count>transfer_limit-content.size())throw DbError("REQUEST_TOO_LARGE","Input exceeds 64 MiB");content.append(buffer,count);}
    if(in.bad())throw DbError("IO_ERROR","Cannot read input file");
    return content;
}
static void write_transfer_file(const std::string& file,const std::string& content) {
    // Create-new prevents an export from replacing an existing user file.
    auto path=fs::u8path(file);
#ifdef _WIN32
    auto handle=CreateFileW(path.c_str(),GENERIC_WRITE,0,nullptr,CREATE_NEW,FILE_ATTRIBUTE_NORMAL,nullptr);
    if(handle==INVALID_HANDLE_VALUE)throw DbError("IO_ERROR","Cannot create export file (existing files are never overwritten)");
    std::size_t offset=0;bool ok=true;
    while(offset<content.size()){DWORD written=0;auto chunk=static_cast<DWORD>(std::min<std::size_t>(content.size()-offset,1024u*1024u));if(!WriteFile(handle,content.data()+offset,chunk,&written,nullptr)||written!=chunk){ok=false;break;}offset+=written;}
    if(ok)ok=FlushFileBuffers(handle)!=0;
    CloseHandle(handle);
#else
    auto fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600);
    if(fd<0)throw DbError("IO_ERROR","Cannot create export file (existing files are never overwritten)");
    std::size_t offset=0;bool ok=true;
    while(offset<content.size()){auto written=::write(fd,content.data()+offset,content.size()-offset);if(written<0&&errno==EINTR)continue;if(written<=0){ok=false;break;}offset+=static_cast<std::size_t>(written);}
    if(ok)ok=::fsync(fd)==0;
    ::close(fd);
#endif
    if(!ok)throw DbError("IO_ERROR","Export file write failed; partial new file retained for inspection");
}
static int transfer_mode(const std::string& mode,const std::string& url,const std::string& file,
                         const std::string& format,const std::string& table,const std::string& sql,
                         bool header,std::uint64_t timeout_ms) {
    httplib::Client client(url);client.set_read_timeout(timeout_ms/1000,(timeout_ms%1000)*1000);client.set_connection_timeout(3,0);
    auto created=client.Post("/api/v1/sessions","{}","application/json");
    if(!created||created->status!=200)throw DbError("IO_ERROR","Cannot create a server session");
    auto sid=json::parse(created->body).at("session_id").get<std::string>();auto endpoint="/api/v1/sessions/"+sid;
    struct CloseSession {httplib::Client& client;std::string endpoint;~CloseSession(){try{client.Delete(endpoint);}catch(...){}}} guard{client,endpoint};
    json request={{"format",format},{"header",header}};if(!table.empty())request["table"]=table;if(!sql.empty())request["sql"]=sql;
    if(mode=="import")request["content"]=read_transfer_file(file);
    auto payload=request.dump();if(payload.size()>transfer_limit)throw DbError("REQUEST_TOO_LARGE","Encoded import request exceeds 64 MiB");
    auto response=client.Post(endpoint+"/"+mode,payload,"application/json");
    if(!response){std::cerr<<"Connection lost. Import outcome may be unknown; do not retry automatically.\n";return 2;}
    auto result=json::parse(response->body);
    if(response->status!=200||!result.value("ok",false)){std::cout<<result.dump(2)<<"\n";return 1;}
    if(mode=="export"){if(!result.contains("content")||!result["content"].is_string())throw DbError("INVALID_RESPONSE","Export content missing");write_transfer_file(file,result["content"].get<std::string>());result.erase("content");result["file"]=file;}
    std::cout<<result.dump(2)<<"\n";return 0;
}
static int shared_batch_mode(const std::string& url,const std::string& file,std::uint64_t timeout_ms){
    auto path=fs::u8path(file);
    if(!fs::is_regular_file(path)||fs::file_size(path)>256u*1024u)throw DbError("BATCH_LIMIT","Batch script must be a regular file no larger than256 KiB");
    auto queries=interchange::split_sql_script(read_transfer_file(file));
    if(queries.empty()||queries.size()>16)throw DbError("BATCH_LIMIT","Batch script must contain1..16 SELECT statements");
    httplib::Client client(url);client.set_read_timeout(timeout_ms/1000,(timeout_ms%1000)*1000);client.set_connection_timeout(3,0);
    auto created=client.Post("/api/v1/sessions","{}","application/json");
    if(!created||created->status!=200)throw DbError("IO_ERROR","Cannot create a server session");
    auto sid=input::parse_json(created->body).at("session_id").get<std::string>();auto endpoint="/api/v1/sessions/"+sid;
    struct CloseSession {httplib::Client& client;std::string endpoint;~CloseSession(){try{client.Delete(endpoint);}catch(...){}}} guard{client,endpoint};
    const auto payload=json{{"queries",queries}}.dump();auto response=client.Post(endpoint+"/batch",payload,"application/json");
    if(!response){std::cerr<<"Connection lost. Batch is not retried automatically.\n";return 2;}
    if(response->body.size()>64u*1024u*1024u)throw DbError("INVALID_RESPONSE","Batch response exceeds64 MiB");
    // This bounded server response can legitimately contain more than65536
    // result cells; request-shape caps must not be imposed on result rows.
    auto result=json::parse(response->body);std::cout<<result.dump(2)<<"\n";return response->status==200&&result.value("ok",false)?0:1;
}

static std::uint64_t unsigned_argument(const std::string& text, const std::string& name,
                                       std::uint64_t minimum, std::uint64_t maximum) {
    if(text.empty() || text.find_first_not_of("0123456789")!=std::string::npos)
        throw DbError("ARGUMENT", name+" must be an unsigned decimal integer");
    std::uint64_t value=0;
    for(char ch:text) {
        auto digit=static_cast<unsigned>(ch-'0');
        if(digit>maximum || value>(maximum-digit)/10)
            throw DbError("ARGUMENT",name+" outside allowed range");
        value=value*10+digit;
    }
    if(value<minimum || value>maximum)throw DbError("ARGUMENT",name+" outside allowed range");
    return value;
}
// SQL-aware script splitting: preserve quoted semicolons and both SQL comments.
static std::vector<std::string> statements(const std::string& s, bool include_tail=true, std::string* tail=nullptr) {
    std::vector<std::string> out; std::string part; char quote=0; bool line=false,block=false,meaningful=false;
    for(std::size_t i=0;i<s.size();++i) {
        char c=s[i], n=i+1<s.size()?s[i+1]:0;
        if(line) { part+=c; if(c=='\n') line=false; continue; }
        if(block) { part+=c; if(c=='*'&&n=='/') {part+=n;++i;block=false;} continue; }
        if(!quote && c=='-'&&n=='-') {part+=c;part+=n;++i;line=true;continue;}
        if(!quote && c=='/'&&n=='*') {part+=c;part+=n;++i;block=true;continue;}
        if((c=='\''||c=='"')&&(!quote||quote==c)) {meaningful=true;part+=c;if(quote&&n==c){part+=n;++i;continue;}quote=quote?0:c;continue;}
        if(!quote&&c==';') {if(meaningful)out.push_back(part);part.clear();meaningful=false;}
        else {part+=c;if(!std::isspace(static_cast<unsigned char>(c)))meaningful=true;}
    }
    if(include_tail&&meaningful)out.push_back(part);
    if(tail)*tail=part;
    return out;
}
static int client_mode(const std::string& url, const std::string& file, std::uint64_t timeout_ms) {
    httplib::Client client(url); client.set_read_timeout(timeout_ms/1000,(timeout_ms%1000)*1000); client.set_connection_timeout(3,0);
    auto created=client.Post("/api/v1/sessions", "{}", "application/json");
    if(!created||created->status!=200) {std::cerr<<"Cannot connect to "<<url<<"\n";return 2;}
    auto sid=json::parse(created->body).at("session_id").get<std::string>();
    auto endpoint="/api/v1/sessions/"+sid;
    auto execute=[&](const std::string& sql) {
        auto r=client.Post(endpoint+"/sql",json{{"sql",sql}}.dump(),"application/json");
        if(!r) {std::cerr<<"Connection lost. Write outcome may be unknown; do not retry automatically.\n";return false;}
        auto j=json::parse(r->body);std::cout<<j.dump(2)<<"\n";return j.value("ok",false);
    };
    int code=0;
    if(!file.empty()) {
        std::ifstream in(fs::u8path(file),std::ios::binary);if(!in){std::cerr<<"Cannot read SQL file\n";code=2;}
        else {std::ostringstream b;b<<in.rdbuf();for(auto& s:statements(b.str()))if(!execute(s)){code=1;break;}}
    } else {
        std::string line,pending;std::cout<<"EduDB SQL shell (.quit to exit, semicolon to execute)\n";
        while(std::cout<<(pending.empty()?"edudb> ":"    -> "),std::getline(std::cin,line)) {
            if(line==".quit"||line==".exit")break;
            pending+=line+"\n";
            std::string remainder;
            auto ready=statements(pending,false,&remainder);
            for(auto& statement:ready)execute(statement);
            pending=std::move(remainder);
        }
    }
    client.Delete(endpoint);return code;
}
int main(int argc,char** argv) {
#ifdef _WIN32
    int wide_count=0;
    auto wide=CommandLineToArgvW(GetCommandLineW(),&wide_count);
    std::vector<std::string> utf8_args;
    std::vector<char*> utf8_pointers;
    if(wide){
        utf8_args.reserve(wide_count);
        for(int i=0;i<wide_count;++i){int n=WideCharToMultiByte(CP_UTF8,0,wide[i],-1,nullptr,0,nullptr,nullptr);std::string value(static_cast<std::size_t>(n),'\0');WideCharToMultiByte(CP_UTF8,0,wide[i],-1,value.data(),n,nullptr,nullptr);value.pop_back();utf8_args.push_back(std::move(value));}
        LocalFree(wide);
        for(auto& value:utf8_args)utf8_pointers.push_back(value.data());
        argc=wide_count;argv=utf8_pointers.data();
    }
    SetConsoleOutputCP(CP_UTF8);SetConsoleCP(CP_UTF8);
#endif
    try {
        std::string mode=argc>1?argv[1]:"help", data="data-v4", url="http://127.0.0.1:8765", file,format="csv",table,sql,source;bool header=true,data_given=false;
        std::vector<std::string> archive_files;std::uint64_t target_lsn=0;bool target_given=false,replica_given=false,port_given=false;unsigned replica_port=0;std::string replication_token;
        StorageOptions storage_options;json media_request={{"action","status"}};
        if(mode=="help"||mode=="--help"||mode=="-h") {
            std::cout<<"EduDB 4.0.0\nedudb serve --data DIR --web DIR --port 8765\nedudb shell --url http://127.0.0.1:8765\nedudb run --url http://127.0.0.1:8765 --file demo.sql [--timeout-ms 300000]\nedudb batch --url URL --file readonly-batch.sql [--timeout-ms 300000]\nedudb import --url URL --format csv --table TABLE --file input.csv [--header true]\nedudb import --url URL --format sql --file dump.sql\nedudb export --url URL --format csv --table TABLE --file new.csv\nedudb export --url URL --format sql --file new.sql\nedudb migrate-v2 --source CLOSED_V2_DIR --data NEW_V4_DIR\nedudb migrate-v3 --source CLOSED_V3_DIR --data NEW_V4_DIR\nedudb restore --source BACKUP_DIR --data NEW_DIR [--archive MANIFEST ...] [--target-lsn COMMIT_LSN]\nedudb serve --data RESTORED_DIR --replica-port 8876 --replication-token TOKEN\nedudb ship --source ARCHIVE_MANIFEST --port 8876 --replication-token TOKEN\nSQL recovery: BACKUP DATABASE AS 'label'; SET ARCHIVING ON; ARCHIVE WAL; SHOW RECOVERY; PROMOTE STANDBY;\nExport files are create-new; existing files are never overwritten.\n";return 0;
        }
        if(mode!="serve"&&mode!="shell"&&mode!="run"&&mode!="batch"&&mode!="import"&&mode!="export"&&mode!="migrate-v2"&&mode!="migrate-v3"&&mode!="media"&&mode!="restore"&&mode!="ship")throw DbError("ARGUMENT","Unknown mode: "+mode);
        fs::path base=fs::absolute(fs::u8path(argv[0])).parent_path();
        std::string web=(fs::is_directory(base/"web")?base/"web":base.parent_path()/"web").u8string();int port=8765;std::size_t frames=128,limit=32u*1024u*1024u;std::uint64_t timeout_ms=300000;
        for(int i=2;i<argc;++i) {
            std::string a=argv[i];if(i+1>=argc)throw DbError("ARGUMENT","Missing value for "+a);
            std::string v=argv[++i];
            if(a=="--data"){data=v;data_given=true;}else if(a=="--source")source=v;else if(a=="--port"){port=static_cast<int>(unsigned_argument(v,a,1,65535));port_given=true;}else if(a=="--url")url=v;
            else if(a=="--archive"){if(archive_files.size()>=4096)throw DbError("ARGUMENT","At most4096 archive manifests");archive_files.push_back(v);}
            else if(a=="--target-lsn"){target_lsn=unsigned_argument(v,a,0,std::numeric_limits<std::uint64_t>::max());target_given=true;}
            else if(a=="--replica-port"){replica_port=static_cast<unsigned>(unsigned_argument(v,a,0,65535));replica_given=true;}
            else if(a=="--replication-token")replication_token=v;
            else if(a=="--file")file=v;else if(a=="--web")web=v;else if(a=="--frames")frames=static_cast<std::size_t>(unsigned_argument(v,a,2,65536));
            else if(a=="--private-limit")limit=static_cast<std::size_t>(unsigned_argument(v,a,1,1024u*1024u*1024u));
            else if(a=="--data-layout")storage_options.data_layout=v;
            else if(a=="--recovery-mode")storage_options.recovery_mode=v;
            else if(a=="--aries-steal"||a=="--aries-force-at-commit"){
                if(v!="true"&&v!="false")throw DbError("ARGUMENT",a+" requires true or false");
                if(a=="--aries-steal")storage_options.aries_steal=v=="true";
                else storage_options.aries_force_at_commit=v=="true";
            }
            else if(a=="--media-members")storage_options.media_members=static_cast<std::size_t>(unsigned_argument(v,a,2,16));
            else if(a=="--action")media_request["action"]=v;
            else if(a=="--member")media_request["member"]=unsigned_argument(v,a,0,15);
            else if(a=="--timeout-ms")timeout_ms=unsigned_argument(v,a,1,600000);
            else if(a=="--format")format=v;else if(a=="--table")table=v;else if(a=="--sql")sql=v;
            else if(a=="--header"){if(v!="true"&&v!="false")throw DbError("ARGUMENT","--header requires true or false");header=v=="true";}
            else throw DbError("ARGUMENT","Unknown option "+a);
        }
        if(mode!="restore"&&(!archive_files.empty()||target_given))throw DbError("ARGUMENT","--archive/--target-lsn apply only to restore");
        if(replica_given&&mode!="serve")throw DbError("ARGUMENT","--replica-port applies only to serve");
        if(!replication_token.empty()&&mode!="ship"&&!replica_given)throw DbError("ARGUMENT","--replication-token requires ship or a serve receiver");
        if(replica_given&&(replication_token.empty()||replica_port==static_cast<unsigned>(port)))throw DbError("ARGUMENT","Receiver requires a token and a distinct loopback port");
        if(mode!="serve"&&(!storage_options.data_layout.empty()||storage_options.media_members))throw DbError("ARGUMENT","Media layout creation options apply only to serve");
        if(mode!="serve"&&(!storage_options.recovery_mode.empty()||storage_options.aries_steal.has_value()||storage_options.aries_force_at_commit.has_value()))throw DbError("ARGUMENT","Recovery mode options apply only to serve");
        if(mode!="media"&&(media_request.at("action")!="status"||media_request.contains("member")))throw DbError("ARGUMENT","--action/--member apply only to offline media maintenance");
        if(mode=="restore"){if(source.empty()||!data_given)throw DbError("ARGUMENT","restore requires --source BACKUP_DIR and --data NEW_DIR");std::cout<<recovery::restore_backup(source,archive_files,target_lsn,data).dump(2)<<"\n";return 0;}
        if(mode=="ship"){if(source.empty()||replication_token.empty()||!port_given)throw DbError("ARGUMENT","ship requires --source ARCHIVE_MANIFEST --port RECEIVER_PORT --replication-token TOKEN");std::cout<<recovery::ship_archive(source,static_cast<unsigned>(port),replication_token).dump(2)<<"\n";return 0;}
        if(mode=="media"){if(!data_given)throw DbError("ARGUMENT","media requires explicit --data CLOSED_DATABASE_DIR");std::cout<<Storage::media_maintenance(data,media_request).dump(2)<<"\n";return 0;}
        if(mode=="batch"){if(file.empty())throw DbError("ARGUMENT","batch requires --file");return shared_batch_mode(url,file,timeout_ms);}
        if(mode=="run"&&file.empty())throw DbError("ARGUMENT","run requires --file");
        if(mode=="shell"||mode=="run")return client_mode(url,file,timeout_ms);
        if(mode=="migrate-v2"){if(source.empty()||!data_given)throw DbError("ARGUMENT","migrate-v2 requires --source CLOSED_V2_DIR and explicit --data NEW_V4_DIR");const auto result=Storage::migrate_v2(source,data);std::cout<<result.dump(2)<<"\n";return result.value("status",std::string())=="PASS"?0:1;}
        if(mode=="migrate-v3"){if(source.empty()||!data_given)throw DbError("ARGUMENT","migrate-v3 requires --source CLOSED_V3_DIR and explicit --data NEW_V4_DIR");const auto result=Storage::migrate_v3(source,data);std::cout<<result.dump(2)<<"\n";return result.value("status",std::string())=="PASS"?0:1;}
        if(mode=="import"||mode=="export"){if(file.empty())throw DbError("ARGUMENT",mode+" requires --file");if(format!="csv"&&format!="sql")throw DbError("ARGUMENT","--format requires csv or sql");return transfer_mode(mode,url,file,format,table,sql,header,timeout_ms);}
        if(port<1||port>65535||frames<2)throw DbError("ARGUMENT","Invalid port or buffer frame count");
        if(!fs::is_regular_file(fs::u8path(web)/"index.html"))throw DbError("ARGUMENT","Web directory must contain index.html: "+web);
        Engine engine(data,frames,limit,storage_options);
        std::unique_ptr<recovery::StandbyReceiver> receiver;
        if(replica_given){if(engine.recovery_identity().at("role")=="RESTORED_READ_ONLY")engine.become_standby();const auto id=engine.recovery_identity();if(id.at("role")!="STANDBY")throw DbError("STANDBY_SOURCE","Receiver requires a separately restored standby database");recovery::ReceiverOptions opts;opts.database_uuid=id.at("uuid");opts.timeline=id.at("timeline");opts.inbox_directory=(fs::u8path(data)/"inbox").u8string();opts.owner_directory=data;opts.base_lsn=id.at("durable_lsn");opts.token=replication_token;opts.port=replica_port;receiver=std::make_unique<recovery::StandbyReceiver>(opts,[&](const std::string& manifest,bool validate){return engine.replay_archive(manifest,5000,validate);});receiver->start();}
        httplib::Server server;running=&server;
        server.new_task_queue=[] {return new httplib::ThreadPool(32);};
        server.set_socket_options([](socket_t sock) {
#ifdef _WIN32
            // SO_REUSEADDR permits multiple live listeners on Windows.
            if(!httplib::set_socket_opt(sock,SOL_SOCKET,SO_EXCLUSIVEADDRUSE,1))
                throw DbError("IO_ERROR","Cannot reserve exclusive local listening socket");
#else
            if(!httplib::set_socket_opt(sock,SOL_SOCKET,SO_REUSEADDR,1))
                throw DbError("IO_ERROR","Cannot configure local listening socket");
#endif
        });
        server.set_payload_max_length(transfer_limit);server.set_read_timeout(30,0);server.set_write_timeout(60,0);
        const std::string host="127.0.0.1:"+std::to_string(port), local="localhost:"+std::to_string(port);
        server.set_pre_routing_handler([&](const httplib::Request& req,httplib::Response& res){
            auto h=req.get_header_value("Host"),o=req.get_header_value("Origin");
            if((h!=host&&h!=local)||(!o.empty()&&o!="http://"+host&&o!="http://"+local)) {
                send(res,json{{"ok",false},{"error",{{"code","ORIGIN"},{"message","Local origin required"}}}},403);
                return httplib::Server::HandlerResponse::Handled;
            }
            const bool is_import=req.path.size()>=7&&req.path.compare(req.path.size()-7,7,"/import")==0&&req.path.rfind("/api/v1/sessions/",0)==0;
            const auto length=req.get_header_value("Content-Length");
            if(!length.empty()){
                const std::uint64_t maximum=is_import?transfer_limit:1024u*1024u;std::uint64_t declared=0;bool excessive=false;
                for(char digit:length){if(digit<'0'||digit>'9'){send(res,json{{"ok",false},{"error",{{"code","INVALID_REQUEST"},{"message","Invalid Content-Length"}}}},400);return httplib::Server::HandlerResponse::Handled;}const auto n=static_cast<unsigned>(digit-'0');if(declared>(maximum-n)/10){excessive=true;break;}declared=declared*10+n;}
                if(excessive||declared>maximum){send(res,json{{"ok",false},{"error",{{"code","REQUEST_TOO_LARGE"},{"message",is_import?"Encoded import exceeds 64 MiB":"SQL and ordinary requests exceed 1 MiB"}}}},413);return httplib::Server::HandlerResponse::Handled;}
            }
            if(!is_import&&req.body.size()>1024u*1024u){send(res,json{{"ok",false},{"error",{{"code","REQUEST_TOO_LARGE"},{"message","SQL and ordinary requests are limited to 1 MiB; import accepts at most 64 MiB encoded JSON"}}}},413);return httplib::Server::HandlerResponse::Handled;}
            return httplib::Server::HandlerResponse::Unhandled;
        });
        auto protect=[&](auto handler) {
            return [handler](const httplib::Request& req,httplib::Response& res) {
                const bool is_import=req.path.size()>=7&&req.path.compare(req.path.size()-7,7,"/import")==0&&req.path.rfind("/api/v1/sessions/",0)==0;
                if(!is_import&&req.body.size()>1024u*1024u){send(res,json{{"ok",false},{"error",{{"code","REQUEST_TOO_LARGE"},{"message","SQL and ordinary requests are limited to 1 MiB"}}}},413);return;}
                try{handler(req,res);}catch(const DbError& e){send(res,json{{"ok",false},{"error",{{"code",e.code},{"message",e.what()},{"position",e.position}}}},400);}
                catch(const std::exception& e){send(res,json{{"ok",false},{"error",{{"code","REQUEST_ERROR"},{"message",e.what()}}}},400);}
            };
        };
        server.set_error_handler([](const auto&,auto& res){if(res.get_header_value("Content-Type").find("application/json")!=std::string::npos)return;send(res,json{{"ok",false},{"error",{{"code",res.status==413?"REQUEST_TOO_LARGE":"HTTP_ERROR"},{"message","HTTP status "+std::to_string(res.status)}}}},res.status);});
        server.Get("/api/v1/health",[&](const auto&,auto& res){send(res,json{{"ok",true},{"product","EduDB"},{"version","4.0.0"},{"instance_id",engine.instance_id()},{"input_limits",input::describe()}});});
        server.Post("/api/v1/sessions",protect([&](const auto&,auto& res){send(res,json{{"ok",true},{"session_id",engine.open_session()}});}));
        server.Post(R"(/api/v1/sessions/([^/]+)/sql)",protect([&](const auto& req,auto& res){
            auto body=request_json(req);if(!body.is_object()||!body.contains("sql")||!body["sql"].is_string())throw DbError("INVALID_REQUEST","Body must be an object with a string sql field");auto outcome=engine.execute(req.matches[1].str(),body["sql"].template get<std::string>());if(outcome.value("ok",false)&&outcome["plan"].is_object()&&outcome["plan"].value("node",std::string())=="PromoteStandby"&&receiver)receiver->stop();send(res,wire_result(std::move(outcome)));
        }));
        server.Post(R"(/api/v1/sessions/([^/]+)/batch)",protect([&](const auto& req,auto& res){
            auto body=request_json(req);
            if(!body.is_object()||!body.contains("queries")||!body["queries"].is_array())throw DbError("INVALID_REQUEST","Batch body must contain a queries array of SQL strings");
            if(body["queries"].empty()||body["queries"].size()>16)throw DbError("BATCH_LIMIT","Batch requires1..16 queries");
            std::vector<std::string> queries;std::size_t bytes=0;
            for(const auto& value:body["queries"]){if(!value.is_string())throw DbError("INVALID_REQUEST","Batch queries must be strings");const auto& text=value.template get_ref<const std::string&>();if(text.size()>256u*1024u-bytes)throw DbError("BATCH_LIMIT","Batch SQL text exceeds256 KiB");bytes+=text.size();queries.push_back(text);}
            auto result=engine.execute_shared_batch(req.matches[1].str(),queries);if(result.contains("results"))for(auto& query:result["results"])query=wire_result(std::move(query));send(res,result);
        }));
        server.Delete(R"(/api/v1/sessions/([^/]+))",protect([&](const auto& req,auto& res){engine.close_session(req.matches[1].str());send(res,json{{"ok",true}});}));
        server.Post(R"(/api/v1/sessions/([^/]+)/import)",protect([&](const auto& req,auto& res){auto body=request_json(req);if(!body.is_object())throw DbError("INVALID_REQUEST","Import request must be an object");send(res,wire_result(engine.import_data(req.matches[1].str(),body)));}));
        server.Post(R"(/api/v1/sessions/([^/]+)/export)",protect([&](const auto& req,auto& res){auto body=request_json(req);if(!body.is_object())throw DbError("INVALID_REQUEST","Export request must be an object");send(res,wire_result(engine.export_data(req.matches[1].str(),body)));}));
        server.Post(R"(/api/v1/sessions/([^/]+)/inspect)",protect([&](const auto& req,auto& res){auto body=request_json(req);if(!body.is_object())throw DbError("INVALID_REQUEST","Inspect request must be an object");send(res,engine.inspect(req.matches[1].str(),body));}));
        server.Get("/api/v1/diagnostics",protect([&](const auto&,auto& res){auto out=engine.diagnostics();if(receiver)out["replication_receiver"]=receiver->status();send(res,out);}));
        server.Post("/api/v1/checkpoint",protect([&](const auto&,auto& res){send(res,engine.checkpoint());}));
        if(fs::exists(fs::u8path(web)))server.set_mount_point("/",web);
        std::signal(SIGINT,stop_server);std::signal(SIGTERM,stop_server);
        if(!server.bind_to_port("127.0.0.1",port)) {
            running=nullptr;std::cerr<<"Cannot bind local port "<<port<<"\n";return 2;
        }
        std::cout<<json{{"event","listening"},{"instance_id",engine.instance_id()},{"url","http://"+host},{"data",data},{"web",web},{"receiver_port",receiver?receiver->port():0}}.dump()<<std::endl;
        bool ok=server.listen_after_bind();running=nullptr;
        if(!ok){std::cerr<<"Local server stopped with an error\n";return 2;}return 0;
    } catch(const DbError& e){std::cerr<<json{{"ok",false},{"error",{{"code",e.code},{"message",e.what()},{"position",e.position}}}}.dump()<<"\n";return 2;}
      catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 2;}
}
