#include "edudb.hpp"
#include "standby_transport.hpp"
#include "httplib.h"
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
using namespace edudb;
namespace fs=std::filesystem;
int main(){try{
 const auto root=fs::absolute(fs::path("standby-input-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));
 recovery::ReceiverOptions options;options.database_uuid=std::string(32,'1');options.timeline=std::string(32,'2');options.token=std::string(32,'3');options.inbox_directory=(root/"inbox").u8string();options.owner_directory=root.u8string();
 std::atomic<int> callbacks{0};recovery::StandbyReceiver receiver(options,[&](const std::string&,bool)->json{++callbacks;throw std::runtime_error("Invalid metadata reached storage replay");});receiver.start();
 httplib::Client client("127.0.0.1",static_cast<int>(receiver.port()));client.set_read_timeout(5,0);client.set_write_timeout(5,0);httplib::Headers headers{{"X-EduDB-Token",options.token}};int cases=0;
 std::vector<std::string> inputs={std::string(20000,'[')+"0"+std::string(20000,']'),"[]","{\"format\":42}","{\"unknown\":\"中文 [ { \\\"quoted\\\" } ]\"}"};
 std::string wide="[";for(int i=0;i<70000;++i){if(i)wide+=',';wide+='0';}wide+=']';inputs.push_back(std::move(wide));
 for(const auto& text:inputs){std::string body(8,'\0');const auto bytes=static_cast<std::uint64_t>(text.size());for(unsigned i=0;i<8;++i)body[i]=static_cast<char>(bytes>>(8*i));body+=text;body+=std::string(64,'\0');auto result=client.Post("/receive",headers,body,"application/octet-stream");if(!result||result->status!=409)throw std::runtime_error("Malformed envelope did not receive a bounded protocol rejection");auto response=json::parse(result->body);if(response["error"]["code"]!="RECEIVER_PROTOCOL")throw std::runtime_error(response.dump());auto status=client.Get("/status",headers);if(!status||status->status!=200)throw std::runtime_error("Receiver not alive after rejection");auto state=json::parse(status->body);if(state["forced_lsn"]!=0||state["replayed_lsn"]!=0||callbacks.load()!=0)throw std::runtime_error("Rejected metadata advanced persisted receive state");++cases;}
 receiver.stop();std::cout<<"PASS bounded native loopback standby metadata "<<cases<<" cases; no storage replay or watermark advance\n";return 0;
 }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
