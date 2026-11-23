#include "edudb.hpp"
#include "format_copy.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#ifdef _WIN32
#include <windows.h>
#endif
using namespace edudb;namespace fs=std::filesystem;namespace fc=edudb::format_copy;
namespace {std::size_t checks=0;void need(bool b,const std::string& m){++checks;if(!b)throw std::runtime_error(m);}template<class F>void error(const std::string& c,F f){try{f();}catch(const DbError& e){need(e.code==c,"Expected "+c+", got "+e.code+": "+e.what());return;}throw std::runtime_error("Missing "+c);}void put(const fs::path& p,const std::string& s){std::ofstream o(p,std::ios::binary);o<<s;if(!o)throw std::runtime_error("fixture write");}std::size_t owned(const fs::path& root){std::size_t count=0;for(const auto& e:fs::directory_iterator(root))if(e.path().filename().u8string().rfind(".edudb-v3-",0)==0)++count;return count;}}
int main(){auto root=fs::temp_directory_path()/("edudb-format-copy-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));try{fs::create_directory(root);put(root/"owned-test.marker","format copy");need(fc::sha256("")=="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","SHA256 empty NIST");need(fc::sha256("abc")=="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","SHA256 abc NIST");need(fc::sha256(std::string(1000000,'a'))=="cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0","SHA256 million-a NIST");auto source=root/"source";fs::create_directory(source);put(source/"database.lock","");put(source/"data.pages","readonly source bytes");fc::write_control(source,{{"format","EduDB"},{"version",3},{"page_size",8192},{"uuid",std::string(32,'a')}});auto control_sha=fc::file_sha256(source/"control.json"),data_sha=fc::file_sha256(source/"data.pages");
#ifdef _WIN32
auto attributes=GetFileAttributesW((source/"data.pages").c_str());need(SetFileAttributesW((source/"data.pages").c_str(),attributes|FILE_ATTRIBUTE_READONLY)!=0,"Set readonly fixture data");
#endif
{fc::LegacySnapshot snap(source,root);need(fc::file_sha256(snap.path()/"data.pages")==data_sha,"Read-only original data copied exactly");need(snap.source_files().at("data.pages").at("sha256")==data_sha,"Source has per-file SHA");error("DATABASE_LOCKED",[&]{fc::LegacySnapshot second(source,root);});snap.promote_owned_copy();need(fc::read_control(snap.path()/"control.json")["version"]==4,"Owned copy envelope4");need(fc::read_control(source/"control.json")["version"]==3,"Original envelope remains3");snap.verify_source_unchanged();
#ifdef _WIN32
need((GetFileAttributesW((snap.path()/"data.pages").c_str())&FILE_ATTRIBUTE_READONLY)==0,"Copy is writable without changing source attribute");
#endif
}need(owned(root)==0,"Snapshot ordinary cleanup");need(fc::file_sha256(source/"control.json")==control_sha&&fc::file_sha256(source/"data.pages")==data_sha,"Original SHA unchanged");fc::Limits tiny;tiny.bytes=1;error("MIGRATION_LIMIT",[&]{fc::LegacySnapshot too_big(source,root,tiny);});need(owned(root)==0,"Bound failure cleans only owned snapshot");{fc::Destination child(source/"illegal-child");error("MIGRATION_PATH",[&]{fc::LegacySnapshot overlap(source,root,{},&child);});}need(owned(root)==0&&!fs::exists(source/"illegal-child"),"Overlap refusal before copy");auto existing=root/"existing";fs::create_directory(existing);put(existing/"user.txt","preserve me");error("TARGET_EXISTS",[&]{fc::Destination d(existing);});{fc::Destination target(root/"late");fc::OwnedDirectory stage(root,"staging");fs::create_directory(target.path());put(target.path()/"user.txt","never overwrite");error("TARGET_EXISTS",[&]{stage.publish(target);});}need(owned(root)==0&&fs::file_size(root/"late/user.txt")==15,"Publication race preserves user target and cleans staging");{fc::Destination target(root/"published");fc::OwnedDirectory stage(root,"staging");put(stage.path()/"data","pass");stage.publish(target);}need(fs::exists(root/"published/data")&&owned(root)==0,"Atomic publication moves accepted owned directory");
#ifdef _WIN32
need(SetFileAttributesW((source/"data.pages").c_str(),attributes)!=0,"Restore owned fixture attribute");
#endif
need(fs::equivalent(root.parent_path(),fs::temp_directory_path())&&root.filename().string().rfind("edudb-format-copy-",0)==0&&fs::exists(root/"owned-test.marker"),"Owned test cleanup guard");fs::remove_all(root);std::cout<<"PASS format copy "<<checks<<" checks\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL format copy after "<<checks<<": "<<e.what()<<"; evidence "<<root.u8string()<<"\n";return 1;}}
