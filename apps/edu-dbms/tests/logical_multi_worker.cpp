#include "edudb.hpp"
#include <filesystem>
#include <iostream>
using namespace edudb;
namespace {void sql(Engine& db,const std::string& session,const std::string& text){auto result=db.execute(session,text);if(!result.value("ok",false))throw std::runtime_error(text+": "+result.dump());}}
int main(int argc,char** argv){try{if(argc!=2)throw std::runtime_error("usage SEEDED_ISOLATED_DB");Engine db(std::filesystem::u8path(argv[1]).u8string(),2);auto a=db.open_session(),b=db.open_session();sql(db,a,"BEGIN");sql(db,a,"UPDATE t SET v='甲第一次' WHERE id=1");sql(db,a,"UPDATE t SET v='甲第二次' WHERE id=1");sql(db,b,"BEGIN");sql(db,b,"UPDATE t SET v='乙已提交' WHERE id=2");sql(db,b,"COMMIT");sql(db,a,"ROLLBACK");std::cout<<"{\"status\":\"PASS_COMPLETE_MULTI_UNDO\"}\n";return 0;}catch(const DbError& e){std::cerr<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";return 4;}catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"message",e.what()}}.dump()<<"\n";return 5;}}
