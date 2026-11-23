#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
using edudb::json;
static void environment(const char* name,const std::string& value){
#ifdef _WIN32
    _putenv_s(name,value.c_str());
#else
    setenv(name,value.c_str(),1);
#endif
}
int main(){try{
    const auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB半连接独立模型_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));edudb::Engine db(directory.u8string(),128);auto session=db.open_session();std::string line;
    while(std::getline(std::cin,line)){
        const auto request=json::parse(line);environment("EDUDB_OPTIMIZER_DECORRELATE",request.value("mode",std::string("ON")));environment("EDUDB_QUERY_OPERATOR_MEMORY_BYTES",std::to_string(request.value("memory",65536)));
        std::cout<<json{{"result",db.execute(session,request.at("sql").get<std::string>())},{"data_directory",directory.u8string()}}.dump()<<"\n";
    }
    db.close_session(session);
}catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}}
