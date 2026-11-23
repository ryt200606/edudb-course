#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#ifdef _WIN32
#include <windows.h>
#endif
int main(){
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
#endif
    try{using edudb::json;auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB自连接独立_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));edudb::Engine db(directory.u8string(),64);auto sid=db.open_session();std::string line;
    while(std::getline(std::cin,line)){const auto request=json::parse(line);const auto mode=request.value("mode",std::string("ON"));
#ifdef _WIN32
        _putenv_s("EDUDB_OPTIMIZER_SELF_JOIN",mode.c_str());
#else
        setenv("EDUDB_OPTIMIZER_SELF_JOIN",mode.c_str(),1);
#endif
        std::cout<<json{{"result",db.execute(sid,request.at("sql").get<std::string>())},{"data_directory",directory.u8string()}}.dump()<<"\n";
    }db.close_session(sid);
    }catch(const std::exception&e){std::cerr<<e.what()<<"\n";return 1;}
}
