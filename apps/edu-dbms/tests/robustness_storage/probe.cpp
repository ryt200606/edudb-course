#include "edudb.hpp"
#include <filesystem>
#include <iostream>
#include <memory>
#ifdef _WIN32
#include <windows.h>
#endif

using edudb::json;
static json table() {
    return {{"columns",json::array({{{"name","id"},{"type","INTEGER"}},{{"name","payload"},{"type","TEXT"}}})},
            {"indexes",json::array({{{"name","ix_id"},{"column","id"},{"unique",true}}})},
            {"rows",json::array()}};
}
int main(int argc,char** argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX);
#endif
    if(argc!=3) return 2;
    const std::string mode=argv[1],path=argv[2];
    try {
        if(mode=="seed"||mode=="seed-checkpoint") {
            edudb::Storage db(path,2);
            auto tx=db.begin(); auto t=table();
            for(int i=0;i<300;++i)t["rows"].push_back(json::array({i,std::string(150,'x')}));
            db.write_table(tx,"t",t);db.commit(tx);
            if(mode=="seed-checkpoint")db.checkpoint();
            std::cout<<json{{"status","PASS"},{"diagnostics",db.diagnostics()}}.dump()<<"\n";
        } else if(mode=="inspect") {
            edudb::Storage db(path,2);auto tx=db.begin();auto t=db.read_table(tx,"t");
            auto v=db.verify(tx);db.commit(tx);
            std::cout<<json{{"status","OPENED"},{"rows",t["rows"].size()},{"verify",v},{"diagnostics",db.diagnostics()}}.dump()<<"\n";
        } else if(mode=="limit-commit") {
            edudb::Storage db(path,2,3*8192);auto tx=db.begin();db.write_table(tx,"t",table());db.commit(tx);
            tx=db.begin();std::string failure="NONE";int inserted=0;
            try {for(int i=0;i<40;++i){db.insert_row(tx,"t",json::array({i,"x"}));++inserted;}}
            catch(const edudb::DbError& e){failure=e.code;}
            std::string committed="ACK";
            try {db.commit(tx);}catch(const edudb::DbError& e){committed=e.code;}
            tx=db.begin();auto t=db.read_table(tx,"t");auto v=db.verify(tx);db.commit(tx);
            std::cout<<json{{"status","LIMIT_PROBED"},{"inserted_before_failure",inserted},{"failure",failure},{"commit",committed},{"rows",t["rows"].size()},{"verify",v}}.dump()<<"\n";
        } else if(mode=="locks-handles") {
            bool second_rejected=false;
            {edudb::Storage first(path,2);try{edudb::Storage second(path,2);}catch(const edudb::DbError& e){second_rejected=e.code=="DATABASE_LOCKED";}}
            edudb::Storage reopened(path,2);auto tx=reopened.begin();reopened.commit(tx);
            std::cout<<json{{"status","PASS"},{"second_rejected",second_rejected},{"reopened",true}}.dump()<<"\n";
        } else return 2;
    } catch(const edudb::DbError& e) {
        std::cout<<json{{"status","ERROR"},{"code",e.code},{"message",e.what()}}.dump()<<"\n";
    } catch(const std::exception& e) {
        std::cout<<json{{"status","EXCEPTION"},{"message",e.what()}}.dump()<<"\n";
    }
}
