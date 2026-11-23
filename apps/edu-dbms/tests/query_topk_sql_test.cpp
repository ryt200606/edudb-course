#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& message){++checks;if(!value)throw std::runtime_error(message);}
struct Environment {
    std::string name,previous;bool present=false;
    Environment(std::string key,const char* value):name(std::move(key)){const char* old=std::getenv(name.c_str());present=old!=nullptr;if(old)previous=old;
#ifdef _WIN32
        _putenv_s(name.c_str(),value);
#else
        setenv(name.c_str(),value,1);
#endif
    }
    ~Environment(){
#ifdef _WIN32
        _putenv_s(name.c_str(),present?previous.c_str():"");
#else
        if(present)setenv(name.c_str(),previous.c_str(),1);else unsetenv(name.c_str());
#endif
    }
};
static json ok(edudb::Engine& db,const std::string& session,const std::string& sql){auto response=db.execute(session,sql);check(response.value("ok",false),sql+" => "+response.dump());return response;}
static json twin(edudb::Engine& db,const std::string& session,const std::string& sql){
    json baseline;{Environment disabled("EDUDB_OPTIMIZER_TOPK","OFF");baseline=ok(db,session,sql);}
    auto actual=ok(db,session,sql);check(actual["rows"]==baseline["rows"]&&actual["columns"]==baseline["columns"],"Bounded Top-K differs from full stable external order: "+sql);return actual;
}
int main(){auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB真实前K排序_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));try{
    Environment enabled("EDUDB_OPTIMIZER_TOPK","ON"),memory("EDUDB_QUERY_OPERATOR_MEMORY_BYTES","65536");edudb::Engine db(directory.u8string(),8);auto session=db.open_session();
    ok(db,session,"CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER,b VARCHAR(32),payload VARCHAR(2000),r REAL)");
    std::string sql="INSERT INTO t VALUES";for(int n=0;n<200;++n){if(n)sql+=',';sql+='('+std::to_string(n)+','+(n%11==0?"NULL":std::to_string(n%17)) +','+(n%13==0?"NULL":"'中文"+std::to_string(n%5)+"'")+",'"+std::string(n<70?8:1100,'x')+"',"+std::to_string(n%7)+".0)";}ok(db,session,sql);
    const auto small=twin(db,session,"SELECT id,b FROM t ORDER BY a DESC,b,id LIMIT 7 OFFSET 2");check(small["plan"]["final_sort_actual"]["node"]=="BoundedTopK"&&small["plan"]["final_sort_actual"]["k"]==9,"SQL really invokes a nine-entry Top-K node for OFFSET2 LIMIT7");check(small["plan"]["final_sort_actual"]["input_rows"]==200&&small["plan"]["final_sort_actual"]["retained_peak"].get<int>()<=9,"Unordered input is fully scanned but only K candidates retained");check(small["plan"]["final_sort_actual"]["fallback_external_sort"]==false&&small["plan"]["final_sort_actual"]["memory_bytes_estimate_peak"].get<int>()<=65536,"Small K uses a bounded actual heap without fake spill");
    for(const auto& query:{"SELECT id FROM t ORDER BY a LIMIT 10","SELECT id FROM t ORDER BY a DESC LIMIT 10 OFFSET 5","SELECT id,b FROM t ORDER BY b DESC,a LIMIT 13 OFFSET 1","SELECT a AS x,id FROM t ORDER BY x DESC,2 LIMIT 9","SELECT DISTINCT a FROM t ORDER BY a DESC LIMIT 4 OFFSET 2","SELECT a,COUNT(*) FROM t GROUP BY a HAVING COUNT(*)>5 ORDER BY COUNT(*) DESC,a LIMIT 5","SELECT id FROM t WHERE id<10 UNION ALL SELECT id FROM t WHERE id>190 ORDER BY id DESC LIMIT 4 OFFSET 3","SELECT id FROM t WHERE id<15 INTERSECT SELECT id FROM t WHERE id>5 ORDER BY id DESC LIMIT 3","SELECT a.id,b.id FROM t a JOIN t b ON a.id=b.id USE JOIN(HASH) ORDER BY a.a DESC,b.id LIMIT 5","SELECT a.id FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.id=a.id) ORDER BY a.a LIMIT 5","SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 250"})twin(db,session,query);
    const auto stable=twin(db,session,"SELECT id FROM t ORDER BY a LIMIT 9");check(stable["rows"][0][0]==17&&stable["rows"][1][0]==34,"Equal keys preserve original input order");
    const auto wide=twin(db,session,"SELECT payload,id FROM t ORDER BY a,id LIMIT 50");check(wide["plan"]["final_sort_actual"]["fallback_external_sort"]==true&&wide["plan"]["final_sort_actual"]["spill_bytes"].get<std::uint64_t>()>0,"Actual-width fallback writes a real native temporary run");check(wide["plan"]["final_sort_actual"]["external_sort_actual"]["node"]=="ExternalSort","Fallback exposes real external-sort diagnostics");
    const auto huge=twin(db,session,"SELECT id FROM t ORDER BY id LIMIT 18446744073709551614 OFFSET 5");check(huge["plan"]["final_sort"]["node"]=="ExternalSort"&&huge["rows"].size()==195,"OFFSET+LIMIT overflow retains correct external-sort semantics");
    const auto large=twin(db,session,"SELECT id FROM t ORDER BY id LIMIT 1000000");check(large["rows"].size()==200&&large["plan"]["final_sort_actual"]["node"]=="ExternalSort","Large K plans existing external sort directly without huge allocation or redundant candidate runs");
    const auto zero=ok(db,session,"SELECT 1/(id-id) FROM t ORDER BY id LIMIT 0");check(zero["rows"].empty()&&zero["metrics"]["rows_scanned"]==0,"LIMIT0 binds without evaluating projection or opening scans");
    auto explanation=ok(db,session,"EXPLAIN SELECT id FROM t ORDER BY a LIMIT 4 OFFSET 3");check(explanation["plan"]["final_sort"]["node"]=="BoundedTopK"&&explanation["plan"]["final_sort"]["k"]==7&&explanation["metrics"].value("rows_scanned",0)==0,"Plain EXPLAIN shows bounded choice without executing it: "+explanation.dump());
    {
        Environment invalid("EDUDB_OPTIMIZER_TOPK","TYPO");auto failure=db.execute(session,"SELECT id FROM t ORDER BY a LIMIT 5");check(!failure.value("ok",true)&&failure["error"]["code"]=="INVALID_CONFIGURATION","Unknown optimizer setting is rejected");
    }
    check(ok(db,session,"SELECT 42")["rows"][0][0]==42,"Session remains usable after autocommit configuration error");
    const auto failure=db.execute(session,"SELECT payload,1/(id-117) FROM t ORDER BY id DESC LIMIT 50");check(!failure.value("ok",true)&&failure["error"]["code"]=="DIVISION_BY_ZERO","Input projection failure during fallback is not hidden by Top-K");check(failure["metrics"]["io"]["files"]["temp"]["write_bytes"].get<std::uint64_t>()>0,"Failure occurs after actual fallback writes: "+failure.dump());
    for(const auto& file:std::filesystem::directory_iterator(directory))check(file.path().filename().u8string().rfind("edudb-query-",0)!=0,"Failure and successful SQL leave no owned temporary files");
    check(ok(db,session,"SELECT COUNT(*) FROM t")["rows"][0][0]==200,"Read-only sort errors do not alter source rows");db.close_session(session);
    std::cout<<"PASS "<<checks<<" actual SQL bounded Top-K and full-sort dual checks\n";
}catch(const std::exception&e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" directory="<<directory.u8string()<<"\n";return 1;}}
