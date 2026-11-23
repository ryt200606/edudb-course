#include "edudb.hpp"
#include "access.hpp"
#include "io.hpp"
#include "interchange.hpp"
#include "input_limits.hpp"
#include "query_operators.hpp"
#include "query_relational.hpp"
#include "materialized_delta.hpp"
#include "query_spill.hpp"
#include "lock_manager.hpp"
#include "execution_progress.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>


#include <iostream>
#include <stdexcept>
#ifdef _WIN32
#include <windows.h>
#endif

namespace edudb { namespace {
using Clock=std::chrono::steady_clock;
#include "../src/sql_frontend.inc"
} }
static int checks=0;
static void check(bool v,const std::string& text){++checks;if(!v)throw std::runtime_error(text);}
static void bad(const std::string& sql){bool caught=false;try{edudb::Parser(sql).parse();}catch(const edudb::DbError&e){caught=e.code=="QUERY_TOO_COMPLEX"&&e.position>=0;}check(caught,"Parser rejects bounded input with byte position before binding");}
int main(){
#ifdef _WIN32
SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
#endif
try{
    auto parse=[](const std::string& sql){return edudb::Parser(sql).parse();};
    check(parse("SELECT "+std::string(127,'(')+"1"+std::string(127,')')).items.size()==1,"Existing127 parentheses accepted");bad("SELECT "+std::string(128,'(')+"1"+std::string(128,')'));
    std::string negation="SELECT ";for(int n=0;n<127;++n)negation+="NOT ";negation+="TRUE";check(parse(negation).items[0].value->depth==128,"Existing128 expression depth accepted");bad("SELECT NOT "+negation.substr(7));
    std::string flat="SELECT 1 WHERE TRUE";for(int n=0;n<5000;++n)flat+=" AND TRUE";bad(flat);check(parse("SELECT 42").items.size()==1,"Failure restores parser-node scope");
    std::string nested="SELECT 1";for(int n=0;n<20;++n)nested="SELECT ("+nested+")";bad(nested);
    std::string wide="SELECT ";const auto item=negation.substr(7);for(int n=0;n<2000;++n){if(n)wide+=',';wide+=item;}check(wide.size()<1024u*1024u,"AST-node case fits ordinary SQL byte contract");bad(wide);
    {std::string nested="SELECT 1 WHERE TRUE";for(int level=0;level<4;++level){nested="SELECT 1 WHERE EXISTS("+nested+")";for(int i=0;i<126;++i)nested+=" AND TRUE";}bool caught=false;try{parse(nested);}catch(const edudb::DbError&e){caught=e.code=="QUERY_TOO_COMPLEX"&&std::string(e.what()).find("height")!=std::string::npos&&e.position>=0;}check(caught,"Cross-query ownership height rejects at construction before a long shared_ptr destruction chain exists");}
    check(parse("\xef\xbb\xbfSELECT 1").items.size()==1,"Native SQL accepts UTF8 BOM");
    {std::string values="INSERT INTO t VALUES";for(int n=0;n<50000;++n){if(n)values+=',';values+="(1,2,3)";}auto st=parse(values);check(st.values.size()==50000&&st.values.back().size()==3,"50K three-column VALUES retains500K token contract");}
    {std::string query="SELECT 1";for(int n=0;n<64;++n)query+=" UNION ALL SELECT 1";check(parse(query).set_operation=="UNION","Existing64 set-operation node contract retained");bad(query+" UNION ALL SELECT 1");}
    {edudb::Statement st;st.kind="SELECT";auto leaf=edudb::expr(edudb::Expr::Literal);leaf->value=true;auto tree=leaf;for(int n=0;n<161;++n){auto parent=edudb::expr(edudb::Expr::Binary,"AND",3);parent->args={tree,leaf};tree=parent;}st.where=tree;bool caught=false;try{edudb::bounded_read_tables(st);}catch(const edudb::DbError&e){caught=e.code=="QUERY_TOO_COMPLEX";}check(caught,"Iterative completed-tree validation does not trust forged/stale expression depth metadata");}
    {edudb::Statement st;st.kind="SELECT";st.sources.push_back({"outer","o","",false});auto subquery=edudb::expr(edudb::Expr::Subquery,"EXISTS",0);subquery->query=std::make_shared<edudb::Statement>();subquery->query->sources.push_back({"inner","i","",false});st.where=subquery;check(edudb::bounded_read_tables(st)==std::vector<std::string>({"inner","outer"}),"Iterative read dependency closure includes nested sources");}
    std::cout<<"PASS "<<checks<<" parser pre-bind depth/nodes/large-batch/dependency checks\n";
}catch(const std::exception&e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<"\n";return 1;}}
