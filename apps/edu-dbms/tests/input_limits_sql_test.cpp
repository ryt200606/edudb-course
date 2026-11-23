#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#ifdef _WIN32
#include <windows.h>
#endif
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& message){++checks;if(!value)throw std::runtime_error(message);}
static json ok(edudb::Engine& db,const std::string& sid,const std::string& sql){auto r=db.execute(sid,sql);check(r.value("ok",false),sql.substr(0,120)+" => "+r.dump());return r;}
static json bad(edudb::Engine& db,const std::string& sid,const std::string& sql,const std::string& code){auto r=db.execute(sid,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,"Expected "+code+" => "+r.dump());if(code=="QUERY_TOO_COMPLEX")check(r["error"]["position"].get<int>()>=0,"SQL resource failure retains byte position");return r;}
int main(){
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
#endif
    const auto path=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB输入边界_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{
    edudb::Engine db(path.u8string(),8);auto sid=db.open_session();ok(db,sid,"CREATE TABLE t(id INTEGER PRIMARY KEY,v VARCHAR(64),b BLOB(8192))");
    check(ok(db,sid,"SELECT "+std::string(127,'(')+"1"+std::string(127,')'))["rows"][0][0]==1,"Existing127 parentheses execute normally");std::string nots;for(int i=0;i<127;++i)nots+="NOT ";check(ok(db,sid,"SELECT "+nots+"TRUE")["rows"][0][0]==false,"Existing127 NOTs execute normally");
    check(ok(db,sid,"\xef\xbb\xbfSELECT 42")["rows"][0][0]==42,"Native SQL BOM is retained");check(ok(db,sid,"SELECT (SELECT (SELECT 7))")["rows"][0][0]==7,"Normal nested scalar query remains supported");
    check(ok(db,sid,"/* "+std::string(5000,'(')+" */ SELECT 42 -- (((\n")["rows"][0][0]==42,"SQL delimiter scan skips comments");check(ok(db,sid,"SELECT '"+std::string(5000,'(')+"中文' AS \"(((alias\"")["rows"][0][0].get<std::string>().size()>5000,"SQL delimiter scan skips quoted values and identifiers");
    std::string batch="INSERT INTO t VALUES";for(int n=0;n<1000;++n){if(n)batch+=',';batch+='('+std::to_string(n)+",'中文"+std::to_string(n)+"',X'00FF')";}check(ok(db,sid,batch)["affected_rows"]==1000,"Normal actual1000-row SQL batch remains atomic");
    ok(db,sid,"INSERT INTO t VALUES(-9223372036854775808,'最小',X'00'),(9223372036854775807,'最大',X'FF')");check(ok(db,sid,"SELECT id FROM t WHERE id<0")["rows"][0][0]==std::numeric_limits<std::int64_t>::min(),"Int64 minimum is not rounded or rejected");
    auto imported=db.import_data(sid,{{"format","csv"},{"table","t"},{"content","\xef\xbb\xbfid,v,b\r\n1001,\"含逗号,中文\",00FF\r\n1002,\"引号\"\"与\r\n换行\",FFFF\r\n1003,\\N,\\N\r\n1004,\"\\N\",00\r\n"}});check(imported.value("ok",false)&&imported["affected_rows"]==4,"CSV BOM/quote/newline/NULL/BLOB import remains supported");
    auto values=ok(db,sid,"SELECT id,v,b FROM t WHERE id>=1001 AND id<1005 ORDER BY id")["rows"];check(values[0][1]=="含逗号,中文"&&values[1][1]=="引号\"与\r\n换行"&&values[2][1].is_null()&&values[2][2].is_null()&&values[3][1]=="\\N"&&values[0][2]==json{{"$blob","00FF"}},"Imported values preserve text/NULL/hex distinction");
    auto csv_failure=db.import_data(sid,{{"format","csv"},{"table","t"},{"content","id,v,b\n2000,先插入,00\nbad,类型错,FF\n"}});check(!csv_failure.value("ok",true)&&csv_failure["error"]["code"]=="TYPE_ERROR"&&csv_failure["error"]["input_record"]==3&&csv_failure["atomic"]==true,"CSV type failure identifies record and rolls back the entire import");check(ok(db,sid,"SELECT COUNT(*) FROM t WHERE id=2000")["rows"][0][0]==0,"CSV valid prefix is not committed");
    const std::vector<std::string> attacks={"SELECT "+std::string(50000,'(')+"1"+std::string(50000,')'),"SELECT "+std::string(50000,'+')+"1",[&](){std::string x="SELECT ";for(int n=0;n<50000;++n)x+="NOT ";return x+"TRUE";}(),[&](){std::string x="SELECT 1 WHERE TRUE";for(int n=0;n<50000;++n)x+=" AND TRUE";return x;}(),[&](){std::string x="SELECT 1 WHERE FALSE";for(int n=0;n<50000;++n)x+=" OR FALSE";return x;}(),[&](){std::string x="SELECT 1";for(int n=0;n<50000;++n)x+="+1";return x;}(),[&](){std::string x="SELECT 1";for(int n=0;n<50000;++n)x+=" UNION ALL SELECT 1";return x;}(),[&](){std::string x="SELECT 1";for(int n=0;n<50;++n)x="SELECT ("+x+")";return x;}()};
    {std::string combined="SELECT 1 WHERE TRUE";for(int level=0;level<4;++level){combined="SELECT 1 WHERE EXISTS("+combined+")";for(int i=0;i<126;++i)combined+=" AND TRUE";}bad(db,sid,combined,"QUERY_TOO_COMPLEX");check(ok(db,sid,"SELECT 42")["rows"][0][0]==42,"Combined query/expression ownership guard leaves session alive after destruction");}
    for(const auto& sql:attacks){bad(db,sid,sql,"QUERY_TOO_COMPLEX");check(ok(db,sid,"SELECT 42")["rows"][0][0]==42,"Service/native session is alive after rejection and AST destruction");}
    std::string forest="SELECT ";for(int n=0;n<2000;++n){if(n)forest+=',';forest+=nots+"TRUE";}
    ok(db,sid,"BEGIN");ok(db,sid,"INSERT INTO t VALUES(3000,'事务中',X'AA')");bad(db,sid,forest,"QUERY_TOO_COMPLEX");bad(db,sid,"COMMIT","TRANSACTION_ABORTED");bad(db,sid,"SELECT 1","TRANSACTION_ABORTED");ok(db,sid,"ROLLBACK");check(ok(db,sid,"SELECT COUNT(*) FROM t WHERE id=3000")["rows"][0][0]==0,"Explicit transaction is aborted and rollback removes the earlier private write");
    auto script_failure=db.import_data(sid,{{"format","sql"},{"content","INSERT INTO t VALUES(4000,'前缀',X'00');SELECT "+std::string(5000,'(')+"1;"}});check(!script_failure.value("ok",true)&&script_failure["error"]["code"]=="QUERY_TOO_COMPLEX"&&script_failure["error"]["input_statement"]==2&&script_failure["error"]["position"].get<int>()>=0,"SQL import parse failure identifies the local statement and byte");check(ok(db,sid,"SELECT COUNT(*) FROM t WHERE id=4000")["rows"][0][0]==0,"SQL import does not execute a parsed prefix before later parse error");
    std::string script;for(int n=0;n<2000;++n)script+="INSERT INTO t(id) VALUES("+nots+"TRUE);";auto forest_failure=db.import_data(sid,{{"format","sql"},{"content",script}});check(!forest_failure.value("ok",true)&&forest_failure["error"]["code"]=="IMPORT_RESOURCE"&&forest_failure["error"]["input_statement"].get<int>()>1&&forest_failure["transaction_state"]=="idle","Whole-import AST forest is bounded before any DML/type checks");
    json padding=0;for(int n=0;n<65;++n)padding=json::array({std::move(padding)});auto deep=db.import_data(sid,{{"format","csv"},{"table","t"},{"content","id,v,b\n"},{"padding",padding}});check(!deep.value("ok",true)&&deep["error"]["code"]=="REQUEST_TOO_COMPLEX"&&deep["transaction_state"]=="idle","Native request objects use iterative JSON validation");
    auto inspect=db.inspect(sid,{{"table","t"},{"padding",padding}});check(!inspect.value("ok",true)&&inspect["error"]["code"]=="REQUEST_TOO_COMPLEX","Native inspection validates the same shape boundary");
    ok(db,sid,"BEGIN");ok(db,sid,"INSERT INTO t VALUES(5000,'关闭回滚',X'AA')");auto active=db.import_data(sid,{{"format","csv"},{"table","t"},{"content","id,v,b\n"}});check(!active.value("ok",true)&&active["error"]["code"]=="TRANSACTION_ACTIVE","Import still refuses an active explicit transaction");db.close_session(sid);sid=db.open_session();check(ok(db,sid,"SELECT COUNT(*) FROM t WHERE id=5000")["rows"][0][0]==0,"Closing the session rolls back earlier uncommitted data");
    check(ok(db,sid,"VERIFY")["verification"]["ok"]==true,"Boundary errors retain physical/database invariants");db.close_session(sid);std::cout<<"PASS "<<checks<<" actual native SQL/import/transaction/deep-tree destruction boundary checks\n";
    }catch(const std::exception&e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<" data="<<path.u8string()<<"\n";return 1;}
}
