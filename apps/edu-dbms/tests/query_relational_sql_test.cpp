#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool v,const std::string& s){++checks;if(!v)throw std::runtime_error(s);}
static json ok(edudb::Engine& db,const std::string& session,const std::string& sql){auto r=db.execute(session,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static void rows(edudb::Engine& db,const std::string& session,const std::string& sql,const json& expected){auto r=ok(db,session,sql);check(r["rows"]==expected,sql+" => actual "+r["rows"].dump()+" expected "+expected.dump());}
static void error(edudb::Engine& db,const std::string& session,const std::string& sql,const std::string& code){auto r=db.execute(session,sql);check(!r.value("ok",true)&&r["error"]["code"]==code,sql+" => "+r.dump());}
int main(){
    auto dir=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB真实集合外连接_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{
        edudb::Engine db(dir.u8string(),4);auto sid=db.open_session();
        ok(db,sid,"CREATE TABLE sa(k INTEGER,v VARCHAR(8))");ok(db,sid,"CREATE TABLE sb(k REAL,v VARCHAR(8))");
        ok(db,sid,"INSERT INTO sa VALUES(NULL,'n'),(NULL,'n'),(1,'a'),(1,'a'),(1,'a'),(2,'b')");
        ok(db,sid,"INSERT INTO sb VALUES(NULL,'n'),(1.0,'a'),(1.0,'a'),(3.0,'c'),(3.0,'c')");
        for(const auto& op:{"UNION","INTERSECT","EXCEPT"})for(bool all:{false,true}){
            auto sql="SELECT k,v FROM sa "+std::string(op)+(all?" ALL ":" ")+"SELECT k,v FROM sb ORDER BY k,v";
            auto r=ok(db,sid,sql);std::size_t expected=std::string(op)=="UNION"?(all?11:4):std::string(op)=="INTERSECT"?(all?3:2):(all?3:1);
            check(r["rows"].size()==expected,"SQL set multiplicity matches independent fixed fixture");
            check(r["columns"][0]["type"]=="REAL","Common numeric schema is explicit");
            for(const auto& row:r["rows"])if(!row[0].is_null())check(row[0].is_number_float(),"INTEGER operands are really converted, not hidden by json numeric equality");
            check(r["plan"].contains("physical_actual"),"Actual SQL set operator executed");
        }
        rows(db,sid,"SELECT 1 AS k UNION SELECT 2 INTERSECT SELECT 2 ORDER BY k",json::array({json::array({1}),json::array({2})}));
        rows(db,sid,"SELECT 1 AS k UNION SELECT 2 EXCEPT SELECT 1 EXCEPT SELECT 2",json::array());
        rows(db,sid,"SELECT 1 AS k UNION (SELECT 2 EXCEPT SELECT 2)",json::array({json::array({1})}));
        auto lazy=ok(db,sid,"SELECT 7 AS k UNION ALL SELECT 1/0 LIMIT 1");check(lazy["rows"].size()==1&&lazy["rows"][0][0]==7.0&&lazy["plan"]["operand_actual"][1].is_null(),"UNION ALL does not evaluate an unopened right operand after LIMIT");
        auto explained=ok(db,sid,"EXPLAIN SELECT k FROM sa UNION SELECT k FROM sb");check(!explained["plan"].contains("physical_actual"),"Set EXPLAIN binds both schemas without executing data operators");
        error(db,sid,"SELECT 1,2 UNION SELECT 3","COLUMN_COUNT_MISMATCH");error(db,sid,"SELECT 'x' UNION SELECT 1","TYPE_ERROR");
        ok(db,sid,"CREATE TABLE la(id INTEGER PRIMARY KEY,k INTEGER,tag VARCHAR(8))");ok(db,sid,"CREATE TABLE rb(id INTEGER PRIMARY KEY,k INTEGER,tag VARCHAR(8))");
        ok(db,sid,"INSERT INTO la VALUES(1,1,'a'),(2,2,'b'),(3,NULL,'c'),(4,1,'d'),(5,4,'e')");
        ok(db,sid,"INSERT INTO rb VALUES(11,1,'u'),(12,1,'v'),(13,3,'w'),(14,NULL,'x')");
        rows(db,sid,"SELECT l.id,r.id FROM la l LEFT JOIN rb r ON l.k=r.k ORDER BY l.id,r.id",json::array({json::array({1,11}),json::array({1,12}),json::array({2,nullptr}),json::array({3,nullptr}),json::array({4,11}),json::array({4,12}),json::array({5,nullptr})}));
        rows(db,sid,"SELECT r.id,l.id FROM la l RIGHT OUTER JOIN rb r ON l.k=r.k ORDER BY r.id,l.id",json::array({json::array({11,1}),json::array({11,4}),json::array({12,1}),json::array({12,4}),json::array({13,nullptr}),json::array({14,nullptr})}));
        auto full=ok(db,sid,"SELECT l.id,r.id FROM la l FULL JOIN rb r ON l.k=r.k ORDER BY l.id IS NULL,l.id,r.id");check(full["rows"].size()==9,"FULL outer keeps duplicate matches and both unmatched sets");check(full["plan"]["optimizer"]["unsafe_predicate_pushdown_disabled"]==true&&full["plan"]["optimizer"]["reordering_disabled"]==true,"Outer boundaries are not passed to inner-only rewrites");
        rows(db,sid,"SELECT l.id FROM la l LEFT JOIN rb r ON l.k=r.k WHERE r.id IS NULL ORDER BY l.id",json::array({json::array({2}),json::array({3}),json::array({5})}));
        rows(db,sid,"SELECT l.id,r.id FROM la l LEFT JOIN rb r ON l.k=r.k AND r.id=11 ORDER BY l.id",json::array({json::array({1,11}),json::array({2,nullptr}),json::array({3,nullptr}),json::array({4,11}),json::array({5,nullptr})}));
        rows(db,sid,"SELECT l.id,r.id FROM la l LEFT JOIN rb r ON l.k=r.k AND l.id=2 ORDER BY l.id",json::array({json::array({1,nullptr}),json::array({2,nullptr}),json::array({3,nullptr}),json::array({4,nullptr}),json::array({5,nullptr})}));
        ok(db,sid,"CREATE TABLE empty_side(id INTEGER)");rows(db,sid,"SELECT l.id,r.id FROM la l LEFT JOIN empty_side r ON l.id=r.id ORDER BY l.id",json::array({json::array({1,nullptr}),json::array({2,nullptr}),json::array({3,nullptr}),json::array({4,nullptr}),json::array({5,nullptr})}));
        rows(db,sid,"SELECT COUNT(*) FROM la CROSS JOIN rb",json::array({json::array({20})}));
        ok(db,sid,"CREATE TABLE lc(id INTEGER PRIMARY KEY)");ok(db,sid,"INSERT INTO lc VALUES(2),(3)");
        rows(db,sid,"SELECT l.id,r.id,c.id FROM la l LEFT JOIN rb r ON l.k=r.k JOIN lc c ON l.id=c.id ORDER BY l.id",json::array({json::array({2,nullptr,2}),json::array({3,nullptr,3})}));
        error(db,sid,"SELECT l.id FROM la l LEFT JOIN rb r ON l.k=r.k USE JOIN(HASH)","INVALID_JOIN_METHOD");
        rows(db,sid,"SELECT 40+2",json::array({json::array({42})}));db.close_session(sid);
        std::cout<<"PASS "<<checks<<" actual Engine SQL checks: sets/common types/precedence/outer/NULL/WHERE/ON/empty/cross; no performance claim\n";return 0;
    }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<"; preserved "<<dir.u8string()<<"\n";return 1;}
}
