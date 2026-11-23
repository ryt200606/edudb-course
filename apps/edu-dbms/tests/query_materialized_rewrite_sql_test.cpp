#include "edudb.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <set>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& text){++checks;if(!value)throw std::runtime_error(text);}
static json ok(edudb::Engine& db,const std::string& s,const std::string& sql){auto r=db.execute(s,sql);check(r.value("ok",false),sql+" => "+r.dump());return r;}
static std::multiset<std::string> bag(const json& rows){std::multiset<std::string> result;for(const auto& row:rows)result.insert(row.dump());return result;}
static bool rewritten(const json& response){return response["plan"].value("node",std::string())=="MaterializedViewScan";}
int main(){try{
    const auto directory=std::filesystem::temp_directory_path()/std::filesystem::u8path("EduDB真实物化成本改写_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));edudb::Engine db(directory.u8string(),4);auto s=db.open_session(),writer=db.open_session();
    ok(db,s,"CREATE TABLE big_rows(id INTEGER PRIMARY KEY,g INTEGER,x REAL)");std::string insert="INSERT INTO big_rows VALUES ";for(int n=1;n<=1000;++n){if(n>1)insert+=',';insert+='('+std::to_string(n)+','+std::to_string(n%5)+','+std::to_string(n)+".0)";}ok(db,s,insert);
    const std::string query="SELECT g,COUNT(*) AS n,SUM(x) AS total FROM big_rows GROUP BY g";
    const std::string forced="SELECT g,COUNT(*) AS n,SUM(x) AS total FROM big_rows USE SCAN GROUP BY g";
    ok(db,s,"CREATE MATERIALIZED VIEW summed_rows WITH(refresh='immediate') AS "+query);
    const auto explained=ok(db,s,"EXPLAIN "+query);check(rewritten(explained),"Costed EXPLAIN selects real materialized relation: "+explained.dump());
    check(explained["plan"]["view"]=="summed_rows"&&explained["plan"]["estimated_cost"].get<double>()<explained["plan"]["original_estimated_cost"].get<double>(),"Selected physical estimate is lower");
    check(!explained["plan"]["chosen_scan"].contains("actual"),"Plain EXPLAIN opens no data operators");
    auto actual=ok(db,s,query),reference=ok(db,s,forced);check(rewritten(actual)&&!rewritten(reference)&&bag(actual["rows"])==bag(reference["rows"]),"Real MV scan matches explicit base scan bag");
    check(actual["plan"]["chosen_scan"]["sources"][0]["table"]=="summed_rows"&&actual["metrics"]["rows_scanned"]==5,"Execution actually reads five materialized rows, not merely renamed base plan");
    const auto analyzed=ok(db,s,"EXPLAIN ANALYZE "+query);check(rewritten(analyzed)&&analyzed["plan"]["persistent_freshness_checked_under_locks"]==true,"Analyzed plan proves locked persistent freshness and chosen actual child");
    check(!rewritten(ok(db,s,query+" ORDER BY g")),"Unsupported ORDER tail keeps original correct path");
    const std::string filtered="SELECT g,COUNT(*) AS n,SUM(x) AS total FROM big_rows WHERE id>0 GROUP BY g";
    ok(db,s,"CREATE MATERIALIZED VIEW filtered_rows WITH(refresh='immediate') AS "+filtered);
    check(rewritten(ok(db,s,filtered)),"Exact filtered aggregate can use fresh materialization");
    const auto indexed=ok(db,s,"SELECT g,COUNT(*) AS n,SUM(x) AS total FROM big_rows USE INDEX(__pk_big_rows) WHERE id>0 GROUP BY g");check(!rewritten(indexed)&&indexed["plan"]["sources"][0]["index"]=="__pk_big_rows","Explicit USE INDEX is honored and actually executed");
    const std::string manual_query="SELECT g,COUNT(*) AS n FROM big_rows WHERE x>1000 GROUP BY g";
    ok(db,s,"CREATE MATERIALIZED VIEW manual_rows AS "+manual_query);ok(db,s,"INSERT INTO big_rows VALUES(1001,1,1001.0)");
    const auto stale=ok(db,s,manual_query);check(!rewritten(stale)&&stale["rows"].size()==1&&stale["rows"][0][1]==1,"Stale manual relation cannot supply base-query answers");
    ok(db,s,"CREATE TABLE small_keys(g INTEGER PRIMARY KEY)");ok(db,s,"INSERT INTO small_keys VALUES(0),(1),(2),(3),(4)");
    const std::string joined="SELECT b.g,COUNT(*) AS n FROM big_rows b JOIN small_keys s ON b.g=s.g GROUP BY b.g";
    ok(db,s,"CREATE MATERIALIZED VIEW joined_rows AS "+joined);
    const auto hinted=ok(db,s,"SELECT b.g,COUNT(*) AS n FROM big_rows b JOIN small_keys s ON b.g=s.g USE JOIN(NESTED_LOOP) GROUP BY b.g");check(!rewritten(hinted)&&hinted["plan"]["join_algorithm"]=="NESTED_LOOP","Explicit USE JOIN remains a real requested join path");
    ok(db,writer,"BEGIN");ok(db,writer,"REFRESH MATERIALIZED VIEW summed_rows");
    const auto busy=ok(db,s,query);check(!rewritten(busy)&&bag(busy["rows"])==bag(ok(db,s,forced)["rows"]),"Busy MV candidate is skipped with nowait rather than replacing or failing the base query");ok(db,writer,"ROLLBACK");
    ok(db,s,"BEGIN");ok(db,s,"DROP MATERIALIZED VIEW summed_rows");
    check(!rewritten(ok(db,s,query)),"Old discovery cache entry for a dropped private relation is safely ignored");
    ok(db,s,"CREATE TABLE summed_rows(id INTEGER)");check(!rewritten(ok(db,s,query)),"A reused cached name is re-read and rejected as an ordinary table");ok(db,s,"ROLLBACK");check(rewritten(ok(db,s,query)),"Rollback restores the real candidate and consistent data");
    const std::string subquery="SELECT id,EXISTS(SELECT 1 FROM small_keys s WHERE s.g=big_rows.g) AS found FROM big_rows WHERE id<=10";
    ok(db,s,"CREATE MATERIALIZED VIEW subquery_rows AS "+subquery);
    const auto different=ok(db,s,"SELECT id,EXISTS(SELECT 1 FROM small_keys s WHERE s.g=999) AS found FROM big_rows WHERE id<=10");check(!rewritten(different)&&different["rows"].size()==10,"Different nested AST cannot be accepted by an outer-only fingerprint");for(const auto& row:different["rows"])check(row[1]==false,"Original correlated/uncorrelated Apply truth is preserved");
    const auto large_literal=ok(db,s,"SELECT OCTET_LENGTH('"+std::string(300000,'a')+"') FROM big_rows WHERE id=1");check(!rewritten(large_literal)&&large_literal["rows"]==json::array({json::array({300000})}),"Unrelated cached views cannot turn a large literal into unbounded rewrite fingerprint allocation");
    db.close_session(writer);db.close_session(s);
    const auto legacy_directory=directory.u8string()+"_legacy_semantics";
    const std::string legacy_query="SELECT COUNT(*) AS n,SUM(x) AS total FROM legacy_base";
    {edudb::Engine legacy(legacy_directory,4);auto session=legacy.open_session();ok(legacy,session,"CREATE TABLE legacy_base(x REAL)");std::string values="INSERT INTO legacy_base VALUES ";for(int n=0;n<200;++n){if(n)values+=',';values+='('+std::to_string(n)+".0)";}ok(legacy,session,values);ok(legacy,session,"CREATE MATERIALIZED VIEW legacy_sums AS "+legacy_query);legacy.close_session(session);}
    {edudb::Storage storage(legacy_directory,4);auto tx=storage.begin();auto metadata=storage.read_table(tx,"legacy_sums",false);auto state=metadata["materialized_refresh_state"];state.erase("calculation_semantics");storage.set_materialized_metadata(tx,"legacy_sums",state);storage.commit(tx);}
    {edudb::Engine legacy(legacy_directory,4);auto session=legacy.open_session();const auto unknown=ok(legacy,session,legacy_query);check(!rewritten(unknown),"Unknown historical numeric semantics cannot satisfy rewrite freshness");ok(legacy,session,"REFRESH MATERIALIZED VIEW legacy_sums");check(rewritten(ok(legacy,session,legacy_query)),"Explicit refresh establishes current exact arithmetic version before substitution");legacy.close_session(session);}
    std::cout<<"PASS "<<checks<<" actual costed MV scan/hints/freshness/busy/cache-reuse checks\n";
}catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<"\n";return 1;}}
