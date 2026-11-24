#include "edudb.hpp"
#include <chrono>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <stdexcept>

using edudb::json;
namespace fs=std::filesystem;
static void require(bool ok,const std::string& message){if(!ok)throw std::runtime_error(message);}
static json run(edudb::Engine& db,const std::string& sid,const std::string& sql){
    auto answer=db.execute(sid,sql);
    require(answer.at("ok"),sql.substr(0,120)+" => "+answer.value("error",json()).dump());
    return answer;
}
static bool equal(double a,double b){return std::abs(a-b)<1e-8;}

int main(){
    const auto path=fs::temp_directory_path()/("edudb-joint-mcv-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{
        json observed=json::array(),dump;
        {
            edudb::Engine engine((path/"primary").u8string());
            auto sid=engine.open_session();
            auto sql=[&](const std::string& q){return run(engine,sid,q);};
            auto estimate=[&](const std::string& name,const std::string& predicate,const std::string& hint=""){
                auto result=sql("EXPLAIN ANALYZE SELECT a,b FROM "+name+hint+" WHERE "+predicate);
                return result.at("plan").at("sources")[0];
            };
            for(const auto name:{"paired","shuffled"}){
                const std::string table=name,stats=table+"_ab";
                const bool correlated=table=="paired";
                sql("CREATE TABLE "+table+"(a INTEGER,b INTEGER)");
                for(int batch=0;batch<10;++batch){
                    std::string insert="INSERT INTO "+table+" VALUES ";
                    for(int i=batch*100;i<(batch+1)*100;++i){
                        if(i%100)insert+=",";
                        insert+="("+std::to_string(i%10)+","+std::to_string(correlated?i%10:(i/10)%10)+")";
                    }
                    sql(insert);
                }
                sql("CREATE INDEX "+table+"_a ON "+table+"(a)");
                sql("CREATE INDEX "+table+"_b ON "+table+"(b)");
                sql("ANALYZE "+table+" WITH (sample_rows=1000,target=128)");
                auto before=estimate(table,"a=2 AND b=2");
                require(equal(before.at("estimated_rows"),10),"baseline independent estimate");
                sql("CREATE STATISTICS "+stats+" (mcv) ON a,b FROM "+table);
                require(sql("SHOW STATISTICS FROM "+table)["rows"][0][2]=="not analyzed","definitions await ANALYZE");
                sql("ANALYZE "+table+" WITH (sample_rows=1000,target=128)");
                const double expected=correlated?100:10;
                auto after=estimate(table,"a=2 AND b=2");
                require(equal(after.at("estimated_rows"),expected),"joint estimate");
                require(sql("SELECT COUNT(*) FROM "+table+" WHERE a=2 AND b=2").at("rows")[0][0]==expected,"actual result");
                require(after.at("joint_statistics_used")==json::array({stats}),"explain provenance");
                require(equal(estimate(table,"b=2 AND a=2").at("estimated_rows"),expected),"predicate order");
                auto seq=estimate(table,"a=2 AND b=2"," USE SCAN");
                require(equal(seq.at("estimated_rows"),expected)&&seq.at("estimated_access_entries")==1000,"forced seq estimates");
                auto single=estimate(table,"a=2 AND b=2"," USE INDEX("+table+"_a)");
                require(equal(single.at("estimated_rows"),expected)&&equal(single.at("estimated_access_entries"),100),"forced index entry visits");
                bool intersect=false;
                for(const auto& c:after.at("candidates")){
                    require(equal(c.at("estimated_rows"),expected),"all access paths predict the same output");
                    if(c.at("scan")=="IndexIntersect"){
                        intersect=true;
                        require(equal(c.at("estimated_access_entries"),200)&&equal(c.at("estimated_candidate_rows"),expected),"intersection postings");
                    }
                }
                require(intersect,"intersection was enumerated");
                const auto cross=estimate(table,"a=2 AND b=3").at("estimated_rows").get<double>();
                require(equal(cross,correlated?0:10),"contradictory pair");
                observed.push_back({{"table",table},{"actual",int(expected)},{"before",before["estimated_rows"]},
                    {"after",after["estimated_rows"]},{"cross_pair",cross},{"access",after["scan"]},
                    {"single_index_entries",single["estimated_access_entries"]}});
            }
            sql("BEGIN");sql("INSERT INTO paired VALUES(2,3)");
            require(sql("SHOW STATISTICS FROM paired")["rows"][0][2]=="stale","uncommitted change marks stats stale");
            require(estimate("paired","a=2 AND b=3")["joint_statistics_used"].empty(),"stale excluded");
            sql("ROLLBACK");
            require(sql("SHOW STATISTICS FROM paired")["rows"][0][2]=="fresh","rollback restores fresh stats");
            sql("CREATE TABLE nulls(a INTEGER,b INTEGER)");
            sql("INSERT INTO nulls VALUES(NULL,2),(NULL,NULL),(1,NULL),(1,2),(2,3)");
            sql("CREATE INDEX nulls_a ON nulls USING BITMAP(a)");
            sql("CREATE INDEX nulls_b ON nulls USING BITMAP(b)");
            sql("CREATE STATISTICS nulls_ab ON a,b FROM nulls");
            sql("ANALYZE nulls");
            for(const auto& p:{"a IS NULL AND b IS NULL","NOT(a=1 AND b=2)","a IS NULL AND b=2","a=1 AND b=NULL"}){
                auto actual=sql(std::string("SELECT COUNT(*) FROM nulls WHERE ")+p)["rows"][0][0].get<double>();
                auto plan=estimate("nulls",p);
                require(equal(plan.at("estimated_rows"),actual),std::string("three-valued predicate ")+p);
                for(const auto& candidate:plan["candidates"])
                    require(equal(candidate.at("estimated_rows"),actual),"bitmap estimates use full predicate");
            }
            sql("BEGIN");sql("DROP STATISTICS paired_ab");sql("ROLLBACK");
            require(sql("SHOW STATISTICS FROM paired")["rows"][0][2]=="fresh","statistics DROP rolls back");
            auto bad=engine.execute(sid,"ALTER TABLE paired DROP COLUMN a");
            require(!bad["ok"].get<bool>()&&bad.at("error").at("code")=="DEPENDENT_OBJECT","dependent column protected");
            sql("CREATE TABLE renamed(x INTEGER,y INTEGER,z INTEGER)");
            sql("CREATE STATISTICS dims ON x,y FROM renamed");
            auto dependent=engine.execute(sid,"ALTER TABLE renamed DROP COLUMN x");
            require(!dependent["ok"].get<bool>()&&dependent.at("error").at("code")=="DEPENDENT_OBJECT","statistics-only dependency");
            sql("ALTER TABLE renamed RENAME COLUMN x TO u");
            sql("INSERT INTO renamed VALUES(1,2,8)");
            sql("ANALYZE renamed");
            require(sql("SHOW STATISTICS FROM renamed")["statistics"]["joint_mcv"]["dims"]["columns"]==json::array({"u","y"}),"bound renamed columns");
            sql("DROP STATISTICS dims");
            sql("ALTER TABLE renamed DROP COLUMN u");
            auto old=engine.open_session();
            sql("CREATE TABLE second_name(x INTEGER,y INTEGER)");
            run(engine,old,"BEGIN PROTOCOL SNAPSHOT");
            sql("BEGIN PROTOCOL SNAPSHOT");
            sql("CREATE STATISTICS contested ON y,z FROM renamed");
            sql("COMMIT");
            auto create=engine.execute(old,"CREATE STATISTICS contested ON x,y FROM second_name");
            auto conflict=create.at("ok").get<bool>()?engine.execute(old,"COMMIT"):create;
            require(!conflict.at("ok").get<bool>()&&
                    (conflict["error"]["code"]=="SERIALIZATION_FAILURE"||conflict["error"]["code"]=="STATISTICS_EXISTS"),"global name has one winner");
            run(engine,old,"ROLLBACK");
            require(sql("SHOW STATISTICS FROM second_name")["rows"].empty(),"old snapshot cannot create duplicate global name");
            engine.close_session(old);
            // A sample containing only observed diagonal pairs must not turn
            // their absence in a reservoir into an impossible cross-pair.
            sql("ANALYZE paired WITH (sample_rows=100,target=16)");
            auto sampled=estimate("paired","a=2 AND b=3").at("estimated_rows").get<double>();
            require(sampled>0&&sampled<10,"partial sampling retains uncertainty about unseen pair");
            sql("ANALYZE paired WITH (sample_rows=1000,target=128)");
            dump=engine.export_data(sid,{{"format","sql"},{"table","nulls"}});
            require(dump.at("ok")&&dump.at("content").get<std::string>().find("CREATE STATISTICS")!=std::string::npos,"SQL export");
            engine.close_session(sid);
        }
        {
            edudb::Engine reopened((path/"primary").u8string());
            auto sid=reopened.open_session();
            require(run(reopened,sid,"SHOW STATISTICS FROM shuffled")["rows"][0][2]=="fresh","persistent on restart");
            run(reopened,sid,"DROP STATISTICS shuffled_ab");
            require(run(reopened,sid,"SHOW STATISTICS FROM shuffled")["rows"].empty(),"drop removes definition");
        }
        {
            edudb::Engine target((path/"import").u8string());
            auto sid=target.open_session();
            auto reply=target.import_data(sid,{{"format","sql"},{"content",dump.at("content")}});
            require(reply.at("ok"),reply.value("error",json()).dump());
            require(run(target,sid,"SHOW STATISTICS FROM nulls")["rows"][0][2]=="not analyzed","logical import restores definition");
        }
        fs::remove_all(path);
        std::cout<<observed.dump(2)<<"\nPASS joint statistics\n";
        return 0;
    }catch(const std::exception& error){
        std::cerr<<error.what()<<" ("<<path.u8string()<<")\n";return 1;
    }
}
