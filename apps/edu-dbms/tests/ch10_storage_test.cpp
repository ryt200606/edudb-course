#include "edudb.hpp"
#include "query_spill.hpp"
#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>
using edudb::json;
using edudb::Storage;
namespace fs=std::filesystem;
namespace {
std::size_t checks=0;
void require(bool ok,const std::string& message){++checks;if(!ok)throw std::runtime_error(message);}
json column(const char* name,const char* type){return {{"name",name},{"type",type},{"length",1000},{"not_null",false},{"primary",false},{"unique",false},{"default",nullptr}};}
json definition(const std::string& method="HEAP"){
    json keys=method=="HEAP"?json::array():json::array({"id"});json options=json::object();
    if(method!="HEAP")options["max_keys"]=7;
    if(method=="HASH_STATIC"||method=="HASH_LINEAR")options["buckets"]=4;
    return {{"columns",json::array({column("id","INTEGER"),column("note","VARCHAR")})},{"rows",json::array()},
        {"organization",{{"method",method},{"columns",keys},{"options",options}}},
        {"indexes",json::array({json{{"name","ix_id"},{"column","id"},{"columns",json::array({"id"})},{"unique",true},{"method","BPLUS"},{"include",json::array()},{"options",{{"max_keys",7}}}}})}};
}
std::vector<edudb::StoredRow> scan(Storage& db,const std::shared_ptr<edudb::Transaction>& tx){
    auto c=db.scan(tx,"t");edudb::StoredRow row;std::vector<edudb::StoredRow> rows;
    while(c->next(row))rows.push_back(row);
    return rows;
}
void versions(const fs::path& path){
    std::string identity;
    {Storage db(path.u8string(),4);auto workspace=db.query_workspace();identity=workspace.second;
        require(fs::path(workspace.first).is_absolute()&&fs::equivalent(fs::u8path(workspace.first),path),"workspace directory");
        require(identity.size()==32,"workspace identity");auto tx=db.begin();db.write_table(tx,"t",definition());
        auto empty=db.read_table(tx,"t",false);require(empty["data_version"]==0&&empty["schema_version"]==1,"new table versions");
        for(int i=0;i<120;++i)db.insert_row(tx,"t",json::array({i,"学生"+std::to_string(i)}));
        auto meta=db.read_table(tx,"t",false);require(meta["row_count"]==120&&meta["data_version"]>empty["data_version"],"insert data version");
        auto shape=db.inspect(tx,"t","ix_id",2048).at("structure");auto measured=meta["physical_stats"]["indexes"][0];
        require(measured["pages"]==shape["pages"].size()&&measured["leaf_pages"]==shape["leaf_pages"],"exact BPLUS page/leaf counts");
        json stats={{"analyzed_rows",120},{"data_version",meta["data_version"]},{"schema_version",meta["schema_version"]},{"columns",json::array()}};
        db.set_statistics(tx,"t",stats);auto fresh=db.read_table(tx,"t",false);require(fresh["statistics"]["analyzed_rows"]==120&&!fresh["statistics"]["stale"].get<bool>(),"statistics fresh in own transaction");db.commit(tx);
    }
    {Storage db(path.u8string(),3);require(db.query_workspace().second==identity,"stable workspace identity");auto tx=db.begin();auto meta=db.read_table(tx,"t",false);
        require(meta["statistics"]["analyzed_rows"]==120&&!meta["statistics"]["stale"].get<bool>(),"statistics persisted after reopen");
        auto repeated=db.read_table(tx,"t",false);require(repeated["physical_stats"]["cache_hit"]==true&&repeated["physical_stats"]["metadata_io"]["total_page_reads"]==0,"same transaction exact-summary cache");
        auto rows=scan(db,tx);auto version=meta["data_version"];db.update_row(tx,"t",rows[0].id,rows[0].values);
        require(db.read_table(tx,"t",false)["data_version"]==version,"unchanged UPDATE does not bump version");
        require(db.transaction_stats(tx)["dirty_pages"]==0,"unchanged UPDATE makes no private dirty pages");db.commit(tx);
        tx=db.begin();rows=scan(db,tx);auto changed=rows[0].values;changed[1]="changed";db.update_row(tx,"t",rows[0].id,changed);
        require(db.read_table(tx,"t",false)["statistics"]["stale"]==true,"changed UPDATE stales statistics");db.rollback(tx);
        tx=db.begin();require(db.read_table(tx,"t",false)["data_version"]==version,"data version rollback");
        require(db.read_table(tx,"t",false)["statistics"]["stale"]==false,"statistics freshness rollback");
        db.set_statistics(tx,"t",json{{"analyzed_rows",999}});db.rollback(tx);tx=db.begin();require(db.read_table(tx,"t",false)["statistics"]["analyzed_rows"]==120,"statistics write rollback");
        rows=scan(db,tx);changed=rows[0].values;changed[1]="changed";db.update_row(tx,"t",rows[0].id,changed);db.commit(tx);
        tx=db.begin();meta=db.read_table(tx,"t",false);require(meta["data_version"]>version&&meta["statistics"]["stale"]==true,"committed UPDATE version and stale");
        auto data=meta["data_version"],schema=meta["schema_version"],physical=meta["physical_version"];db.reindex(tx,"t","ix_id");auto rebuilt=db.read_table(tx,"t",false);
        require(rebuilt["data_version"]==data&&rebuilt["schema_version"]==schema&&rebuilt["physical_version"]>physical,"REINDEX physical version only");db.rollback(tx);
        tx=db.begin();auto rewrite=db.read_table(tx,"t",true);data=rewrite["data_version"];schema=rewrite["schema_version"];physical=rewrite["physical_version"];db.write_table(tx,"t",rewrite);auto same=db.read_table(tx,"t",false);
        require(same["data_version"]==data&&same["schema_version"]==schema&&same["physical_version"]>physical,"identity-preserving rewrite versions");db.rollback(tx);
        tx=db.begin();rewrite=db.read_table(tx,"t",true);data=rewrite["data_version"];schema=rewrite["schema_version"];rewrite["columns"].push_back(column("age","INTEGER"));for(auto& row:rewrite["rows"])row.push_back(20);db.write_table(tx,"t",rewrite);auto altered=db.read_table(tx,"t",false);
        require(altered["schema_version"]>schema&&altered["data_version"]>data&&altered["statistics"]["stale"]==true,"schema rewrite versions and stale");db.rollback(tx);
        tx=db.begin();require(db.read_table(tx,"t",false)["schema_version"]==schema,"DDL version rollback");db.commit(tx);
    }
}
void organizations(const fs::path& root){
    for(const std::string method:{"HEAP","ORDERED","BPLUS","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR"}){
        Storage db((root/fs::u8path(method)).u8string(),3);auto tx=db.begin();db.write_table(tx,"t",definition(method));
        for(int i=0;i<50;++i)db.insert_row(tx,"t",json::array({i,std::string(100,'a')}));
        auto before=db.transaction_stats(tx).at("index_page_reads");
        auto planned=db.read_table_for_plan(tx,"t");
        if(method=="HEAP"){
            require(before==db.transaction_stats(tx).at("index_page_reads"),"HEAP planner reads its owning descriptor");
        }else{
            require(planned.at("physical_stats").at("exact")&&planned.at("physical_stats").contains("row_map"),
                    method+" retains full transaction-visible organization measurement");
        }
        auto meta=db.read_table(tx,"t",false);auto shape=db.inspect(tx,"t","__organization",2048).at("structure");
        require(planned["physical_stats"]["table_pages"]==meta["physical_stats"]["table_pages"]&&
                planned["index_stats"]==meta["index_stats"]&&meta["physical_stats"].contains("row_map"),
                method+" retains exact planner inputs and public diagnostics");
        require(meta["physical_stats"]["table_pages"]==shape["pages"].size(),method+" exact table organization pages");
        require(meta["physical_stats"]["table_pages"]>0,method+" nonzero physical pages");db.commit(tx);
    }
}
void snapshots(const fs::path& path){
    Storage db(path.u8string(),3);auto init=db.begin();db.write_table(init,"t",definition());for(int i=0;i<80;++i)db.insert_row(init,"t",json::array({i,"before"}));
    auto expected=db.read_table(init,"t",false);db.set_statistics(init,"t",json{{"analyzed_rows",80}});db.commit(init);
    edudb::TransactionOptions options;options.protocol=edudb::TransactionProtocol::Snapshot;options.read_only=true;
    auto old=db.begin(options);db.refresh_read_view(old,{{"t",true,false}});
    edudb::TransactionOptions writer_options;writer_options.protocol=edudb::TransactionProtocol::Snapshot;
    auto writer=db.begin(writer_options);db.refresh_read_view(writer,{{"t",false,true}});for(int i=80;i<180;++i)db.insert_row(writer,"t",json::array({i,"after"}));db.commit(writer);
    auto historical=db.read_table(old,"t",false);require(historical["row_count"]==80&&historical["data_version"]==expected["data_version"],"snapshot metadata version");
    require(historical["physical_stats"]["indexes"][0]["pages"]==expected["physical_stats"]["indexes"][0]["pages"],"snapshot exact physical pages, not global latest cache");
    require(historical["statistics"]["stale"]==false,"historical statistics fresh in historical view");db.commit(old);
    auto current=db.begin();auto latest=db.read_table(current,"t",false);require(latest["row_count"]==180&&latest["statistics"]["stale"]==true,"latest statistics stale separately");
    require(latest["physical_stats"]["indexes"][0]["pages"]>historical["physical_stats"]["indexes"][0]["pages"],"latest physical tree larger");db.commit(current);
}
std::uint32_t crc(const std::vector<unsigned char>& data,std::size_t count){std::uint32_t c=~0u;for(std::size_t i=0;i<count;++i){c^=data[i];for(int k=0;k<8;++k)c=(c>>1)^((c&1)?0xedb88320u:0);}return ~c;}
void put32(std::vector<unsigned char>& b,std::size_t p,std::uint32_t n){for(unsigned i=0;i<4;++i)b[p+i]=static_cast<unsigned char>(n>>(8*i));}
void put64(std::vector<unsigned char>& b,std::size_t p,std::uint64_t n){for(unsigned i=0;i<8;++i)b[p+i]=static_cast<unsigned char>(n>>(8*i));}
void write(const fs::path& path,const std::vector<unsigned char>& bytes){std::ofstream f(path,std::ios::binary);f.write(reinterpret_cast<const char*>(bytes.data()),static_cast<std::streamsize>(bytes.size()));if(!f)throw std::runtime_error("fixture write");}
void query_cleanup(const fs::path& path){
    std::vector<unsigned char> sealed;std::uint64_t run_id=0;std::string identity;
    const auto filename=[&](char token){return path/("edudb-query-"+std::string(64,token)+"-"+std::to_string(run_id)+".tmp");};
    {Storage db(path.u8string(),3);identity=db.query_workspace().second;
        {edudb::QuerySpillStore store(path.u8string(),identity);auto writer=store.create();writer->append(json::array({1,"中文"}));run_id=writer->finish().id;writer.reset();
            for(const auto& item:fs::directory_iterator(path))if(item.path().filename().u8string().rfind("edudb-query-",0)==0){std::ifstream f(item.path(),std::ios::binary);sealed.assign(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());}
        }
        require(sealed.size()>80,"real QuerySpillStore sealed fixture");write(filename('a'),sealed);
        auto unfinished=sealed;unfinished.resize(85);put64(unfinished,48,0);put64(unfinished,56,80);put32(unfinished,68,0);put32(unfinished,76,0);put32(unfinished,76,crc(unfinished,80));write(filename('b'),unfinished);
        auto bad=sealed;bad[76]^=1;write(filename('c'),bad);
        auto foreign=sealed;foreign[8]=foreign[8]=='0'?'1':'0';put32(foreign,76,0);put32(foreign,76,crc(foreign,80));write(filename('d'),foreign);
        auto damaged=sealed;damaged.back()^=1;write(filename('e'),damaged);write(path/"edudb-query-not-owned.tmp",sealed);
        bool locked=false;try{Storage second(path.u8string());}catch(const edudb::DbError& e){locked=e.code=="DATABASE_LOCKED";}require(locked&&fs::exists(filename('a')),"active database cannot run startup cleanup");
    }
    {Storage db(path.u8string(),3);require(!fs::exists(filename('a'))&&!fs::exists(filename('b')),"valid sealed/unsealed orphan runs cleaned");
        require(fs::exists(filename('c'))&&fs::exists(filename('d'))&&fs::exists(filename('e'))&&fs::exists(path/"edudb-query-not-owned.tmp"),"corrupt/foreign/unrecognized runs retained");
        auto diag=db.diagnostics();require(diag["stale_query_files_removed"]==2&&diag["unrecognized_query_files_retained"]>=4,"cleanup diagnostics");
    }
}
}
int main(){
    const auto parent=fs::temp_directory_path();const auto root=parent/("edudb-ch10-stage1-"+std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count()));
    try{require(!fs::exists(root),"fresh test directory");fs::create_directory(root);std::ofstream(root/"owned-test.marker")<<"ch10 storage test";
        versions(root/"versions");organizations(root/"organizations");snapshots(root/"snapshots");query_cleanup(root/"cleanup");
        require(fs::equivalent(root.parent_path(),parent)&&root.filename().u8string().rfind("edudb-ch10-stage1-",0)==0&&fs::exists(root/"owned-test.marker"),"bounded owned cleanup target");
        fs::remove_all(root);std::cout<<"PASS ch10_storage "<<checks<<" checks: versions/stats/physical pages/snapshot/orphan ownership\n";return 0;
    }catch(const std::exception& e){std::cerr<<"FAIL ch10_storage: "<<e.what()<<"; isolated evidence retained at "<<root.u8string()<<"\n";return 1;}
}
