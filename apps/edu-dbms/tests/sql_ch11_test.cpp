#include "edudb.hpp"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <random>
#include <stdexcept>
using edudb::json;
namespace fs=std::filesystem;
static std::size_t checks=0;
static void require(bool v,const std::string& why){++checks;if(!v)throw std::runtime_error(why);}
static json ok(edudb::Engine& db,const std::string& sid,const std::string& sql){auto r=db.execute(sid,sql);require(r.value("ok",false),sql+" => "+r.dump());return r;}
static void rows(edudb::Engine& db,const std::string& sid,const std::string& sql,const json& expected){require(ok(db,sid,sql)["rows"]==expected,"Wrong results: "+sql);}
static void bad(edudb::Engine& db,const std::string& sid,const std::string& sql,const std::string& code){auto r=db.execute(sid,sql);require(!r.value("ok",true)&&r["error"]["code"]==code,"Expected "+code+": "+sql+" => "+r.dump());}
struct EnvironmentLimit{
    std::string name;std::optional<std::string> previous;
    static void assign(const std::string& n,const std::optional<std::string>& value){
#ifdef _WIN32
        _putenv_s(n.c_str(),value?value->c_str():"");
#else
        if(value)setenv(n.c_str(),value->c_str(),1);else unsetenv(n.c_str());
#endif
    }
    EnvironmentLimit(std::string n,std::string value):name(std::move(n)){if(const char* p=std::getenv(name.c_str()))previous=p;assign(name,value);}
    ~EnvironmentLimit(){assign(name,previous);}
};
int main(int argc,char** argv){
    fs::path directory;
    json blob_io=json::object();
    try{
        directory=argc>1?fs::u8path(argv[1]):fs::temp_directory_path()/("edudb-ch11-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        require(!fs::exists(directory),"Test requires a fresh isolated directory");
        {
            edudb::Engine db(directory.u8string(),8);auto s=db.open_session();
            bad(db,s,"SELECT "+std::string(129,'(')+"1"+std::string(129,')'),"QUERY_TOO_COMPLEX");
            bad(db,s,"SELECT "+std::string(500001,','),"QUERY_TOO_COMPLEX");
            bad(db,s,"SELECT "+std::string(500001,'+'),"QUERY_TOO_COMPLEX");
            rows(db,s,"SELECT 40+2",json::array({json::array({42})}));
            rows(db,s,"SELECT 9007199254740993=9007199254740992.0,9007199254740993>9007199254740992.0,9223372036854775807<9223372036854775808.0,-9223372036854775808=-9223372036854775808.0",json::array({json::array({false,true,true,true})}));
            for(const auto& method:{"BPLUS","BTREE","DENSE","SPARSE","MULTILEVEL","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR","BITMAP","BITMAP_HYBRID"}){
                auto table="m_"+std::string(method);auto ix="ix_"+std::string(method);for(auto* n:{&table,&ix})for(auto& c:*n)if(c>='A'&&c<='Z')c=static_cast<char>(c-'A'+'a');
                ok(db,s,"CREATE TABLE "+table+" (id INTEGER PRIMARY KEY,k VARCHAR(16),v INTEGER) ORGANIZATION ORDERED(k)"+(std::string(method)=="SPARSE"?" WITH(multilevel=FALSE)":""));
                std::string insert="INSERT INTO "+table+" VALUES ";json expected=json::array();
                for(int i=0;i<80;++i){if(i)insert+=",";insert+="("+std::to_string(i)+",'g"+std::to_string(i%8)+"',"+std::to_string(i*3)+")";if(i%8==1)expected.push_back(json::array({i,i*3}));}
                ok(db,s,insert);auto include=std::string(method)=="BPLUS"||std::string(method)=="BTREE"?" INCLUDE(id,v)":"";auto options=std::string(method)=="BITMAP"?" WITH(hybrid=FALSE)":"";ok(db,s,"CREATE INDEX "+ix+" ON "+table+" USING "+method+"(k)"+include+options);
                auto forced=ok(db,s,"EXPLAIN ANALYZE SELECT id,v FROM "+table+" USE INDEX("+ix+") WHERE k='g1'");require(forced["plan"]["sources"][0]["index"]==ix&&forced["plan"]["sources"][0]["scan"]!="SeqScan","Forced method actually used");
                rows(db,s,"SELECT id,v FROM "+table+" USE INDEX("+ix+") WHERE k='g1' ORDER BY id",expected);
                auto inspected=ok(db,s,"INSPECT INDEX "+ix);require(inspected["inspection"].is_object(),"Real index inspection object");
                ok(db,s,"UPDATE "+table+" SET k='g1' WHERE id=0");ok(db,s,"DELETE FROM "+table+" WHERE id=1");
                expected.erase(expected.begin());expected.insert(expected.begin(),json::array({0,0}));
                rows(db,s,"SELECT id,v FROM "+table+" USE INDEX("+ix+") WHERE k='g1' ORDER BY id",expected);
                ok(db,s,"REINDEX INDEX "+ix);rows(db,s,"SELECT id,v FROM "+table+" USE INDEX("+ix+") WHERE k='g1' ORDER BY id",expected);
                ok(db,s,"DROP INDEX "+ix);
            }
            ok(db,s,"CREATE TABLE typed(id INTEGER PRIMARY KEY,department VARCHAR(20),score REAL,payload VARCHAR(100))");
            ok(db,s,"INSERT INTO typed VALUES(1,'计算机',1.25,'一'),(2,'计算机',2.5,'二'),(3,'电子',2.5,'三'),(4,NULL,9,'空')");
            ok(db,s,"CREATE UNIQUE INDEX typed_pair ON typed USING BPLUS(department,score) INCLUDE(id,payload) WITH(max_keys=3,prefix_compression=TRUE)");
            rows(db,s,"SELECT id,payload FROM typed WHERE department='计算机' AND score>=1.5 ORDER BY id",json::array({json::array({2,"二"})}));
            bad(db,s,"INSERT INTO typed VALUES(5,'计算机',2.5,'冲突')","UNIQUE_VIOLATION");
            ok(db,s,"INSERT INTO typed VALUES(5,NULL,9,'可重复NULL')");
            rows(db,s,"SELECT id FROM typed USE INDEX(typed_pair) WHERE department IS NULL ORDER BY id",json::array({json::array({4}),json::array({5})}));
            rows(db,s,"SELECT id FROM typed USE INDEX(typed_pair) WHERE department IS NOT NULL ORDER BY id",json::array({json::array({1}),json::array({2}),json::array({3})}));
            rows(db,s,"SELECT id FROM typed WHERE department=NULL",json::array());
            ok(db,s,"ALTER TABLE typed RENAME COLUMN department TO dept");
            rows(db,s,"SELECT id FROM typed WHERE dept='计算机' ORDER BY id",json::array({json::array({1}),json::array({2})}));
            bad(db,s,"ALTER TABLE typed DROP COLUMN payload","DEPENDENT_OBJECT");
            ok(db,s,"CREATE TABLE long_search_keys(id INTEGER PRIMARY KEY,k VARCHAR(240))");std::string long_insert="INSERT INTO long_search_keys VALUES ";
            auto long_key=[](int i){auto digits=std::to_string(i);return std::string(150,'x')+std::string(4-digits.size(),'0')+digits+"-remaining-search-key-tail";};
            for(int i=0;i<120;++i){if(i)long_insert+=",";long_insert+="("+std::to_string(i)+",'"+long_key(i)+"')";}ok(db,s,long_insert);
            ok(db,s,"CREATE INDEX compressed_long_keys ON long_search_keys(k) WITH(max_keys=7,prefix_compression=TRUE)");
            auto compressed=ok(db,s,"INSPECT INDEX compressed_long_keys")["inspection"]["structure"];require(compressed["descriptor"]["stats"].value("bulk_truncated_separators",0)>0,"SQL sorted builder stores real truncated separator prefixes");
            rows(db,s,"SELECT id FROM long_search_keys USE INDEX(compressed_long_keys) WHERE k='"+long_key(63)+"'",json::array({json::array({63})}));
            ok(db,s,"CREATE TABLE hash_options(id INTEGER PRIMARY KEY,k VARCHAR(32),value INTEGER)");
            std::string hash_insert="INSERT INTO hash_options VALUES ";for(int i=0;i<40;++i){if(i)hash_insert+=",";hash_insert+="("+std::to_string(i)+",'Alpha-"+std::to_string(i%11)+"',"+std::to_string(i)+")";}ok(db,s,hash_insert);
            ok(db,s,"CREATE INDEX default_bstar ON hash_options(value) WITH(bstar=TRUE)");
            require(ok(db,s,"INSPECT INDEX default_bstar")["inspection"]["structure"]["descriptor"]["options"]["max_keys"]==31,"B* default fixed count remains page-feasible");
            rows(db,s,"SELECT id FROM hash_options USE INDEX(default_bstar) WHERE value=21",json::array({json::array({21})}));ok(db,s,"DROP INDEX default_bstar");
            ok(db,s,"CREATE INDEX duplicate_alias ON hash_options(k) WITH(duplicates='posting')");
            require(ok(db,s,"INSPECT INDEX duplicate_alias")["inspection"]["structure"]["descriptor"]["options"]["dup_layout"]=="posting","Duplicate option alias reaches actual posting layout");
            rows(db,s,"SELECT id FROM hash_options USE INDEX(duplicate_alias) WHERE k='Alpha-7' ORDER BY id",json::array({json::array({7}),json::array({18}),json::array({29})}));ok(db,s,"DROP INDEX duplicate_alias");
            for(const auto& algorithm:{"fnv1a64","poly31","first_byte","constant"}){
                auto ix="hash_"+std::string(algorithm);ok(db,s,"CREATE INDEX "+ix+" ON hash_options USING HASH_STATIC(k) WITH(hash='"+algorithm+"',buckets=4,max_keys=3)");
                json expected=json::array();for(int i=0;i<40;++i)if(i%11==7)expected.push_back(json::array({i}));
                rows(db,s,"SELECT id FROM hash_options USE INDEX("+ix+") WHERE k='Alpha-7' ORDER BY id",expected);
                auto shape=ok(db,s,"INSPECT INDEX "+ix)["inspection"]["structure"];require(shape["descriptor"]["options"]["hash"]==algorithm,"Hash function option reaches persistent descriptor");
                if(std::string(algorithm)=="constant"||std::string(algorithm)=="first_byte")require(shape["descriptor"]["stats"].value("overflow_pages",0)>0,"Bad hash really causes bucket overflow");
                ok(db,s,"REINDEX INDEX "+ix);rows(db,s,"SELECT id FROM hash_options USE INDEX("+ix+") WHERE k='Alpha-7' ORDER BY id",expected);ok(db,s,"DROP INDEX "+ix);
            }
            bad(db,s,"CREATE INDEX bad_hash_name ON hash_options USING HASH_STATIC(k) WITH(hash='unknown')","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_hash_prefix ON hash_options USING HASH_STATIC(k) WITH(prefix_compression=TRUE)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_hash_buckets ON hash_options USING HASH_EXTENDIBLE(k) WITH(buckets=4)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_hash_depth ON hash_options USING HASH_LINEAR(k) WITH(max_depth=3)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_probe ON hash_options USING BPLUS(k) WITH(probing=TRUE)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_hybrid ON hash_options USING BPLUS(k) WITH(hybrid=TRUE)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_posting ON hash_options USING BTREE(k) WITH(dup_layout='posting')","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_suffix ON hash_options USING BPLUS(k) WITH(suffix_truncation=TRUE)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_redistribution ON hash_options USING BPLUS(k) WITH(redistribution=TRUE)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX conflicting_duplicates ON hash_options USING BPLUS(k) WITH(duplicates='posting',dup_layout='rid')","DUPLICATE_OPTION");
            bad(db,s,"CREATE INDEX bad_locator ON hash_options USING BPLUS(k) WITH(locator='primary')","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_include ON hash_options USING HASH_STATIC(k) INCLUDE(value)","INVALID_INDEX");
            ok(db,s,"CREATE TABLE sparse_prefix(id INTEGER PRIMARY KEY,a VARCHAR(8),b INTEGER) ORGANIZATION ORDERED(a,b) WITH(multilevel=FALSE)");
            ok(db,s,"INSERT INTO sparse_prefix VALUES(1,'x',1),(2,'x',2),(3,'y',1),(4,NULL,7),(5,NULL,8)");
            ok(db,s,"CREATE INDEX sparse_a ON sparse_prefix USING SPARSE(a)");
            bad(db,s,"CREATE INDEX sparse_wrong_capacity ON sparse_prefix USING SPARSE(a) WITH(max_keys=7)","INVALID_INDEX_ORGANIZATION");
            bad(db,s,"CREATE INDEX sparse_wrong_compression ON sparse_prefix USING SPARSE(a) WITH(prefix_compression=FALSE)","INVALID_INDEX_ORGANIZATION");
            rows(db,s,"SELECT id FROM sparse_prefix USE INDEX(sparse_a) WHERE a='x' ORDER BY id",json::array({json::array({1}),json::array({2})}));
            ok(db,s,"UPDATE sparse_prefix SET b=b+10 WHERE a='x'");
            rows(db,s,"SELECT id,b FROM sparse_prefix USE INDEX(sparse_a) WHERE a='x' ORDER BY id",json::array({json::array({1,11}),json::array({2,12})}));
            rows(db,s,"SELECT id FROM sparse_prefix USE INDEX(sparse_a) WHERE a IS NULL ORDER BY id",json::array({json::array({4}),json::array({5})}));
            bad(db,s,"CREATE UNIQUE INDEX sparse_unique_fail ON sparse_prefix USING SPARSE(a)","UNIQUE_VIOLATION");
            ok(db,s,"DELETE FROM sparse_prefix WHERE id=2");ok(db,s,"DROP INDEX sparse_a");ok(db,s,"ALTER TABLE sparse_prefix ORGANIZATION ORDERED(a,b) WITH(multilevel=TRUE)");ok(db,s,"CREATE UNIQUE INDEX sparse_unique ON sparse_prefix USING MULTILEVEL(a)");
            ok(db,s,"INSERT INTO sparse_prefix VALUES(6,NULL,9)");bad(db,s,"INSERT INTO sparse_prefix VALUES(7,'x',99)","UNIQUE_VIOLATION");
            ok(db,s,"CREATE TABLE flags(id INTEGER PRIMARY KEY,a INTEGER,b BOOLEAN)");
            bad(db,s,"CREATE INDEX bad_bitmap_threshold ON flags USING BITMAP(a) WITH(bitmap_threshold=257)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_bitmap_threshold_type ON flags USING BITMAP(a) WITH(bitmap_threshold=TRUE)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_unique_bitmap ON flags USING BITMAP(a) WITH(hybrid='TRUE')","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_disabled_threshold ON flags USING BITMAP(a) WITH(hybrid=FALSE,bitmap_threshold=4)","INVALID_OPTION");
            bad(db,s,"CREATE INDEX bad_alias_hybrid ON flags USING BITMAP_HYBRID(a) WITH(hybrid=FALSE)","INVALID_OPTION");
            ok(db,s,"INSERT INTO flags VALUES(1,1,TRUE),(2,2,FALSE),(3,NULL,TRUE),(4,1,NULL),(5,3,TRUE)");
            ok(db,s,"CREATE INDEX bitmap_a ON flags USING BITMAP(a)");ok(db,s,"CREATE INDEX bitmap_b ON flags USING BITMAP(b)");
            auto count_all=ok(db,s,"SELECT COUNT(*) FROM flags");require(count_all["rows"]==json::array({json::array({5})})&&count_all["plan"]["node"]=="BitmapCount"&&count_all["metrics"]["heap_page_reads"]==0,"Existence bitmap count avoids heap");
            rows(db,s,"SELECT id FROM flags WHERE NOT(a=1) ORDER BY id",json::array({json::array({2}),json::array({5})}));
            rows(db,s,"SELECT id FROM flags WHERE a IS NULL OR b IS NULL ORDER BY id",json::array({json::array({3}),json::array({4})}));
            rows(db,s,"SELECT COUNT(*) FROM flags WHERE a=1 AND b=TRUE",json::array({json::array({1})}));
            ok(db,s,"DELETE FROM flags WHERE id=2");ok(db,s,"INSERT INTO flags VALUES(6,9,FALSE)");
            rows(db,s,"SELECT id FROM flags WHERE NOT(a=1) ORDER BY id",json::array({json::array({5}),json::array({6})}));
            rows(db,s,"SELECT COUNT(*) FROM flags WHERE NOT(a=1) AND (b=TRUE OR b=FALSE)",json::array({json::array({2})}));
            ok(db,s,"CREATE TABLE binary_rows(id INTEGER PRIMARY KEY,data BLOB(100000))");std::string hex;
            for(int i=0;i<65536;++i)hex+="01";
            ok(db,s,"INSERT INTO binary_rows VALUES(1,X'"+hex+"'),(2,NULL),(3,X'')");
            rows(db,s,"SELECT OCTET_LENGTH(data),BLOB_SLICE(data,65535,9) FROM binary_rows WHERE id=1",json::array({json::array({65536,json{{"$blob","0101"}}})}));
            rows(db,s,"SELECT OCTET_LENGTH(data) FROM binary_rows ORDER BY id",json::array({json::array({65536}),json::array({nullptr}),json::array({0})}));
            bad(db,s,"INSERT INTO binary_rows VALUES(4,X'1')","INVALID_BLOB");bad(db,s,"SELECT BLOB_SLICE(X'01',0,1)","FUNCTION_ARGUMENT");
            ok(db,s,"CREATE TABLE csv_rows(id INTEGER PRIMARY KEY,n VARCHAR(80),v REAL,b BOOLEAN,data BLOB)");
            auto imported=db.import_data(s,{{"format","csv"},{"table","csv_rows"},{"content","id,n,v,b,data\r\n9223372036854775807,\"中文,第一行\n第二行\",1.25,true,00FF\r\n2,\"\\N\",\\N,false,\r\n"}});
            require(imported.value("ok",false)&&imported["affected_rows"]==2,"Typed CSV import: "+imported.dump());
            auto exported=db.export_data(s,{{"format","csv"},{"table","csv_rows"}});require(exported.value("ok",false)&&exported["content"].get<std::string>().find("9223372036854775807")!=std::string::npos,"CSV preserves int64");
            auto failed=db.import_data(s,{{"format","csv"},{"table","csv_rows"},{"content","id,n,v,b,data\n9,valid,1,true,00\n2,conflict,1,true,00\n"}});require(!failed.value("ok",true),"Conflicting import fails");
            rows(db,s,"SELECT COUNT(*) FROM csv_rows WHERE id=9",json::array({json::array({0})}));
            ok(db,s,"BEGIN");auto active_import=db.import_data(s,{{"format","csv"},{"table","csv_rows"},{"content",""}});require(!active_import.value("ok",true)&&active_import["transaction_state"]=="active","Import rejection preserves caller transaction");ok(db,s,"ROLLBACK");
            auto script_fail=db.import_data(s,{{"format","sql"},{"content","CREATE TABLE atomic_script(id INTEGER PRIMARY KEY); INSERT INTO atomic_script VALUES(1),(1);"}});require(!script_fail.value("ok",true),"SQL import fails atomically");bad(db,s,"SELECT * FROM atomic_script","TABLE_NOT_FOUND");
            auto tx_control=db.import_data(s,{{"format","sql"},{"content","BEGIN;CREATE TABLE prohibited(id INTEGER);COMMIT;"}});require(!tx_control.value("ok",true),"Import excludes transaction controls");
            auto dump=db.export_data(s,{{"format","sql"},{"table","csv_rows"}});require(dump.value("ok",false),"SQL dump: "+dump.dump());
            ok(db,s,"ALTER TABLE csv_rows RENAME TO original_csv_rows");auto restored=db.import_data(s,{{"format","sql"},{"content",dump["content"]}});require(restored.value("ok",false),"SQL logical restore: "+restored.dump());
            rows(db,s,"SELECT id,n,v,b,OCTET_LENGTH(data) FROM csv_rows ORDER BY id",ok(db,s,"SELECT id,n,v,b,OCTET_LENGTH(data) FROM original_csv_rows ORDER BY id")["rows"]);
            ok(db,s,"CREATE TABLE composite_pk(a VARCHAR(8),b INTEGER,PRIMARY KEY(a,b))");ok(db,s,"INSERT INTO composite_pk VALUES('键',1),('键',2)");
            bad(db,s,"INSERT INTO composite_pk VALUES('键',1)","UNIQUE_VIOLATION");bad(db,s,"INSERT INTO composite_pk VALUES(NULL,9)","NOT_NULL_VIOLATION");
            auto composite_dump=db.export_data(s,{{"format","sql"},{"table","composite_pk"}});require(composite_dump.value("ok",false),"Composite dump");ok(db,s,"DROP TABLE composite_pk");require(db.import_data(s,{{"format","sql"},{"content",composite_dump["content"]}}).value("ok",false),"Composite restore");bad(db,s,"INSERT INTO composite_pk VALUES('键',1)","UNIQUE_VIOLATION");
            // Independent deterministic model checks mutation and index-key
            // changes, rather than checking only counters reported by SQL.
            ok(db,s,"CREATE TABLE model_rows(id INTEGER PRIMARY KEY,k INTEGER,tag VARCHAR(16))");ok(db,s,"CREATE INDEX model_hash ON model_rows USING HASH_EXTENDIBLE(k)");
            std::map<int,std::pair<int,std::string>> model;std::mt19937 rng(20261101);
            for(int step=0;step<120;++step){int id=int(rng()%40),key=int(rng()%13);auto it=model.find(id);if(it==model.end()){auto tag="t"+std::to_string(id);ok(db,s,"INSERT INTO model_rows VALUES("+std::to_string(id)+","+std::to_string(key)+",'"+tag+"')");model[id]={key,tag};}else if(step%3==0){ok(db,s,"DELETE FROM model_rows WHERE id="+std::to_string(id));model.erase(it);}else{ok(db,s,"UPDATE model_rows SET k="+std::to_string(key)+" WHERE id="+std::to_string(id));it->second.first=key;}
                if(step%11==0){json expected=json::array();for(auto& e:model)if(e.second.first==key)expected.push_back(json::array({e.first,e.second.second}));rows(db,s,"SELECT id,tag FROM model_rows WHERE k="+std::to_string(key)+" ORDER BY id",expected);}}
            require(ok(db,s,"VERIFY")["verification"].value("ok",false),"Whole DB verifies real structures and payloads");
            ok(db,s,"CHECKPOINT");db.close_session(s);
        }
        {edudb::Engine db(directory.u8string(),4);auto s=db.open_session();auto length=ok(db,s,"SELECT OCTET_LENGTH(data) FROM binary_rows WHERE id=1");require(length["rows"]==json::array({json::array({65536})}),"BLOB persisted length");auto full=ok(db,s,"SELECT data FROM binary_rows WHERE id=1");require(full["rows"][0][0]["$blob"].get<std::string>().size()==131072,"BLOB actual full payload");auto slice=ok(db,s,"SELECT BLOB_SLICE(data,65535,2) FROM binary_rows WHERE id=1");require(slice["rows"]==json::array({json::array({json{{"$blob","0101"}}})}),"BLOB offset-range persisted result");auto bytes=[](const json& r){return r.at("metrics").at("io").at("files").at("data").at("read_bytes").get<std::uint64_t>();};blob_io={{"length_file_read_bytes",bytes(length)},{"full_file_read_bytes",bytes(full)},{"slice_file_read_bytes",bytes(slice)},{"scope","OS file calls, 4-frame engine cache; OS cache not cleared"}};require(bytes(length)<bytes(full)&&bytes(slice)<bytes(full),"Lazy BLOB metadata/range use fewer file-read bytes than full value");rows(db,s,"SELECT COUNT(*) FROM flags WHERE NOT(a=1)",json::array({json::array({2})}));bad(db,s,"INSERT INTO composite_pk VALUES('键',1)","UNIQUE_VIOLATION");db.close_session(s);}
        {EnvironmentLimit limit("EDUDB_QUERY_MATERIALIZED_BYTE_LIMIT","4096");auto budget_directory=directory;budget_directory+="-blob-budget";edudb::Engine db(budget_directory.u8string(),4);auto s=db.open_session();ok(db,s,"CREATE TABLE blobs(id INTEGER PRIMARY KEY,b BLOB)");std::string hex;for(int i=0;i<8192;++i)hex+="FF";ok(db,s,"INSERT INTO blobs VALUES(1,X'"+hex+"')");rows(db,s,"SELECT OCTET_LENGTH(b) FROM blobs",json::array({json::array({8192})}));bad(db,s,"SELECT b FROM blobs","QUERY_LIMIT_EXCEEDED");rows(db,s,"SELECT 40+2",json::array({json::array({42})}));db.close_session(s);}
        std::cout<<json{{"status","PASS"},{"checks",checks},{"data",directory.u8string()},{"model_seed",20261101},{"blob_io",blob_io}}.dump()<<"\n";return 0;
    }catch(const std::exception& e){std::cerr<<json{{"status","FAIL"},{"checks",checks},{"error",e.what()},{"data",directory.u8string()}}.dump()<<"\n";return 1;}
}
