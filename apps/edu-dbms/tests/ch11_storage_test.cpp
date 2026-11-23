#include "edudb.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

using edudb::json;
using edudb::RowId;
using edudb::StoredRow;
using edudb::Storage;
namespace fs=std::filesystem;
namespace {
void require(bool condition,const std::string& message){if(!condition)throw std::runtime_error(message);}
json column(const char* name,const char* type){return {{"name",name},{"type",type},{"length",16384},{"not_null",false},{"primary",false},{"unique",false},{"default",nullptr}};}
json index(const char* name,const char* field,bool unique=false,const char* locator="primary"){
    return {{"name",name},{"column",field},{"columns",json::array({field})},{"unique",unique},{"method","BPLUS"},{"include",json::array()},{"options",{{"locator",locator},{"max_keys",7}}}};
}
json definition(const std::string& method,json keys=json::array({"id"}),json options=json::object()){
    if(method=="HEAP")keys=json::array();
    json table={{"columns",json::array({column("id","INTEGER"),column("k","INTEGER"),column("payload","VARCHAR")})},{"rows",json::array()},
        {"organization",{{"method",method},{"columns",keys},{"options",options}}},{"indexes",json::array({index("ix_id","id",true),index("ix_k_direct","k",false,"direct"),index("ix_k_primary","k")})}};
    return table;
}
std::vector<StoredRow> scan(Storage& db,const std::shared_ptr<edudb::Transaction>& tx,const std::string& name,const edudb::ScanSpec& spec={}){
    auto cursor=db.scan(tx,name,spec);std::vector<StoredRow> result;StoredRow row;while(cursor->next(row))result.push_back(row);return result;
}
std::map<std::uint64_t,json> values(const std::vector<StoredRow>& rows){std::map<std::uint64_t,json> result;for(const auto& row:rows){require(row.id.logical_id!=0,"scan omitted logical identity");require(result.emplace(row.id.logical_id,row.values).second,"duplicate logical identity");}return result;}
void verify(Storage& db,const std::shared_ptr<edudb::Transaction>& tx,const std::string& context){auto report=db.verify(tx);require(report.value("ok",false),context+": "+report.dump());}
edudb::ScanSpec point(const std::string& index_name,std::int64_t key){edudb::ScanSpec spec;spec.index=index_name;spec.lower_key=json::array({key});spec.upper_key=spec.lower_key;spec.projection_known=false;return spec;}

void organizations(const fs::path& root){
    const std::vector<std::string> methods={"HEAP","ORDERED","BPLUS","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR"};
    for(std::size_t m=0;m<methods.size();++m){auto directory=root/fs::u8path(methods[m]);std::map<std::uint64_t,json> expected;
        {
            Storage db(directory.u8string(),5);auto tx=db.begin();json options=json::object();if(methods[m]!="HEAP")options["max_keys"]=7;if(methods[m]=="HASH_STATIC"||methods[m]=="HASH_LINEAR")options["buckets"]=8;if(methods[m]=="ORDERED")options["multilevel"]=true;auto table=definition(methods[m],json::array({"id"}),options);db.write_table(tx,"t",table);
            std::vector<int> order(500);for(int i=0;i<500;++i)order[i]=i;std::mt19937 random(110100+static_cast<unsigned>(m));std::shuffle(order.begin(),order.end(),random);
            for(auto i:order){auto row=json::array({i,i%23,std::string(200+i%71,static_cast<char>('a'+i%26))});auto id=db.insert_row(tx,"t",row);require(expected.emplace(id.logical_id,row).second,"insert reused LID");}
            require(values(scan(db,tx,"t"))==expected,methods[m]+" independent initial values");verify(db,tx,methods[m]+" initial graph");
            auto shape=db.inspect(tx,"t","__organization",2048).at("structure");bool full_rows=false;
            for(const auto& page:shape["pages"]){if(methods[m]=="HEAP"){for(const auto& slot:page["rows"])full_rows=full_rows||(slot["record"]["row"].is_array()&&slot["record"]["row"].size()==3);}
                else if(page.contains("entries")&&(page.value("leaf",false)||page.value("kind",std::string())=="hash_bucket"))for(const auto& entry:page["entries"])full_rows=full_rows||(entry[2].is_array()&&entry[2].size()==3);
                if(page.contains("serialized_bytes"))require(page["serialized_bytes"].get<std::size_t>()<=7600,"organization page byte limit");}
            require(full_rows,methods[m]+" pages must contain actual complete rows");db.commit(tx);
        }
        {
            Storage db(directory.u8string(),3);auto tx=db.begin();auto rows=scan(db,tx,"t");require(values(rows)==expected,methods[m]+" exact reopen values");
            for(std::size_t i=0;i<rows.size();++i){if(i%11==0){db.erase_row(tx,"t",rows[i].id);expected.erase(rows[i].id.logical_id);}else if(i%7==0){auto replacement=rows[i].values;replacement[0]=replacement[0].get<int>()+10000;replacement[2]=std::string(400+int(i%83),'z');auto after=db.update_row(tx,"t",rows[i].id,replacement);require(after.logical_id==rows[i].id.logical_id,"key relocation changed stable LID");expected[after.logical_id]=replacement;}}
            verify(db,tx,methods[m]+" updated/direct-reference graph");require(values(scan(db,tx,"t"))==expected,methods[m]+" exact updated model");db.commit(tx);db.checkpoint();
        }
        {Storage db(directory.u8string(),2);auto tx=db.begin();require(values(scan(db,tx,"t"))==expected,methods[m]+" checkpoint/reopen model");verify(db,tx,methods[m]+" checkpoint graph");db.commit(tx);}
        std::cout<<"PASS organization "<<methods[m]<<" 500 random rows, full physical records, key relocation, deletes, two reopens\n";
    }
}

std::map<std::uint64_t,std::uint64_t> direct_pages(Storage& db,const std::shared_ptr<edudb::Transaction>& tx){
    auto shape=db.inspect(tx,"t","ix_k_direct",2048).at("structure");require(!shape.value("truncated",true),"direct-reference inspection incomplete");std::map<std::uint64_t,std::uint64_t> pages;
    for(const auto& page:shape["pages"])if(page.value("leaf",false)&&page.contains("entries"))for(const auto& entry:page["entries"])pages[entry[1].get<std::uint64_t>()]=entry[2]["ref"]["rid"][0].get<std::uint64_t>();
    return pages;
}
void clustered_references(const fs::path& directory){Storage db(directory.u8string(),8);auto tx=db.begin();db.write_table(tx,"t",definition("BPLUS",json::array({"id"}),{{"max_keys",7}}));
    std::map<int,RowId> identities;for(int i=0;i<1800;++i)identities[i]=db.insert_row(tx,"t",json::array({i*2,i*17+5,std::string(40,'p')}));db.commit(tx);
    tx=db.begin();auto before=db.transaction_stats(tx)["heap_page_reads"].get<std::uint64_t>();auto direct=scan(db,tx,"t",point("ix_k_direct",5));auto direct_reads=db.transaction_stats(tx)["heap_page_reads"].get<std::uint64_t>()-before;before=db.transaction_stats(tx)["heap_page_reads"].get<std::uint64_t>();auto primary=scan(db,tx,"t",point("ix_k_primary",5));auto primary_reads=db.transaction_stats(tx)["heap_page_reads"].get<std::uint64_t>()-before;
    require(direct.size()==1&&primary.size()==1&&direct[0].values==primary[0].values,"direct/primary result difference");require(direct_reads==1,"direct reference must read the known data leaf exactly once");require(primary_reads>direct_reads,"primary-key indirection must actually traverse organization tree");
    auto prior=direct_pages(db,tx);for(int i=1800;i<2000;++i)identities[i]=db.insert_row(tx,"t",json::array({(i-1800)*2+1,i*17+5,std::string(40,'p')}));auto after=direct_pages(db,tx);std::size_t moved=0;for(const auto& e:prior)if(after.at(e.first)!=e.second)++moved;require(moved>0,"interleaved inserts must move existing records and update direct refs");verify(db,tx,"post-split direct/primary references");db.commit(tx);
    tx=db.begin();std::vector<int> erase;for(int i=0;i<2000;++i)if(i%100)erase.push_back(i);std::mt19937 random(110303);std::shuffle(erase.begin(),erase.end(),random);for(auto i:erase)db.erase_row(tx,"t",identities.at(i));verify(db,tx,"post-merge direct/primary references");auto survivor=scan(db,tx,"t",point("ix_k_direct",5));require(survivor.size()==1&&survivor[0].values[0]==0,"direct reference after many merges");db.commit(tx);
    RowId a{8,0,1,100},b{8,0,1,101};require(!(a==b)&&((a<b)!=(b<a)),"same physical page/slot must not equate different LIDs");
    std::cout<<"PASS clustered direct read_at="<<direct_reads<<" vs primary root lookup="<<primary_reads<<"; unchanged existing moved refs="<<moved<<"; split/merge/rebalance verification\n";
}

void stable_reorganization(const fs::path& directory){Storage db(directory.u8string(),4);auto tx=db.begin();db.write_table(tx,"t",definition("HEAP"));for(int i=0;i<300;++i)db.insert_row(tx,"t",json::array({i,300-i,std::string(60,'r')}));db.commit(tx);tx=db.begin();auto expected=values(scan(db,tx,"t"));db.commit(tx);
    for(auto method:{"BPLUS","ORDERED","HASH_EXTENDIBLE","HEAP"}){tx=db.begin();auto interchange=db.read_table(tx,"t");require(interchange["row_ids"].size()==300,"interchange must preserve row_ids");json options=json::object();if(std::string(method)!="HEAP")options["max_keys"]=7;if(std::string(method)=="ORDERED")options["multilevel"]=true;interchange["organization"]={{"method",method},{"columns",std::string(method)=="HEAP"?json::array():json::array({"k","id"})},{"options",options}};db.write_table(tx,"t",interchange);require(values(scan(db,tx,"t"))==expected,"reorganization changed stable IDs or rows");verify(db,tx,std::string("reorganization ")+method);db.commit(tx);}
    tx=db.begin();auto id=db.insert_row(tx,"t",json::array({999,999,"new"}));require(id.logical_id>expected.rbegin()->first,"reorganization reused a retired logical id");db.rollback(tx);std::cout<<"PASS stable LIDs through HEAP/BPLUS/ORDERED/HASH_EXTENDIBLE/HEAP rewrites and fresh allocation\n";
}

void sparse_unique(const fs::path& directory){Storage db(directory.u8string(),4);auto tx=db.begin();auto table=definition("ORDERED",json::array({"k","id"}),{{"max_keys",7},{"multilevel",false}});table["indexes"]=json::array({{{"name","sp"},{"column","k"},{"columns",json::array({"k"})},{"method","ORDERED_SPARSE"},{"unique",true},{"options",{{"multilevel",false}}}}});db.write_table(tx,"t",table);db.insert_row(tx,"t",json::array({1,nullptr,"null-one"}));db.insert_row(tx,"t",json::array({2,nullptr,"null-two"}));db.insert_row(tx,"t",json::array({3,1,"first"}));db.insert_row(tx,"t",json::array({4,2,"second"}));db.commit(tx);
    tx=db.begin();auto one=scan(db,tx,"t",point("sp",1));require(one.size()==1&&one[0].values[0]==3,"sparse prefix equality over wider organization key");edudb::ScanSpec nulls;nulls.index="sp";nulls.lower_key=json::array({nullptr});nulls.upper_key=nulls.lower_key;require(scan(db,tx,"t",nulls).size()==2,"sparse NULL prefix scan");db.commit(tx);
    tx=db.begin();bool rejected=false;try{db.insert_row(tx,"t",json::array({5,1,"duplicate"}));}catch(const edudb::DbError& e){rejected=e.code=="UNIQUE_VIOLATION";}require(rejected,"nullable UNIQUE sparse alias must reject non-NULL duplicate insert");db.rollback(tx);
    tx=db.begin();auto rows=scan(db,tx,"t");auto row=std::find_if(rows.begin(),rows.end(),[](const StoredRow& r){return r.values[0]==4;});require(row!=rows.end(),"sparse update fixture");auto changed=row->values;changed[1]=1;rejected=false;try{db.update_row(tx,"t",row->id,changed);}catch(const edudb::DbError& e){rejected=e.code=="UNIQUE_VIOLATION";}require(rejected,"UNIQUE sparse alias rejects duplicate update");db.rollback(tx);
    tx=db.begin();auto definitions=db.read_table(tx,"t",false)["indexes"];definitions[0]["options"]["multilevel"]=true;rejected=false;try{db.set_indexes(tx,"t",definitions);}catch(const edudb::DbError& e){rejected=e.code=="INVALID_INDEX_ORGANIZATION";}require(rejected,"sparse physical flat/multilevel mismatch must not silently alias");db.rollback(tx);tx=db.begin();verify(db,tx,"sparse nullable uniqueness");db.commit(tx);
    tx=db.begin();auto duplicates=table;duplicates["indexes"][0]["name"]="sp_existing";duplicates["indexes"][0]["unique"]=false;duplicates["rows"]=json::array({json::array({1,nullptr,"one"}),json::array({2,nullptr,"two"}),json::array({3,9,"three"}),json::array({4,9,"four"})});db.write_table(tx,"existing",duplicates);db.commit(tx);tx=db.begin();auto unique=db.read_table(tx,"existing",false)["indexes"];unique[0]["unique"]=true;rejected=false;try{db.set_indexes(tx,"existing",unique);}catch(const edudb::DbError& e){rejected=e.code=="UNIQUE_VIOLATION";}require(rejected,"building a UNIQUE sparse alias must validate existing non-NULL duplicates");db.rollback(tx);tx=db.begin();require(scan(db,tx,"existing").size()==4,"failed sparse UNIQUE build changed existing rows");verify(db,tx,"rejected unique build preserves old sparse index");db.commit(tx);
    std::cout<<"PASS sparse actual physical mode, short prefix bounds, repeated NULL and UNIQUE insert/update failures\n";
}

void update_and_lob(const fs::path& directory){Storage db(directory.u8string(),4);auto tx=db.begin();auto table=definition("HEAP");table["indexes"]=json::array({index("ix_id","id",true)});db.write_table(tx,"t",table);RowId first;for(int i=0;i<500;++i){auto id=db.insert_row(tx,"t",json::array({i,i,std::string(80,'a')}));if(!i)first=id;}db.commit(tx);auto allocated=db.diagnostics()["next_page_id"];
    constexpr std::uint64_t expected_update_wal=104+2*(48+8+8192);
    tx=db.begin();auto before=db.read_table(tx,"t",false,false);auto original=first;
    for(int i=0;i<500;++i)first=db.update_row(tx,"t",first,json::array({0,0,std::string(80,static_cast<char>('b'+i%2))}));
    auto pending=db.transaction_stats(tx);require(first==original,"same-size repeated updates must retain physical RID");
    require(pending["private_pages"]==1&&pending["private_heap_pages"]==1&&pending["private_metadata_pages"]==0&&pending["private_index_pages"]==0&&pending["private_retired_pages"]==0&&pending["pending_descriptor_count"]==1,"500 updates retain one heap image and one lazily pending metadata descriptor");
    auto after=db.read_table(tx,"t",false,false);require(after["data_version"].get<std::uint64_t>()==before["data_version"].get<std::uint64_t>()+500,"500 actual updates advance data_version by exactly 500");
    require(after["schema_version"]==before["schema_version"]&&after["physical_version"]==before["physical_version"],"same-size updates preserve schema/physical versions");db.commit(tx);
    auto committed=db.transaction_stats(tx);require(committed["private_pages"]==2&&committed["private_heap_pages"]==1&&committed["private_metadata_pages"]==1&&committed["private_index_pages"]==0&&committed["private_retired_pages"]==0&&committed["pending_descriptor_count"]==0,"500 updates commit exactly one final heap and one metadata image");
    require(committed["wal_bytes"]==expected_update_wal,"one heap and one metadata image produce exactly 16600 WAL bytes");require(db.diagnostics()["next_page_id"]==allocated,"same-size non-indexed update must not allocate");
    tx=db.begin();auto blobs=definition("HEAP");blobs["columns"][2]=column("payload","BLOB");blobs["indexes"]=json::array({index("ix_blob_id","id",true)});db.write_table(tx,"objects",blobs);std::string hex;const char* digits="0123456789abcdef";for(unsigned i=0;i<16384;++i){auto value=i%251;hex+=digits[value>>4];hex+=digits[value&15];}auto blob=json{{"$blob",hex}};auto object=db.insert_row(tx,"objects",json::array({1,10,blob}));db.commit(tx);allocated=db.diagnostics()["next_page_id"];
    tx=db.begin();edudb::ScanSpec lazy;lazy.lazy_blobs=true;auto previous=scan(db,tx,"objects",lazy);require(previous.size()==1&&previous[0].values[2].contains("$lob"),"original lazy BLOB reference");auto reference=previous[0].values[2];auto object_before=db.read_table(tx,"objects",false,false);auto retired=db.diagnostics()["retired_pages"];auto original_object=object;
    object=db.update_row(tx,"objects",object,json::array({1,11,blob}));pending=db.transaction_stats(tx);
    require(object==original_object&&pending["private_pages"]==1&&pending["private_heap_pages"]==1&&pending["private_metadata_pages"]==0&&pending["private_index_pages"]==0&&pending["private_retired_pages"]==0&&pending["pending_descriptor_count"]==1,"unrelated update keeps one heap image with no dirty BLOB/index pages");
    auto updated=scan(db,tx,"objects",lazy);require(updated.size()==1&&updated[0].values[2]==reference,"unchanged BLOB retains the exact persisted tree descriptor and owner");require(db.blob_value(tx,updated[0].values[2]).at("$blob")==hex,"unchanged BLOB retains all original 16KiB, not just a range");
    auto object_after=db.read_table(tx,"objects",false,false);require(object_after["data_version"].get<std::uint64_t>()==object_before["data_version"].get<std::uint64_t>()+1&&object_after["schema_version"]==object_before["schema_version"]&&object_after["physical_version"]==object_before["physical_version"],"unrelated BLOB row update changes only data_version");db.commit(tx);
    committed=db.transaction_stats(tx);require(committed["private_pages"]==2&&committed["private_heap_pages"]==1&&committed["private_metadata_pages"]==1&&committed["private_index_pages"]==0&&committed["private_retired_pages"]==0&&committed["pending_descriptor_count"]==0,"unchanged BLOB commit contains exactly heap plus metadata, zero chunk/index images");require(committed["wal_bytes"]==expected_update_wal&&db.diagnostics()["next_page_id"]==allocated&&db.diagnostics()["retired_pages"]==retired,"unchanged BLOB row update uses 16600 WAL bytes without allocating or retiring chunk pages");
    tx=db.begin();auto rows=scan(db,tx,"objects",lazy);require(rows.size()==1&&rows[0].values[2]==reference,"committed BLOB reference remains byte-for-byte unchanged");auto part=db.blob_value(tx,rows[0].values[2],3071,5);require(part.at("$blob")==hex.substr(3070*2,10),"BLOB range crossing physical chunks");verify(db,tx,"BLOB reuse graph");db.commit(tx);
    tx=db.begin();db.erase_row(tx,"objects",object);db.commit(tx);require(db.diagnostics()["next_page_id"]==allocated,"BLOB retire must not allocate orphan empty index root");std::cout<<"PASS 500-update: 1 final heap + 1 metadata image /16600-byte WAL; unchanged 16KiB BLOB descriptor/full bytes, zero chunk writes, cross-chunk range, orphan-free destruction\n";
}
}
int main(){auto stamp=std::chrono::steady_clock::now().time_since_epoch().count();auto path=fs::temp_directory_path()/fs::u8path("EduDB 第11章存储 "+std::to_string(stamp));try{
    organizations(path/"organizations");clustered_references(path/"clustered");stable_reorganization(path/"reorganization");sparse_unique(path/"sparse");update_and_lob(path/"update-lob");
    require(fs::equivalent(path.parent_path(),fs::temp_directory_path())&&path.filename().u8string().rfind("EduDB 第11章存储 ",0)==0,"test cleanup parent boundary");fs::remove_all(path);
    std::cout<<"PASS ch11 storage: independent full-value/LID models, six actual organizations, direct/primary lookup, relocation and nullable sparse uniqueness\n";return 0;
}catch(const std::exception& e){std::cerr<<"FAIL ch11 storage: "<<e.what()<<"\nEvidence directory: "<<path.u8string()<<"\n";return 1;}}
