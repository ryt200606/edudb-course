#include "edudb.hpp"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
#include <random>
#include <map>

using edudb::json;
namespace fs=std::filesystem;
static void check(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}
static std::uint32_t fixture_crc(const std::vector<unsigned char>& bytes){
    std::uint32_t c=0xffffffffu;for(auto b:bytes){c^=b;for(int bit=0;bit<8;++bit)c=(c>>1)^((c&1)?0xedb88320u:0);}return c^0xffffffffu;
}
static void fixture_u16(std::vector<unsigned char>& bytes,std::size_t offset,std::uint16_t value){bytes[offset]=static_cast<unsigned char>(value);bytes[offset+1]=static_cast<unsigned char>(value>>8);}
static void fixture_seal(std::vector<unsigned char>& bytes){for(int i=0;i<4;++i)bytes[60+i]=0;auto c=fixture_crc(bytes);for(int i=0;i<4;++i)bytes[60+i]=static_cast<unsigned char>(c>>(i*8));}
static void environment(const char* key,const char* value){
#ifdef _WIN32
    _putenv_s(key,value);
#else
    if(*value)setenv(key,value,1);else unsetenv(key);
#endif
}
static json table(int count,bool duplicates=false){
    json result={{"columns",json::array({{{"name","id"},{"type","INTEGER"}},{{"name","payload"},{"type","TEXT"}}})},
                 {"indexes",json::array({{{"name","ix_id"},{"column","id"},{"unique",!duplicates},{"options",{{"max_keys",31}}}}})},{"rows",json::array()}};
    for(int i=0;i<count;++i)result["rows"].push_back(json::array({duplicates?i%7:i,std::string(70,'x')+std::to_string(i)}));
    return result;
}
int main(int argc,char** argv){
    if(argc==4&&std::string(argv[1])=="--crash"){
        try{
            edudb::Storage db(argv[2],4);auto tx=db.begin();db.write_table(tx,"t",table(30));db.commit(tx);
#ifdef _WIN32
            _putenv_s("EDUDB_CRASH_AT",argv[3]);
#else
            setenv("EDUDB_CRASH_AT",argv[3],1);
#endif
            if(std::string(argv[3]).rfind("checkpoint_",0)==0)db.checkpoint();
            else{tx=db.begin();db.write_table(tx,"t",table(45));db.commit(tx);}
            std::cerr<<"Fault point did not exit\n";return 3;
        }catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 4;}
    }
    if(argc==3&&std::string(argv[1])=="--inspect"){
        try{edudb::Storage db(argv[2],4);auto tx=db.begin();auto count=db.read_table(tx,"t")["rows"].size();db.commit(tx);std::cout<<json{{"rows",count},{"diagnostics",db.diagnostics()}}.dump()<<"\n";return 0;}
        catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 4;}
    }
    const auto stamp=std::chrono::steady_clock::now().time_since_epoch().count();
    auto path=fs::temp_directory_path()/fs::u8path("EduDB storage 测试 "+std::to_string(stamp));
    try{
        {
            edudb::Storage db(path.u8string(),4);
            bool locked=false;try{edudb::Storage second(path.u8string());}catch(const edudb::DbError& e){locked=e.code=="DATABASE_LOCKED";}check(locked,"process file lock");
            auto tx=db.begin();db.write_table(tx,"a",table(1200));db.write_table(tx,"b",table(15));db.write_table(tx,"duplicates",table(1200,true));
            check(db.read_table(tx,"a")["rows"].size()==1200,"read own writes");
            check(fs::file_size(path/"data.pages")==16384,"no-steal private pages stay off disk");
            check(fs::file_size(path/"wal-1.log")==64,"private work does not append uncommitted WAL");db.commit(tx);
            auto stats=db.diagnostics();check(stats["evictions"].get<int>()>0,"LRU eviction");check(stats["resident_frames"].get<int>()<=4,"bounded buffer");
            auto read=db.begin();auto before=db.diagnostics()["heap_page_reads"].get<std::uint64_t>();auto meta=db.read_table(read,"a",false);
            check(!meta.contains("rows"),"metadata only omits rows");check(db.diagnostics()["heap_page_reads"].get<std::uint64_t>()==before,"metadata read does not read heap");
            check(db.index_scan(read,"a","ix_id","=",314).at(0).at(0)==314,"persistent B+ equality");
            check(db.index_scan(read,"a","ix_id",">=",1190).size()==10,"persistent B+ range");
            check(db.index_scan(read,"a","ix_id","<",10).size()==10,"persistent B+ left range");
            auto duplicates=db.index_scan(read,"duplicates","ix_id","=",4);check(duplicates.size()==171,"duplicate keys spanning leaves");
            for(const auto& row:duplicates)check(row.at(0)==4,"duplicate equality exact values");
            db.commit(read);
            auto abort=db.begin();auto changed=db.read_table(abort,"a");changed["rows"][0][1]="rollback marker";db.write_table(abort,"a",changed);db.rollback(abort);
            auto after=db.begin();check(db.read_table(after,"a")["rows"][0][1]!="rollback marker","rollback isolation");db.commit(after);
            // Caller holds distinct table locks. Both writers finish private work before either commits.
            auto left=db.begin(),right=db.begin();
            auto writer=[&](const auto& t,const char* name,int value){auto v=db.read_table(t,name);v["rows"][0][1]=std::to_string(value);db.write_table(t,name,v);};
            auto f1=std::async(std::launch::async,[&]{writer(left,"a",111);});auto f2=std::async(std::launch::async,[&]{writer(right,"b",222);});f1.get();f2.get();
            check(db.diagnostics()["active_transactions"]==2,"independent writers overlap");db.commit(left);db.commit(right);
            auto idle=db.begin();bool busy=false;try{db.checkpoint(10);}catch(const edudb::DbError& e){busy=e.code=="CHECKPOINT_BUSY";}check(busy,"checkpoint busy timeout");db.rollback(idle);
            check(db.diagnostics()["active_transactions"]==0,"registry empty after commits and rollback");
        }
        {
            edudb::Storage db(path.u8string(),3);check(db.diagnostics()["recovered_pages"].get<int>()>0,"redo recovery really writes pages");
            auto tx=db.begin();check(db.read_table(tx,"a")["rows"][0][1]=="111","parallel a survives restart");check(db.read_table(tx,"b")["rows"][0][1]=="222","parallel b survives restart");
            check(db.catalog(tx).size()==3,"DML did not lose catalog entries");db.commit(tx);auto old=db.diagnostics()["wal_generation"].get<int>();db.checkpoint();check(db.diagnostics()["wal_generation"].get<int>()>old,"checkpoint WAL rotation");check(db.diagnostics()["wal_bytes"]==64,"new WAL header only");
        }
        {
            edudb::Storage db(path.u8string(),2);auto tx=db.begin();check(db.read_table(tx,"a")["rows"].size()==1200,"checkpoint persistence");db.drop_table(tx,"b");db.rollback(tx);tx=db.begin();check(db.catalog(tx).contains("b"),"DROP rollback");db.drop_table(tx,"b");db.commit(tx);
        }
        {
            const auto concurrent=path/"concurrent-incremental";
            {edudb::Storage db(concurrent.u8string(),2);auto tx=db.begin();db.write_table(tx,"a",table(0));db.write_table(tx,"b",table(0));db.commit(tx);
                auto a=db.begin(),b=db.begin();edudb::RowId minimum;
                auto left=std::async(std::launch::async,[&]{for(int i=0;i<500;++i){std::int64_t key=i==0?std::numeric_limits<std::int64_t>::min():(i==499?std::numeric_limits<std::int64_t>::max():std::int64_t(i-250));auto rid=db.insert_row(a,"a",json::array({key,"a"}));if(i==0)minimum=rid;}});
                auto right=std::async(std::launch::async,[&]{for(int i=0;i<500;++i)db.insert_row(b,"b",json::array({i,"b"}));});left.get();right.get();
                std::promise<void> ready;auto gate=ready.get_future().share();
                auto ca=std::async(std::launch::async,[&]{gate.wait();db.commit(a);});auto cb=std::async(std::launch::async,[&]{gate.wait();db.commit(b);});ready.set_value();ca.get();cb.get();
                check(db.diagnostics()["active_transactions"]==0,"simultaneous successful incremental commits release both registries");
                tx=db.begin();check(db.verify(tx)["ok"].get<bool>(),"concurrent page allocation and WAL commits preserve both table graphs");
                const auto lo=std::numeric_limits<std::int64_t>::min(),hi=std::numeric_limits<std::int64_t>::max();
                check(db.index_scan(tx,"a","ix_id","=",lo).size()==1&&db.index_scan(tx,"a","ix_id","=",hi).size()==1,"int64 extreme equality keys survive persistent splits");
                check(db.index_scan(tx,"a","ix_id","<",lo).empty()&&db.index_scan(tx,"a","ix_id",">",hi).empty(),"exclusive int64 boundaries do not wrap");
                edudb::ScanSpec spec;spec.index="ix_id";spec.lower=lo;spec.upper=hi;spec.lower_inclusive=false;spec.upper_inclusive=false;auto cursor=db.scan(tx,"a",spec);edudb::StoredRow row;std::size_t count=0;while(cursor->next(row))++count;check(count==498,"exclusive two-sided range traverses correct leaf boundaries");
                bool duplicate=false;try{db.update_row(tx,"a",minimum,json::array({hi,"duplicate"}));}catch(const edudb::DbError& e){duplicate=e.code=="UNIQUE_VIOLATION";}check(duplicate,"index-key update rejects an extreme duplicate before mutation");
                db.update_row(tx,"a",minimum,json::array({-9999,"changed"}));check(db.index_scan(tx,"a","ix_id","=",lo).empty()&&db.index_scan(tx,"a","ix_id","=",-9999).size()==1,"indexed key update removes old entry and inserts new entry");db.commit(tx);
            }
            edudb::Storage db(concurrent.u8string(),2);auto tx=db.begin();check(db.read_table(tx,"a")["rows"].size()==500&&db.read_table(tx,"b")["rows"].size()==500,"both simultaneous successful writers survive recovery");check(db.verify(tx)["ok"].get<bool>(),"recovered concurrent incremental graph cross-check");db.commit(tx);
        }
        // A short crashed append must not discard the preceding valid commit.
        fs::path wal;for(const auto& entry:fs::directory_iterator(path))if(entry.path().filename().u8string().find("wal-")==0)wal=entry.path();
        {std::ofstream out(wal,std::ios::binary|std::ios::app);out.write("torn123",7);}
        {
            edudb::Storage db(path.u8string());check(db.diagnostics()["wal_tail_truncated"].get<bool>(),"truncated WAL tail detected");auto tx=db.begin();check(!db.catalog(tx).contains("b"),"valid DROP commit preserved before bad tail");db.commit(tx);
        }
        {
            auto small=path/"small";edudb::Storage db(small.u8string(),2,8192);auto tx=db.begin();bool limit=false;try{db.write_table(tx,"too_large",table(200));}catch(const edudb::DbError& e){limit=e.code=="TRANSACTION_TOO_LARGE";}check(limit,"private page upper bound");db.rollback(tx);check(db.diagnostics()["active_transactions"]==0,"limit rollback frees registry");
            tx=db.begin();check(db.catalog(tx).empty(),"failed rewrite did not publish catalog");db.commit(tx);
        }
        {
            // Format 3 also maintains the stable-LID row-map. Five private
            // frames let the next insert fail during a real index split.
            auto limited=path/"limit-mid-index-split";edudb::Storage db(limited.u8string(),2,4*8192);
            auto tx=db.begin();db.write_table(tx,"t",table(0));db.commit(tx);tx=db.begin();int inserted=0;bool limit=false;
            try{for(int i=0;i<40;++i){db.insert_row(tx,"t",json::array({i,"x"}));++inserted;}}
            catch(const edudb::DbError& e){limit=e.code=="TRANSACTION_TOO_LARGE";}
            check(limit&&inserted==31,"private limit interrupts a real leaf split after heap mutation");
            bool blocked=false;try{db.read_table(tx,"t");}catch(const edudb::DbError& e){blocked=e.code=="TRANSACTION_ABORTED";}
            check(blocked,"resource failure poisons the partial transaction");blocked=false;
            try{db.commit(tx);}catch(const edudb::DbError& e){blocked=e.code=="TRANSACTION_ABORTED";}
            check(blocked,"partial index mutation cannot receive a commit acknowledgment");
            check(db.diagnostics()["active_transactions"]==0,"rejected aborted commit frees private write set and registry");
            tx=db.begin();check(db.read_table(tx,"t")["rows"].empty(),"all earlier writes in failed transaction remain unpublished");
            check(db.verify(tx)["ok"].get<bool>(),"heap and tree remain consistent after rejected partial commit");
            db.insert_row(tx,"t",json::array({99,"healthy"}));db.commit(tx);
            check(!db.diagnostics()["recovery_required"].get<bool>(),"private limit does not fail-stop the whole storage");
        }
        {
            auto corrupt=path/"corrupt";{edudb::Storage db(corrupt.u8string());auto tx=db.begin();db.write_table(tx,"t",table(10));db.commit(tx);}
            auto wal_path=corrupt/"wal-1.log";{std::fstream f(wal_path,std::ios::binary|std::ios::in|std::ios::out);f.seekg(160);char c=0;f.read(&c,1);c^=0x55;f.seekp(160);f.write(&c,1);}
            bool rejected=false;try{edudb::Storage db(corrupt.u8string());}catch(const edudb::DbError& e){rejected=e.code=="CORRUPT_WAL";}check(rejected,"interior WAL corruption rejected");
        }
        {
            auto corrupt=path/"corrupt-final-commit";{edudb::Storage db(corrupt.u8string());auto tx=db.begin();db.write_table(tx,"t",table(10));db.commit(tx);}
            auto log=corrupt/"wal-1.log";{std::fstream f(log,std::ios::binary|std::ios::in|std::ios::out);f.seekg(-1,std::ios::end);char c=0;f.read(&c,1);c^=0x20;f.seekp(-1,std::ios::end);f.write(&c,1);}
            bool rejected=false;try{edudb::Storage db(corrupt.u8string());}catch(const edudb::DbError& e){rejected=e.code=="CORRUPT_WAL";}check(rejected,"complete final COMMIT bitflip must not silently roll back");
        }
        {
            auto incomplete=path/"incomplete-transaction";std::vector<char> precommit_data;
            {edudb::Storage db(incomplete.u8string());
                {std::ifstream in(incomplete/"data.pages",std::ios::binary);precommit_data.assign(std::istreambuf_iterator<char>(in),{});}
                auto tx=db.begin();db.write_table(tx,"t",table(10));db.commit(tx);
            }
            // Model an append that never reached COMMIT, so data must remain
            // at its precommit no-steal image. The new publication workspace
            // may flush ACKed pages even when the pool has spare capacity;
            // removing ACKed WAL while keeping those pages is media corruption,
            // tested separately below, not an unfinished-append scenario.
            {std::ofstream out(incomplete/"data.pages",std::ios::binary|std::ios::trunc);out.write(precommit_data.data(),static_cast<std::streamsize>(precommit_data.size()));check(bool(out),"restore exact precommit data fixture");}
            auto log=incomplete/"wal-1.log";fs::resize_file(log,fs::file_size(log)-56); // COMMIT retains a 48-byte checked header plus 8-byte payload.
            edudb::Storage db(incomplete.u8string());auto tx=db.begin();check(db.catalog(tx).empty(),"valid page records without COMMIT are never replayed");db.commit(tx);
            check(db.diagnostics()["wal_tail_truncated"].get<bool>(),"incomplete transaction tail removed");check(db.diagnostics()["wal_bytes"]==64,"incomplete transaction truncates at preceding commit boundary");
        }
        {
            auto missing=path/"missing-durable-commit";
            {edudb::Storage db(missing.u8string(),2);auto tx=db.begin();db.write_table(tx,"t",table(50));db.commit(tx);}
            // A valid reopen completes redo; all acknowledged pages now have durable data images.
            {edudb::Storage db(missing.u8string(),2);auto tx=db.begin();check(db.read_table(tx,"t")["rows"].size()==50,"durable-WAL truncation fixture");db.commit(tx);}
            const auto log=missing/"wal-1.log";std::string complete;{std::ifstream in(log,std::ios::binary);complete.assign(std::istreambuf_iterator<char>(in),{});}
            for(auto cut:{std::size_t(4),std::size_t(56),complete.size()-64}){
                {std::ofstream out(log,std::ios::binary|std::ios::trunc);out.write(complete.data(),static_cast<std::streamsize>(complete.size()-cut));}
                const auto damaged_size=fs::file_size(log);bool rejected=false;
                try{edudb::Storage db(missing.u8string(),2);auto tx=db.begin();db.read_table(tx,"t");db.commit(tx);}
                catch(const edudb::DbError& e){rejected=e.code=="CORRUPT_WAL";}
                check(rejected,"data newer than last complete commit rejects truncated acknowledged WAL");
                check(fs::file_size(log)==damaged_size,"rejected future-page recovery preserves damaged WAL evidence");
            }
            // Constructor/read failures must release their file handles and process lock.
            {std::ofstream out(log,std::ios::binary|std::ios::trunc);out.write(complete.data(),static_cast<std::streamsize>(complete.size()));}
            edudb::Storage db(missing.u8string(),2);auto tx=db.begin();check(db.verify(tx)["ok"].get<bool>(),"restored original WAL reopens after rejected recovery");db.commit(tx);
        }
        {
            auto generations=path/"checkpoint-generations";
            {edudb::Storage db(generations.u8string(),2);auto tx=db.begin();db.write_table(tx,"t",table(120));db.commit(tx);
                tx=db.begin();std::vector<edudb::RowId> ids;auto cursor=db.scan(tx,"t");edudb::StoredRow row;while(cursor->next(row))ids.push_back(row.id);
                for(auto id:ids){db.erase_row(tx,"t",id);}
                db.commit(tx);check(db.diagnostics()["retired_pages"].get<int>()>0,"fixture retires committed index pages");
                for(int i=0;i<3;++i){db.checkpoint();check(db.diagnostics()["wal_bytes"]==64,"successive checkpoints create valid empty WAL generations");}}
            fs::path current;for(const auto& entry:fs::directory_iterator(generations))if(entry.path().extension()==".log")current=entry.path();
            {std::ofstream out(current,std::ios::binary|std::ios::app);out.write("short",5);}
            edudb::Storage db(generations.u8string(),2);auto tx=db.begin();check(db.read_table(tx,"t")["rows"].empty(),"checkpoint baseline includes committed delete and retired pages");
            check(db.verify(tx)["ok"].get<bool>(),"normal retired pages do not trigger future-LSN rejection");db.commit(tx);
            check(db.diagnostics()["wal_tail_truncated"].get<bool>()&&db.diagnostics()["wal_bytes"]==64,"short tail after new generation safely truncated to checkpoint baseline");
        }
        {
            auto older=path/"valid-older-page-redo";
            {edudb::Storage db(older.u8string(),2);auto tx=db.begin();db.write_table(tx,"t",table(50));db.commit(tx);db.checkpoint();
                tx=db.begin();edudb::StoredRow row;db.scan(tx,"t")->next(row);auto changed=row.values;changed[1]="newer WAL image";db.update_row(tx,"t",row.id,changed);db.commit(tx);}
            edudb::Storage db(older.u8string(),2);auto tx=db.begin();check(db.read_table(tx,"t")["rows"][0][1]=="newer WAL image","valid newer redo replaces an older valid page");
            check(db.verify(tx)["ok"].get<bool>(),"redo after checkpoint preserves heap/index correspondence");db.commit(tx);check(db.diagnostics()["recovered_pages"].get<int>()>0,"older valid page required actual redo write");
        }
        {
            auto torn=path/"torn-data-page";std::uint64_t descriptor=0;
            {edudb::Storage db(torn.u8string(),2);auto tx=db.begin();db.write_table(tx,"t",table(50));db.commit(tx);tx=db.begin();descriptor=db.catalog(tx).at("t").get<std::uint64_t>();db.commit(tx);}
            // A forced partial overwrite models a torn page, while retaining valid full-image WAL.
            {std::fstream f(torn/"data.pages",std::ios::binary|std::ios::in|std::ios::out);f.seekp(static_cast<std::streamoff>(descriptor*8192+100));std::string damage(512,'\0');f.write(damage.data(),damage.size());}
            edudb::Storage db(torn.u8string(),2);auto tx=db.begin();check(db.read_table(tx,"t")["rows"].size()==50,"full-image redo repairs torn data page");db.commit(tx);check(db.diagnostics()["recovered_pages"].get<int>()>0,"torn page repair was an actual redo write");
        }
        {
            auto malformed=path/"malformed-page-layout";std::uint64_t heap=0;
            {edudb::Storage db(malformed.u8string(),2);auto tx=db.begin();db.write_table(tx,"t",table(10));db.commit(tx);db.checkpoint();tx=db.begin();edudb::StoredRow row;db.scan(tx,"t")->next(row);heap=row.id.page_id;db.commit(tx);}
            const auto file=malformed/"data.pages";std::vector<unsigned char> original(8192);
            {std::ifstream in(file,std::ios::binary);in.seekg(static_cast<std::streamoff>(heap*8192));in.read(reinterpret_cast<char*>(original.data()),8192);}
            for(auto field:{std::size_t(6),std::size_t(32),std::size_t(34),std::size_t(36),std::size_t(64),std::size_t(68)}){
                auto bytes=original;
                if(field==68){for(int i=0;i<4;++i)bytes[field+i]=0;}
                else fixture_u16(bytes,field,field==36?0:65535);
                fixture_seal(bytes);
                {std::fstream out(file,std::ios::binary|std::ios::in|std::ios::out);out.seekp(static_cast<std::streamoff>(heap*8192));out.write(reinterpret_cast<const char*>(bytes.data()),8192);}
                bool rejected=false;try{edudb::Storage db(malformed.u8string(),2);auto tx=db.begin();db.read_table(tx,"t");}
                catch(const edudb::DbError& e){rejected=e.code=="CORRUPT_PAGE";}
                check(rejected,"CRC-valid malformed page/slot header rejected before unsafe field access");
            }
            {std::fstream out(file,std::ios::binary|std::ios::in|std::ios::out);out.seekp(static_cast<std::streamoff>(heap*8192));out.write(reinterpret_cast<const char*>(original.data()),8192);}
            edudb::Storage db(malformed.u8string(),2);auto tx=db.begin();check(db.verify(tx)["ok"].get<bool>(),"restored valid layout reopens and releases failed-reader handles");db.commit(tx);
        }
        {
            auto control=path/"corrupt-control";{edudb::Storage db(control.u8string());}
            json manifest;{std::ifstream in(control/"control.json");in>>manifest;}manifest["next_page_id"]=999;
            {std::ofstream out(control/"control.json",std::ios::trunc);out<<manifest.dump();}
            bool rejected=false;try{edudb::Storage db(control.u8string());}catch(const edudb::DbError& e){rejected=e.code=="CORRUPT_CONTROL";}check(rejected,"manifest checksum rejects corruption");
        }
        {
            auto incremental=path/"incremental";
            edudb::Storage db(incremental.u8string(),8);auto tx=db.begin();db.write_table(tx,"t",table(0));std::vector<edudb::RowId> ids;
            for(int i=0;i<1300;++i)ids.push_back(db.insert_row(tx,"t",json::array({i,std::string(70,'x')})));
            check(db.verify(tx)["ok"].get<bool>(),"verify persistent splits and separators");db.commit(tx);
            check(db.diagnostics()["splits"].get<int>()>0,"real persistent tree split");
            auto size=db.diagnostics()["next_page_id"].get<std::uint64_t>();auto wal=db.diagnostics()["wal_bytes"].get<std::uint64_t>();
            tx=db.begin();for(int i=0;i<500;++i)check(db.update_row(tx,"t",ids[0],json::array({0,std::string(70,'x')}))==ids[0],"unchanged update retains RID");
            auto unchanged=db.transaction_stats(tx);check(unchanged["dirty_pages"]==0&&unchanged["private_heap_pages"]==0&&unchanged["private_metadata_pages"]==0&&unchanged["private_index_pages"]==0&&unchanged["pending_descriptor_count"]==0,"unchanged rows create zero private images and no pending descriptor");db.commit(tx);check(db.diagnostics()["wal_bytes"]==wal,"zero mutations create zero WAL");
            tx=db.begin();auto before_update=db.read_table(tx,"t",false,false);auto first_rid=ids[0];
            for(int i=0;i<500;++i)ids[0]=db.update_row(tx,"t",ids[0],json::array({0,std::string(70,static_cast<char>('a'+i%20))}));
            auto pending=db.transaction_stats(tx);check(ids[0]==first_rid,"same-size repeated updates retain the physical RID");
            check(pending["private_pages"]==1&&pending["private_heap_pages"]==1&&pending["private_metadata_pages"]==0&&pending["private_index_pages"]==0&&pending["private_retired_pages"]==0&&pending["pending_descriptor_count"]==1,"500 writes retain one final heap image and one lazily pending descriptor");
            auto after_update=db.read_table(tx,"t",false,false);check(after_update["data_version"].get<std::uint64_t>()==before_update["data_version"].get<std::uint64_t>()+500,"each actual update advances the durable data version");
            check(after_update["schema_version"]==before_update["schema_version"]&&after_update["physical_version"]==before_update["physical_version"],"same-size non-indexed updates preserve schema and physical versions");db.commit(tx);
            check(db.diagnostics()["next_page_id"]==size,"non-indexed fixed-size updates allocate no pages");
            auto committed=db.transaction_stats(tx);check(committed["private_pages"]==2&&committed["private_heap_pages"]==1&&committed["private_metadata_pages"]==1&&committed["private_index_pages"]==0&&committed["private_retired_pages"]==0&&committed["pending_descriptor_count"]==0,"commit contains exactly one final heap and one final metadata image");
            constexpr std::uint64_t expected_update_wal=104+2*(48+8+8192);
            check(committed["wal_bytes"]==expected_update_wal&&db.diagnostics()["wal_bytes"].get<std::uint64_t>()==wal+expected_update_wal,"two final images produce exactly 16600 WAL bytes, including transaction framing");
            tx=db.begin();std::mt19937 rng(31);std::shuffle(ids.begin(),ids.end(),rng);
            for(std::size_t i=0;i<ids.size();++i){db.erase_row(tx,"t",ids[i]);if(i%83==0)check(db.verify(tx)["ok"].get<bool>(),"verify after persistent erase/rebalance");}
            auto verified=db.verify(tx);check(verified["ok"].get<bool>(),"verify all-delete root shrink");check(db.read_table(tx,"t",false)["index_stats"][0]["height"]==1,"persistent root shrinks to leaf");db.commit(tx);
            auto stats=db.diagnostics();check(stats["merges"].get<int>()>0,"persistent merges occurred");check(stats["redistributions"].get<int>()>0,"persistent borrowing occurred");check(stats["root_shrinks"].get<int>()>0,"persistent root shrank");
            tx=db.begin();auto previous=ids.back();auto reused=db.insert_row(tx,"t",json::array({2000,"reuse"}));bool stale=false;try{db.update_row(tx,"t",previous,json::array({2001,"bad"}));}catch(const edudb::DbError& e){stale=e.code=="STALE_ROW_ID";}check(stale,"deleted RID is rejected even if slot reused");check(reused.generation>0,"RID generation persisted");db.commit(tx);db.checkpoint();
        }
        {
            auto relocation=path/"relocation";edudb::Storage db(relocation.u8string(),4);auto tx=db.begin();db.write_table(tx,"t",table(0));
            auto first=db.insert_row(tx,"t",json::array({1,std::string(3000,'a')}));auto other=db.insert_row(tx,"t",json::array({2,std::string(3000,'b')}));
            check(first.page_id==other.page_id,"fixture has two rows on same page");auto moved=db.update_row(tx,"t",first,json::array({1,std::string(7000,'c')}));check(!(moved==first),"variable-length row relocates across pages");
            check(db.index_scan(tx,"t","ix_id","=",1)[0][1].get<std::string>().size()==7000,"index follows relocated RID");check(db.verify(tx)["ok"].get<bool>(),"heap and index agree after relocation");
            auto reused=db.insert_row(tx,"t",json::array({3,std::string(2000,'d')}));check(reused.page_id==first.page_id&&reused.slot_id==first.slot_id&&reused.generation>first.generation,"reuse increments same slot generation");db.commit(tx);
            tx=db.begin();db.write_table(tx,"t",table(0));bool obsolete=false;try{db.erase_row(tx,"t",moved);}catch(const edudb::DbError& e){obsolete=e.code=="STALE_ROW_ID";}check(obsolete,"DDL replacement invalidates old heap RIDs");db.rollback(tx);
        }
        {
            auto broken=path/"corrupt-commit-length";{edudb::Storage db(broken.u8string());auto tx=db.begin();db.write_table(tx,"t",table(10));db.commit(tx);}
            auto log=broken/"wal-1.log";{std::fstream f(log,std::ios::binary|std::ios::in|std::ios::out);f.seekp(-52,std::ios::end);const char length[4]={8,32,0,0};f.write(length,4);}
            bool rejected=false;try{edudb::Storage db(broken.u8string());}catch(const edudb::DbError& e){rejected=e.code=="CORRUPT_WAL";}check(rejected,"COMMIT length bitflip cannot masquerade as truncated tail");
        }
        {
            auto random_path=path/"random-persistent";std::map<int,json> model;std::mt19937 rng(3102);int sequence=0;
            for(int batch=0;batch<6;++batch){
                edudb::Storage db(random_path.u8string(),3);auto tx=db.begin();
                if(batch==0){auto t=table(0);t["columns"].insert(t["columns"].begin()+1,json{{"name","category"},{"type","INTEGER"}});t["indexes"].push_back({{"name","ix_category"},{"column","category"},{"unique",false}});db.write_table(tx,"t",t);}
                std::map<int,edudb::StoredRow> live;auto cursor=db.scan(tx,"t");edudb::StoredRow current;while(cursor->next(current))live.emplace(current.values[0].get<int>(),current);
                check(live.size()==model.size(),"reopened random workload row count");for(const auto& pair:live)check(pair.second.values==model.at(pair.first),"reopened random workload exact values");
                for(int step=0;step<100;++step){auto action=rng()%4;
                    if(live.empty()||action<2){int id=++sequence;json row=json::array({id,step%7==0?json(nullptr):json(int(rng()%9)),std::string(rng()%350+1,static_cast<char>('a'+step%20))});auto rid=db.insert_row(tx,"t",row);live[id]={rid,row};model[id]=row;}
                    else{auto it=live.begin();std::advance(it,static_cast<long>(rng()%live.size()));int id=it->first;
                        if(action==2){db.erase_row(tx,"t",it->second.id);live.erase(it);model.erase(id);}
                        else{auto row=it->second.values;row[1]=step%5==0?json(nullptr):json(int(rng()%9));row[2]=std::string(rng()%600+1,'z');it->second.id=db.update_row(tx,"t",it->second.id,row);it->second.values=row;model[id]=row;}}
                    if(step%25==0)check(db.verify(tx)["ok"].get<bool>(),"random multi-index consistency");
                }
                check(db.verify(tx)["ok"].get<bool>(),"random batch verify before commit");db.commit(tx);if(batch%2)db.checkpoint();
            }
        }
        for(const char* fault:{"wal_append_once","wal_sync_once"}){
            auto io=path/fault;{edudb::Storage db(io.u8string());auto tx=db.begin();db.write_table(tx,"a",table(1));db.write_table(tx,"b",table(1));db.commit(tx);}
            environment("EDUDB_TEST_IO_FAULT",fault);environment("EDUDB_TEST_IO_FAULT_DELAY_MS","2000");
            {edudb::Storage db(io.u8string());environment("EDUDB_TEST_IO_FAULT","");environment("EDUDB_TEST_IO_FAULT_DELAY_MS","");auto a=db.begin(),b=db.begin();edudb::TransactionOptions read_options;read_options.read_only=true;auto readonly_waiter=db.begin(read_options);edudb::StoredRow ar,br;db.scan(a,"a")->next(ar);db.scan(b,"b")->next(br);db.update_row(a,"a",ar.id,json::array({0,"first"}));db.update_row(b,"b",br.id,json::array({0,"second"}));
                auto commit=[&](const auto& t){try{db.commit(t);return std::string("ACK");}catch(const edudb::DbError& e){return e.code;}};
                auto first=std::async(std::launch::async,[&]{return commit(a);});std::this_thread::sleep_for(std::chrono::milliseconds(30));auto second=std::async(std::launch::async,[&]{return commit(b);});
                // Writer B can now wait before WAL on the exclusive ownership
                // permit. A real no-dirty-page commit bypasses that write gate
                // and reaches the existing wal_committers fault barrier.
                auto wal_waiter=std::async(std::launch::async,[&]{return commit(readonly_waiter);});
                check(first.get()=="IO_ERROR","one-shot I/O failure injected");check(second.get()=="RECOVERY_REQUIRED","waiting writer must fail-stop");check(wal_waiter.get()=="RECOVERY_REQUIRED","actual WAL-lock waiter must fail-stop");
                auto diagnostic=db.diagnostics();check(diagnostic["failed_waiting_writers"].get<std::uint64_t>()>=1,"test proves failure recheck under WAL mutex");
                check(diagnostic["failed_waiting_writers"].get<std::uint64_t>()+diagnostic["failed_pre_wal_commits"].get<std::uint64_t>()==2,"both contenders rejected at actual WAL/admission boundary");
                check(diagnostic["active_transactions"]==0,"all failed commits release registry");
                bool blocked=false;try{db.begin();}catch(const edudb::DbError& e){blocked=e.code=="RECOVERY_REQUIRED";}check(blocked,"fail-stop rejects subsequent transactions");}
            {edudb::Storage recovered(io.u8string());auto tx=recovered.begin();check(recovered.verify(tx)["ok"].get<bool>(),"recovery after injected I/O failure is consistent");check(recovered.read_table(tx,"b")["rows"]==table(1)["rows"],"rejected second writer never reached durable contents");recovered.commit(tx);}
        }
        std::cout<<"PASS storage-v3: incremental heap/index, split/borrow/merge/root-shrink, stable LIDs and RID generations, streaming scans, final private images, WAL header CRC and fail-stop writers\n";
        fs::remove_all(path);return 0;
    }catch(const std::exception& e){std::cerr<<"FAIL storage: "<<e.what()<<"\nEvidence directory: "<<path.u8string()<<"\n";return 1;}
}
