// Offline format-2 reader. The input is exclusively locked and never replayed
// in place. Only the owned snapshot is repaired; the new database is published
// by directory rename after every table and fingerprint has been checked.
json Storage::migrate_v2(const std::string& source,const std::string& destination){
    IoScope io("migration");auto src=fs::weakly_canonical(fs::absolute(fs::u8path(source)));auto dest=fs::weakly_canonical(fs::absolute(fs::u8path(destination)));
    auto path_key=[](const fs::path& path){auto text=path.generic_u8string();
#ifdef _WIN32
        for(char& c:text)if(c>='A'&&c<='Z')c=static_cast<char>(c-'A'+'a');
#endif
        return text+"/";
    };
    auto a=path_key(src),b=path_key(dest);if(a==b||a.rfind(b,0)==0||b.rfind(a,0)==0)throw DbError("ARGUMENT","Migration source and target must be separate directories");
    if(!fs::is_directory(src)||fs::exists(dest))throw DbError("ARGUMENT","Source must exist; target must be new");
    NativeFile lock(src/"database.lock",false,false,true);auto control=read_manifest(src/"control.json");
    if(control.value("format",std::string())!="EduDB"||control.value("version",0)!=2||control.value("page_size",0)!=PAGE_SIZE)throw DbError("INCOMPATIBLE_FORMAT","migrate-v2 requires format 2");
    auto gen=control.at("wal_generation").get<std::uint64_t>();auto token=uuid();auto parent=dest.parent_path();fs::create_directories(parent);auto snapshot=parent/(dest.filename().u8string()+".v2-snapshot-"+token);auto staging=parent/(dest.filename().u8string()+".v3-staging-"+token);
    if(!fs::create_directory(snapshot))throw DbError("IO_ERROR","Snapshot directory already exists");
    auto owned_remove=[&](const fs::path& path){if(path.parent_path()!=parent||path.filename().u8string().find(token)==std::string::npos||!fs::is_directory(path))throw DbError("IO_ERROR","Owned migration cleanup guard failed");fs::remove_all(path);};
    json receipt={{"status","IN_PROGRESS"},{"source",src.u8string()},{"destination",dest.u8string()},{"source_format",2},{"destination_format",4},{"page_format",3},{"tables",json::array()},{"source_modified",false}};
    try{
        auto walname="wal-"+std::to_string(gen)+".log";fs::copy_file(src/"data.pages",snapshot/"data.pages");fs::copy_file(src/walname,snapshot/walname);
        NativeFile data(snapshot/"data.pages",false),wal(snapshot/walname,false);auto base=control.at("checkpoint_lsn").get<std::uint64_t>();auto uuid=control.at("uuid").get<std::string>();
        std::array<unsigned char,64> header{};if(wal.size()<64)throw DbError("CORRUPT_WAL","Incomplete v2 WAL generation");wal.read(0,header.data(),header.size());auto hc=get32(header.data()+60);put32(header.data()+60,0);
        if(uuid.size()!=32||std::memcmp(header.data(),"EDUWAL2",7)||std::string(reinterpret_cast<char*>(header.data()+8),32)!=uuid||get64(header.data()+40)!=gen||get64(header.data()+48)!=base||hc!=crc32(header.data(),header.size()))throw DbError("CORRUPT_WAL","Invalid v2 generation identity/checksum");
        auto valid_v2=[](Page page,PageId id){auto c=get32(page.data()+60);put32(page.data()+60,0);return get32(page.data())==PAGE_MAGIC&&get16(page.data()+4)==2&&get64(page.data()+8)==id&&valid_layout(page)&&get16(page.data()+6)<=TYPE_RETIRED&&c==crc32(page.data(),page.size());};
        std::uint64_t off=64,last_lsn=base,pending_id=0,durable=base;std::uint32_t digest=0;std::vector<std::pair<PageId,Page>> pending;auto length=wal.size();bool tail=false;
        const auto started=std::chrono::steady_clock::now();
        while(off<length){if(std::chrono::steady_clock::now()-started>std::chrono::minutes(5))throw DbError("MIGRATION_LIMIT","v2 replay exceeded five minutes");if(length-off<48){tail=true;break;}std::array<unsigned char,48> h{};wal.read(off,h.data(),h.size());auto sum=get32(h.data()+36);put32(h.data()+36,0);if(sum!=crc32(h.data(),h.size())||get32(h.data())!=REC_MAGIC||get32(h.data()+12)!=2)throw DbError("CORRUPT_WAL","Invalid complete v2 record header");auto size=get32(h.data()+4),type=get32(h.data()+8);if((type==1&&size!=0)||(type==2&&size!=PAGE_SIZE+8)||(type==3&&size!=8)||type<1||type>3)throw DbError("CORRUPT_WAL","Invalid v2 fixed record size");if(size>length-off-48){tail=true;break;}std::vector<unsigned char> body(size);if(size)wal.read(off+48,body.data(),body.size());if(get32(h.data()+32)!=crc32(body.data(),body.size()))throw DbError("CORRUPT_WAL","Invalid complete v2 payload checksum");auto lsn=get64(h.data()+16),tid=get64(h.data()+24);if(lsn<=last_lsn)throw DbError("CORRUPT_WAL","Nonmonotone v2 LSN");last_lsn=lsn;
            if(type==1){if(pending_id)throw DbError("CORRUPT_WAL","Nested v2 transaction");pending_id=tid;digest=0;pending.clear();}
            else if(type==2){if(!pending_id||pending_id!=tid)throw DbError("CORRUPT_WAL","v2 PAGE without matching transaction");auto id=get64(body.data());Page page{};std::copy(body.begin()+8,body.end(),page.begin());if(!valid_v2(page,id)||get64(page.data()+16)!=lsn)throw DbError("CORRUPT_WAL","Invalid v2 page image");pending.emplace_back(id,page);digest=((digest<<1)|(digest>>31))^crc32(page.data(),page.size());if(pending.size()>131072)throw DbError("MIGRATION_LIMIT","v2 transaction exceeds supported page budget");}
            else{if(!pending_id||pending_id!=tid||get32(body.data())!=pending.size()||get32(body.data()+4)!=digest)throw DbError("CORRUPT_WAL","Invalid v2 COMMIT");for(const auto& p:pending)data.write(p.first*PAGE_SIZE,p.second.data(),p.second.size());pending.clear();pending_id=0;durable=lsn;}off+=48+size;
        }
        // A torn WAL cannot explain an already newer durable data page.
        for(PageId id=0;id<data.size()/PAGE_SIZE;++id){Page page{};data.read(id*PAGE_SIZE,page.data(),page.size());if(valid_v2(page,id)&&get64(page.data()+16)>durable)throw DbError("CORRUPT_WAL","v2 data is newer than valid WAL commits");}
        auto page_at=[&](PageId id){Page p{};if(id>=(std::uint64_t(1)<<48)||data.size()<(id+1)*PAGE_SIZE)throw DbError("CORRUPT_PAGE","v2 page out of file");data.read(id*PAGE_SIZE,p.data(),p.size());if(!valid_v2(p,id))throw DbError("CORRUPT_PAGE","Invalid v2 page checksum/identity");return p;};
        auto blob=[&](PageId root,std::uint16_t type){std::string result;std::set<PageId> seen;while(root){if(!seen.insert(root).second)throw DbError("CORRUPT_PAGE","v2 metadata cycle");auto p=page_at(root);if(get16(p.data()+6)!=type||get16(p.data()+32)!=1)throw DbError("CORRUPT_PAGE","Invalid v2 metadata page");result+=slot(p,0);if(result.size()>64u*1024u*1024u)throw DbError("MIGRATION_LIMIT","v2 metadata exceeds limit");root=get64(p.data()+24);}return parse_json(result);};
        auto super=page_at(0);if(parse_json(slot(super,0)).at("uuid")!=uuid)throw DbError("CORRUPT_PAGE","v2 superblock UUID mismatch");auto catalog=blob(1,TYPE_CATALOG);if(!catalog.is_object())throw DbError("CORRUPT_PAGE","Invalid v2 catalog");
        {
            Storage output(staging.u8string());for(auto it=catalog.begin();it!=catalog.end();++it){auto descriptor=blob(it.value().get<PageId>(),TYPE_DESCRIPTOR);auto meta=descriptor.at("meta");meta["rows"]=json::array();meta["organization"]={{"method","HEAP"},{"columns",json::array()},{"options",json::object()}};auto tx=output.begin();output.write_table(tx,it.key(),meta);std::uint32_t old_fingerprint=0,new_fingerprint=0;std::map<std::string,std::size_t> exact_rows;std::size_t count=0;
                for(const auto& pageid:descriptor.at("heap")){auto p=page_at(pageid.get<PageId>());if(get16(p.data()+6)!=TYPE_HEAP||get64(p.data()+40)!=it.value().get<PageId>())throw DbError("CORRUPT_PAGE","v2 heap owner mismatch");for(std::size_t i=0;i<get16(p.data()+32);++i)if(slot_length(p,i)){auto row=parse_json(slot(p,i));++exact_rows[row.dump()];old_fingerprint+=text_crc(row.dump());output.insert_row(tx,it.key(),row);++count;}}
                if(count!=descriptor.at("row_count").get<std::size_t>())throw DbError("CORRUPT_PAGE","v2 descriptor row count mismatch");auto c=output.scan(tx,it.key());StoredRow row;std::size_t actual=0;while(c->next(row)){auto encoded=row.values.dump();auto expected=exact_rows.find(encoded);if(expected==exact_rows.end()||!expected->second)throw DbError("MIGRATION_VERIFY","Migration row multiplicity differs");--expected->second;new_fingerprint+=text_crc(encoded);++actual;}if(count!=actual||old_fingerprint!=new_fingerprint)throw DbError("MIGRATION_VERIFY","Migration data fingerprint differs");auto verify=output.verify(tx);if(!verify.value("ok",false))throw DbError("MIGRATION_VERIFY",verify.dump());output.commit(tx);receipt["tables"].push_back({{"table",it.key()},{"rows",count},{"row_crc32",old_fingerprint},{"verified",true}});
            }output.checkpoint();
        }
        fs::rename(staging,dest);receipt["status"]="PASS";receipt["valid_v2_durable_lsn"]=durable;receipt["incomplete_v2_tail_ignored"]=tail||pending_id!=0;receipt["fingerprint_kind"]="exact typed-JSON multiset comparison plus additive CRC32; external SHA256 receipt separately";receipt["io"]=io.snapshot();
    }catch(...){receipt["status"]="FAIL";receipt["snapshot"]=snapshot.u8string();receipt["unpublished_staging"]=staging.u8string();throw;}
    // Snapshot files are no longer open at this point (NativeFile scope ended).
    owned_remove(snapshot);return receipt;
}
