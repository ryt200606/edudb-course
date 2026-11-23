// Included inside Storage::Impl. Bounded initial runs and fan-in-16 external
// merge feed the access module's bottom-up sorted-stream builder. Temporary
// files are uncommitted workspace; every frame is checked before consumption.
void bulk_index(const std::shared_ptr<Transaction>& tx,PageId owner,json& ix,
                std::unique_ptr<RowCursor> cursor,const json& desc){
    std::size_t budget=64u*1024u*1024u;
    if(const char* raw=std::getenv("EDUDB_SORT_MEMORY_BYTES")){
        try{std::string text(raw);if(text.empty()||text.find_first_not_of("0123456789")!=std::string::npos)throw std::invalid_argument("syntax");auto n=std::stoull(text);if(n<256u*1024u||n>64u*1024u*1024u)throw std::invalid_argument("range");budget=static_cast<std::size_t>(n);}
        catch(...){throw DbError("INVALID_CONFIG","EDUDB_SORT_MEMORY_BYTES must be 262144..67108864");}
    }
    constexpr std::size_t fan_in=16,header_size=64,maximum_frame=16u*1024u*1024u;
    struct Run {fs::path path;std::uint64_t id=0,rows=0,bytes=0;};
    struct Reader {std::shared_ptr<void> file;Run run;std::uint64_t at=header_size,rows=0;};
    struct Head {access::Entry entry;std::size_t reader=0;};
    const auto base=fs::absolute(directory).lexically_normal();
    const auto prefix="edudb-sort-"+uuid()+"-";
    std::vector<fs::path> owned;
    std::uint64_t serial=0,input_rows=0,initial_runs=0,created_runs=0,merge_passes=0,written=0,read_bytes=0,peak_estimate=0;
    auto cleanup=[&](bool strict){std::string error;
        for(const auto& path:owned){if(path.parent_path()!=base||path.filename().u8string().rfind(prefix,0)!=0){if(error.empty())error="Temporary file escaped its owned directory";continue;}std::error_code ec;fs::remove(path,ec);if(ec&&error.empty())error="Cannot remove owned sort temporary file: "+ec.message();}
        if(strict&&!error.empty())throw DbError("IO_ERROR",error);
    };
    auto earlier=[](const access::Entry& a,const access::Entry& b){auto c=access::compare_keys(a.key,b.key);return c?c<0:a.id<b.id;};
    auto later=[&](const Head& a,const Head& b){return earlier(b.entry,a.entry);};
    std::function<std::size_t(const json&)> allocation=[&](const json& v)->std::size_t{
        std::size_t bytes=sizeof(json)+32;
        if(v.is_string())bytes+=sizeof(std::string)+v.get_ref<const std::string&>().capacity()+1;
        else if(v.is_array()){bytes+=sizeof(json::array_t);for(const auto& e:v)bytes+=allocation(e)+sizeof(json);}
        else if(v.is_object()){bytes+=sizeof(json::object_t);for(auto it=v.begin();it!=v.end();++it)bytes+=allocation(it.value())+it.key().capacity()+1+sizeof(std::string)+4*sizeof(void*);}
        return bytes;
    };
    auto write_frame=[&](NativeFile& file,Run& run,const access::Entry& e){auto text=json::array({e.key,e.id,e.payload}).dump();if(text.empty()||text.size()>maximum_frame)throw DbError("INDEX_ENTRY_TOO_LARGE","Sort entry exceeds framed record limit");std::array<unsigned char,8> header{};put32(header.data(),static_cast<std::uint32_t>(text.size()));put32(header.data()+4,crc32(reinterpret_cast<const unsigned char*>(text.data()),text.size()));file.write(run.bytes,header.data(),header.size());file.write(run.bytes+header.size(),reinterpret_cast<const unsigned char*>(text.data()),text.size());run.bytes+=header.size()+text.size();written+=header.size()+text.size();++run.rows;};
    auto new_run=[&](std::unique_ptr<NativeFile>& file){Run run;run.id=++serial;run.path=base/(prefix+std::to_string(run.id)+".tmp");
        if(run.path.parent_path()!=base)throw DbError("IO_ERROR","Invalid sort workspace path");
        file=std::make_unique<NativeFile>(run.path,true,false,false,true);owned.push_back(run.path);
        if(db_uuid.size()!=32)throw DbError("CORRUPT_CONTROL","Sort database UUID must have 32 bytes");
        std::array<unsigned char,header_size> header{};std::memcpy(header.data(),"EDUSRT3",7);std::memcpy(header.data()+8,db_uuid.data(),32);put64(header.data()+40,run.id);put32(header.data()+48,3);put32(header.data()+52,header_size);put32(header.data()+60,crc32(header.data(),header.size()));file->write(0,header.data(),header.size());run.bytes=header_size;written+=header_size;++created_runs;return run;
    };
    auto open_reader=[&](const Run& run){auto reader=std::make_unique<Reader>();reader->run=run;reader->file=std::make_shared<NativeFile>(run.path,false);if(static_cast<NativeFile*>(reader->file.get())->size()!=run.bytes||run.bytes<header_size)throw DbError("SORT_CORRUPT","Sort run size mismatch");std::array<unsigned char,header_size> header{};static_cast<NativeFile*>(reader->file.get())->read(0,header.data(),header.size());read_bytes+=header.size();auto sum=get32(header.data()+60);put32(header.data()+60,0);if(std::memcmp(header.data(),"EDUSRT3",7)||std::memcmp(header.data()+8,db_uuid.data(),32)||get64(header.data()+40)!=run.id||get32(header.data()+48)!=3||get32(header.data()+52)!=header_size||sum!=crc32(header.data(),header.size()))throw DbError("SORT_CORRUPT","Sort run header/UUID/checksum mismatch");return reader;};
    auto read_next=[&](Reader& reader,access::Entry& entry){if(reader.at==reader.run.bytes){if(reader.rows!=reader.run.rows)throw DbError("SORT_CORRUPT","Sort run row count mismatch");return false;}if(reader.rows>=reader.run.rows||reader.at>reader.run.bytes||reader.run.bytes-reader.at<8)throw DbError("SORT_CORRUPT","Truncated sort frame");std::array<unsigned char,8> header{};static_cast<NativeFile*>(reader.file.get())->read(reader.at,header.data(),header.size());read_bytes+=header.size();auto length=get32(header.data());if(!length||length>maximum_frame||length>reader.run.bytes-reader.at-header.size())throw DbError("SORT_CORRUPT","Invalid sort frame length");std::string text(length,'\0');static_cast<NativeFile*>(reader.file.get())->read(reader.at+header.size(),reinterpret_cast<unsigned char*>(text.data()),text.size());read_bytes+=text.size();if(get32(header.data()+4)!=crc32(reinterpret_cast<const unsigned char*>(text.data()),text.size()))throw DbError("SORT_CORRUPT","Sort frame checksum mismatch");
        try{auto value=json::parse(text);if(!value.is_array()||value.size()!=3||!value[0].is_array()||!value[1].is_number_unsigned())throw DbError("SORT_CORRUPT","Malformed sort entry");entry={value[0],value[1].get<std::uint64_t>(),value[2]};}catch(const DbError&){throw;}catch(...){throw DbError("SORT_CORRUPT","Invalid JSON sort entry");}reader.at+=header.size()+length;++reader.rows;return true;
    };
    try{
        {
            std::vector<Run> runs;std::vector<access::Entry> buffer;std::size_t used=0;
            auto spill=[&]{if(buffer.empty())return;std::sort(buffer.begin(),buffer.end(),earlier);std::unique_ptr<NativeFile> file;auto run=new_run(file);for(const auto& e:buffer)write_frame(*file,run,e);file.reset();runs.push_back(std::move(run));buffer.clear();used=0;++initial_runs;};
            StoredRow row;
            while(cursor->next(row)){
                if(org_method(desc)!="HEAP"&&ix.at("options").value("locator",std::string("primary"))=="direct"){auto physical=desc.at("organization_index");access::Index organization(pager(tx,owner,true),physical);auto page=organization.locate(key_for(row.values,desc.at("organization").at("column_numbers")),row.id.logical_id);if(!page)throw DbError("CORRUPT_INDEX","Cannot locate organization record while building direct secondary index");row.id.page_id=*page;row.id.slot_id=0;row.id.generation=1;}
                access::Entry entry{key_for(row.values,ix.at("column_numbers")),row.id.logical_id,secondary_payload(desc,ix,row.values,row.id)};
                auto bytes=sizeof(access::Entry)+allocation(entry.key)+allocation(entry.payload)+128;if(bytes>budget)throw DbError("INDEX_ENTRY_TOO_LARGE","One index entry exceeds the configured sort run budget");if(!buffer.empty()&&bytes>budget-used)spill();buffer.push_back(std::move(entry));used+=bytes;peak_estimate=std::max<std::uint64_t>(peak_estimate,used);++input_rows;
            }
            row.values=nullptr;cursor.reset();
            access::Index target(pager(tx,owner),ix);
            std::optional<access::Entry> previous;
            std::uint64_t output_rows=0;
            auto checked=[&](const std::function<bool(access::Entry&)>& next,access::Entry& e){if(!next(e))return false;if(previous){int order=access::compare_keys(previous->key,e.key);if(order>0||(!order&&previous->id>=e.id))throw DbError("SORT_CORRUPT","Merged stream is unordered or repeats a row identity");if(ix.value("unique",false)&&!order){bool null=false;for(const auto& v:e.key)null=null||v.is_null();if(!null)throw DbError("UNIQUE_VIOLATION","Duplicate key in index "+ix.value("name",std::string("<unnamed>")));}}previous=access::Entry{e.key,e.id,nullptr};++output_rows;return true;};
            if(runs.empty()){
                std::sort(buffer.begin(),buffer.end(),earlier);std::size_t at=0;std::function<bool(access::Entry&)> next=[&](access::Entry& e){if(at>=buffer.size())return false;e=std::move(buffer[at++]);return true;};target.bulk_load_sorted([&](access::Entry& e){return checked(next,e);});
            }else{
                spill();std::vector<access::Entry>().swap(buffer);
                // Fault injection touches only this job's first closed run.
                // One CRC bit is flipped without resealing the frame/header.
                if(const char* fault=std::getenv("EDUDB_TEST_SORT_CORRUPT")){
                    const std::string mode(fault);if(!mode.empty()){
                        if(mode!="header"&&mode!="frame")throw DbError("INVALID_CONFIG","EDUDB_TEST_SORT_CORRUPT must be header or frame");
                        NativeFile damaged(runs.front().path,false);std::array<unsigned char,1> byte{};auto offset=mode=="header"?std::uint64_t(60):std::uint64_t(header_size+4);damaged.read(offset,byte.data(),1);byte[0]^=1;damaged.write(offset,byte.data(),1);
                    }
                }
                // Open at most 16 readers and retain only one head per run.
                auto merge=[&](const std::vector<Run>& inputs,const std::function<void(const std::function<bool(access::Entry&)>&)>& consume){std::vector<std::unique_ptr<Reader>> readers;std::vector<Head> heap;for(const auto& run:inputs){readers.push_back(open_reader(run));access::Entry e;if(read_next(*readers.back(),e)){heap.push_back({std::move(e),readers.size()-1});std::push_heap(heap.begin(),heap.end(),later);}}
                    std::function<bool(access::Entry&)> next=[&](access::Entry& e){if(heap.empty())return false;std::pop_heap(heap.begin(),heap.end(),later);auto head=std::move(heap.back());heap.pop_back();e=std::move(head.entry);access::Entry after;if(read_next(*readers[head.reader],after)){heap.push_back({std::move(after),head.reader});std::push_heap(heap.begin(),heap.end(),later);}return true;};consume(next);};
                while(runs.size()>fan_in){std::vector<Run> merged;for(std::size_t start=0;start<runs.size();start+=fan_in){auto end=std::min(runs.size(),start+fan_in);std::vector<Run> group(runs.begin()+static_cast<std::ptrdiff_t>(start),runs.begin()+static_cast<std::ptrdiff_t>(end));if(group.size()==1){merged.push_back(group[0]);continue;}std::unique_ptr<NativeFile> file;auto output=new_run(file);merge(group,[&](const std::function<bool(access::Entry&)>& next){access::Entry e;while(next(e))write_frame(*file,output,e);});file.reset();merged.push_back(std::move(output));for(const auto& old:group){std::error_code ec;fs::remove(old.path,ec);if(ec)throw DbError("IO_ERROR","Cannot remove consumed sort run: "+ec.message());}}runs=std::move(merged);++merge_passes;}
                if(runs.size()>1)++merge_passes;
                merge(runs,[&](const std::function<bool(access::Entry&)>& next){target.bulk_load_sorted([&](access::Entry& e){return checked(next,e);});});
            }
            if(output_rows!=input_rows)throw DbError("SORT_CORRUPT","Sorted output row count mismatch");
            ix["bulk_sort"]={{"input_rows",input_rows},{"memory_budget_bytes",budget},{"peak_estimated_entry_bytes",peak_estimate},{"memory_scope","estimated Entry JSON allocation; excludes cursor, private transaction pages, allocator overhead and process RSS"},{"initial_runs",initial_runs},{"temporary_runs_created",created_runs},{"merge_passes",merge_passes},{"fan_in",fan_in},{"temp_bytes_written",written},{"temp_bytes_read",read_bytes},{"frame_crc",true},{"adjacent_unique_check",ix.value("unique",false)},{"streamed_to_builder",true}};
        }
        cleanup(true);
    }catch(...){cleanup(false);throw;}
}
