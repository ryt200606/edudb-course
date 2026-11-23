// Atomic content interchange. Paths are never supplied to the storage engine.
namespace {
constexpr std::size_t interchange_limit=64u*1024u*1024u;
std::string request_string(const json& r,const std::string& k,bool required=false){
    if(!r.contains(k)){if(required)fail("INVALID_REQUEST","Missing string field: "+k);return "";}
    if(!r[k].is_string())fail("INVALID_REQUEST","String field required: "+k);
    return r[k].get<std::string>();
}
void interchange_request(const json& r){input::check_json_value(r);if(!r.is_object())fail("INVALID_REQUEST","Interchange request must be an object");if(r.contains("header")&&!r["header"].is_boolean())fail("INVALID_REQUEST","header must be BOOLEAN");}
json import_cell(const interchange::Cell& cell,const json& c){
    if(cell.is_null)return convert(nullptr,c);
    auto type=c["type"].get<std::string>();
    if(type=="VARCHAR")return convert(cell.text,c);
    if(type=="BLOB")return convert(json{{"$blob",cell.text}},c);
    if(type=="BOOLEAN"){auto v=upper(cell.text);if(v!="TRUE"&&v!="FALSE")fail("TYPE_ERROR","CSV BOOLEAN requires true or false");return convert(v=="TRUE",c);}
    if(cell.text.empty()||cell.text.find_first_of(" \t\r\n")!=std::string::npos)fail("TYPE_ERROR","CSV numeric cell must be nonempty and have no whitespace");
    try{
        if(type=="INTEGER"){std::size_t end=0;auto v=std::stoll(cell.text,&end);if(end!=cell.text.size())fail("TYPE_ERROR","Invalid CSV INTEGER");return convert(json(v),c);}
        auto tokens=lex(cell.text);bool literal=tokens.size()==2&&tokens[0].kind==Token::Number;
        literal=literal||(tokens.size()==3&&(tokens[0].text=="+"||tokens[0].text=="-")&&tokens[1].kind==Token::Number);
        if(!literal)fail("TYPE_ERROR","CSV REAL requires a decimal or scientific numeric literal");
        errno=0;char* end=nullptr;auto v=std::strtod(cell.text.c_str(),&end);
        if(end!=cell.text.c_str()+cell.text.size()||!std::isfinite(v)||(errno==ERANGE&&v==0))fail("NUMERIC_OVERFLOW","CSV REAL outside finite representable range");
        return convert(json(v),c);
    }catch(const DbError&){throw;}catch(const std::out_of_range&){fail("NUMERIC_OVERFLOW","CSV INTEGER outside int64 range");}catch(...){fail("TYPE_ERROR","Invalid CSV numeric value");}
}
json interchange_error(const std::string& state,const std::string& code,const std::string& message,int pos=-1){auto r=blank_result(state);r["ok"]=false;r["error"]={{"code",code},{"message",message},{"position",pos}};if(code=="IO_ERROR"||code=="RECOVERY_REQUIRED")r["write_outcome"]="unknown; do not automatically retry";return r;}
}

json Engine::import_data(const std::string& id,const json& request){
    const auto started=Clock::now();IoScope io("import");auto result=blank_result();
    std::shared_ptr<Impl::Session> session;std::unique_lock<std::mutex> guard;bool owned=false;std::size_t line=0;std::string input_format;
    try{
        session=impl_->get(id);guard=std::unique_lock<std::mutex>(session->mutex);session->touched=Clock::now();
        if(session->closed)fail("SESSION_NOT_FOUND","Session closed");
        if(impl_->storage.read_only())fail("READ_ONLY_DATABASE","Legacy control3 is read-only; use offline migrate-v3 into a new directory");
        if(session->tx||session->aborted)fail("TRANSACTION_ACTIVE","Import requires an idle session; finish or roll back the transaction");
        interchange_request(request);auto format=lower(request_string(request,"format",true));input_format=format;auto content=request_string(request,"content",true);
        if(content.size()>interchange_limit)fail("IMPORT_LIMIT_EXCEEDED","Import exceeds 64 MiB UTF-8 content limit");
        if(format!="csv"&&format!="sql")fail("INVALID_REQUEST","format must be csv or sql");
        if(format=="csv"){
            auto name=request_string(request,"table",true);auto parsed=interchange::parse_csv(content);
            impl_->start_transaction(*session);owned=true;if(session->active_options.protocol==TransactionProtocol::Tree)fail("TREE_UNSUPPORTED","TREE import/export requires unrestricted accesses and is explicitly rejected");
            Statement access;access.kind="INSERT";access.table=name;impl_->before_statement(*session,access);
            impl_->acquire(*session,"catalog",false);impl_->acquire(*session,"table:"+name,true);
            impl_->materialized_write_guard(*session,name);auto t=impl_->table(*session,name,false);std::vector<int> targets;std::size_t first=0;
            if(request.value("header",true)&&!parsed.empty()){
                std::set<std::string> names;for(auto& c:parsed[0]){if(c.is_null||!names.insert(c.text).second)fail("DUPLICATE_COLUMN","CSV header names must be unique and non-NULL");targets.push_back(column_id(t,c.text));}first=1;
            }else for(std::size_t i=0;i<t["columns"].size();++i)targets.push_back(static_cast<int>(i));
            for(std::size_t i=first;i<parsed.size();++i){line=i+1;if(parsed[i].size()!=targets.size())fail("COLUMN_COUNT_MISMATCH","CSV row width differs from header/schema");json row=json::array();for(auto& c:t["columns"])row.push_back(c["default"]);for(std::size_t c=0;c<targets.size();++c)row[targets[c]]=import_cell(parsed[i][c],t["columns"][targets[c]]);impl_->storage.insert_row(session->tx,name,validate_row(row,t["columns"]));result["affected_rows"]=result["affected_rows"].get<std::uint64_t>()+1;}
            if(result["affected_rows"].get<std::uint64_t>())result["materialized_refreshes"]=impl_->materialized_maintain(*session,{name});
        }else{
            auto scripts=interchange::split_sql_script(content);if(scripts.size()>100000)fail("IMPORT_LIMIT_EXCEEDED","SQL import exceeds 100000 statements");
            std::size_t ast_nodes=0,parsed_estimate=content.size()*2;std::vector<Statement> statements;
            for(auto& text:scripts){
                ++line;if(ast_nodes>=input::sql_ast_node_limit)fail("IMPORT_RESOURCE","SQL import AST forest exceeds250000 nodes",0);
                auto st=Parser(text).parse();static const std::set<std::string> allowed={"CREATE_CLUSTER","DROP_CLUSTER","CREATE_TABLE","DROP_TABLE","ALTER_TABLE","CREATE_INDEX","DROP_INDEX","REINDEX","REORGANIZE","INSERT","UPDATE","DELETE","TRUNCATE","CREATE_MATERIALIZED_VIEW","DROP_MATERIALIZED_VIEW","REFRESH_MATERIALIZED_VIEW"};if(!allowed.count(st.kind))fail("INVALID_IMPORT_STATEMENT","SQL import accepts DDL/DML only; no transaction control, SELECT or CHECKPOINT");
                if(st.input_ast_nodes>input::sql_ast_node_limit-ast_nodes)fail("IMPORT_RESOURCE","SQL import AST forest exceeds250000 nodes",0);
                if(st.input_ast_estimated_bytes>input::sql_import_ast_estimate_limit-parsed_estimate)fail("IMPORT_RESOURCE","SQL import cumulative parsed allocation estimate exceeds384 MiB (not RSS)",0);
                ast_nodes+=st.input_ast_nodes;parsed_estimate+=st.input_ast_estimated_bytes;statements.push_back(std::move(st));
            }
            line=0;
            impl_->start_transaction(*session);owned=true;if(session->active_options.protocol==TransactionProtocol::Tree)fail("TREE_UNSUPPORTED","TREE import/export requires unrestricted accesses and is explicitly rejected");
            for(auto& st:statements){++line;if(impl_->before_statement(*session,st))continue;auto r=impl_->run(*session,st);result["affected_rows"]=result["affected_rows"].get<std::uint64_t>()+r.value("affected_rows",std::uint64_t(0));}
            result["statements"]=statements.size();
        }
        auto before=impl_->storage.transaction_stats(session->tx);auto tx=session->tx;impl_->commit(*session);owned=false;auto after=impl_->storage.transaction_stats(tx);
        result["metrics"]["dirty_pages"]=before.value("dirty_pages",std::uint64_t(0));result["metrics"]["wal_bytes"]=after.value("wal_bytes",std::uint64_t(0));result["format"]=format;result["atomic"]=true;result["transaction_state"]="idle";
    }catch(const DbError& e){if(owned&&session){try{impl_->rollback(*session);}catch(...){}owned=false;}result=interchange_error(session?impl_->state(*session):"idle",e.code,std::string(e.what())+(line?" (input row/statement "+std::to_string(line)+")":""),e.position);if(line)result["error"][input_format=="csv"?"input_record":"input_statement"]=line;result["atomic"]=true;}
    catch(const std::exception& e){if(owned&&session){try{impl_->rollback(*session);}catch(...){}}result=interchange_error(session?impl_->state(*session):"idle","REQUEST_ERROR",e.what());}
    if(session)session->touched=Clock::now();
    if(guard.owns_lock())guard.unlock();
    if(result.value("ok",false))impl_->refresh_cache();
    result["metrics"]["elapsed_ms"]=std::chrono::duration<double,std::milli>(Clock::now()-started).count();result["metrics"]["io"]=io.snapshot();return result;
}

json Engine::export_data(const std::string& id,const json& request){
    const auto started=Clock::now();IoScope io("export");auto result=blank_result();std::shared_ptr<Impl::Session> session;std::unique_lock<std::mutex> guard;bool owned=false;
    try{
        session=impl_->get(id);guard=std::unique_lock<std::mutex>(session->mutex);session->touched=Clock::now();
        if(session->closed)fail("SESSION_NOT_FOUND","Session closed");
        if(session->tx||session->aborted)fail("TRANSACTION_ACTIVE","Export requires an idle session");
        interchange_request(request);auto format=lower(request_string(request,"format",true));auto name=request_string(request,"table");auto sql=request_string(request,"sql");
        if(format!="csv"&&format!="sql")fail("INVALID_REQUEST","format must be csv or sql");
        if((!name.empty()&&!sql.empty())||(format=="sql"&&!sql.empty()))fail("INVALID_REQUEST","Choose table or SELECT sql; SQL dump cannot use SELECT");
        impl_->start_transaction(*session);owned=true;if(session->active_options.protocol==TransactionProtocol::Tree)fail("TREE_UNSUPPORTED","TREE import/export requires unrestricted accesses and is explicitly rejected");impl_->acquire(*session,"catalog",false);std::string content;std::uint64_t rows=0;
        if(format=="csv"){
            json columns,values;
            if(!sql.empty()){auto st=Parser(sql).parse();if(st.kind!="SELECT"||st.explain)fail("INVALID_REQUEST","CSV query export requires SELECT without EXPLAIN");impl_->before_statement(*session,st);auto r=impl_->run(*session,st);columns=r["columns"];values=r["rows"];}
            else{
                if(name.empty())fail("INVALID_REQUEST","CSV export requires table or SELECT sql");
                Statement access;access.kind="DESCRIBE";access.table=name;impl_->before_statement(*session,access);
                impl_->acquire(*session,"table:"+name,false);auto t=impl_->table(*session,name,false);columns=t["columns"];values=json::array();auto cursor=impl_->storage.scan(session->tx,name);StoredRow row;std::size_t bytes=0;
                while(cursor->next(row)){bytes+=row.values.dump().size();if(bytes>interchange_limit||values.size()>=1000000)fail("EXPORT_LIMIT_EXCEEDED","Export exceeds 64 MiB or 1000000 rows; use a limited SELECT");values.push_back(row.values);}
            }
            rows=values.size();content=interchange::encode_csv(columns,values,request.value("header",true));
        }else{
            impl_->storage.refresh_read_view(session->tx,{{"__catalog__",true,false,false}});
            std::vector<std::string> names;if(name.empty()){auto cat=impl_->storage.catalog(session->tx);for(auto it=cat.begin();it!=cat.end();++it)names.push_back(it.key());}else names.push_back(name);
            const auto features=impl_->storage.schema_features(session->tx);
            names.erase(std::remove_if(names.begin(),names.end(),[&](const std::string& n){return features.value(n,json::object()).contains("materialized_aux");}),names.end());
            session->current_accesses={{"__catalog__",true,false,false}};for(auto& n:names)session->current_accesses.push_back({n,true,false,false});
            impl_->storage.refresh_read_view(session->tx,session->current_accesses);
            for(auto& n:names)impl_->acquire(*session,"table:"+n,false);
            std::stable_sort(names.begin(),names.end(),[&](const std::string& a,const std::string& b){return int(Impl::materialized_metadata(features.value(a,json::object())))<int(Impl::materialized_metadata(features.value(b,json::object())));});
            content="-- EduDB v3 logical export; import atomically in an idle session\n";
            std::set<std::string> cluster_names;std::vector<std::pair<std::string,json>> attachments;
            if(name.empty()){auto all=impl_->storage.clusters(session->tx);for(auto it=all.begin();it!=all.end();++it)cluster_names.insert(it.key());}
            for(auto& n:names){auto metadata=impl_->table(*session,n,false);if(metadata.contains("cluster")){cluster_names.insert(metadata["cluster"]["name"].get<std::string>());attachments.emplace_back(n,metadata["cluster"]);}}
            for(const auto& cluster:cluster_names)content+="CREATE CLUSTER "+interchange::quote_identifier(cluster)+";\n";
            std::size_t raw_bytes=0;
            std::vector<std::pair<std::string,json>> materialized_exports;
            for(auto& n:names){auto t=impl_->table(*session,n,false);if(Impl::materialized_metadata(t)){if(!name.empty())fail("MATERIALIZED_EXPORT_REQUIRES_DATABASE","Export the full database to include materialized-view dependencies");rows+=t.value("row_count",std::uint64_t(0));if(rows>1000000)fail("EXPORT_LIMIT_EXCEEDED","Logical export exceeds1000000 rows");materialized_exports.emplace_back(n,t);continue;}t["rows"]=json::array();auto cursor=impl_->storage.scan(session->tx,n);StoredRow row;while(cursor->next(row)){raw_bytes+=row.values.dump().size();++rows;if(raw_bytes>interchange_limit||rows>1000000)fail("EXPORT_LIMIT_EXCEEDED","Logical export exceeds 64 MiB or 1000000 rows");t["rows"].push_back(row.values);}t.erase("cluster");content+=interchange::dump_table(n,t);if(content.size()>interchange_limit)fail("EXPORT_LIMIT_EXCEEDED","Encoded logical export exceeds 64 MiB");}
            for(const auto& attachment:attachments){content+="ALTER TABLE "+interchange::quote_identifier(attachment.first)+" CLUSTER "+interchange::quote_identifier(attachment.second.at("name").get<std::string>())+"(";bool first=true;for(const auto& column:attachment.second.at("columns")){if(!first)content+=", ";first=false;content+=interchange::quote_identifier(column.get<std::string>());}content+=");\n";}
            for(const auto& view:materialized_exports){content+=impl_->materialized_dump(*session,view.first,view.second);if(content.size()>interchange_limit)fail("EXPORT_LIMIT_EXCEEDED","Encoded logical export exceeds64MiB");}
        }
        if(content.size()>interchange_limit)fail("EXPORT_LIMIT_EXCEEDED","Encoded export exceeds 64 MiB");
        impl_->commit(*session);owned=false;result["content"]=std::move(content);result["format"]=format;result["filename"]="edudb-export."+format;result["mime"]=format=="csv"?"text/csv; charset=utf-8":"application/sql; charset=utf-8";result["row_count"]=rows;result["bytes"]=result["content"].get_ref<const std::string&>().size();result["transaction_state"]="idle";
    }catch(const DbError& e){if(owned&&session){try{impl_->rollback(*session);}catch(...){}}result=interchange_error(session?impl_->state(*session):"idle",e.code,e.what(),e.position);}
    catch(const std::exception& e){if(owned&&session){try{impl_->rollback(*session);}catch(...){}}result=interchange_error(session?impl_->state(*session):"idle","REQUEST_ERROR",e.what());}
    if(session)session->touched=Clock::now();
    result["metrics"]["elapsed_ms"]=std::chrono::duration<double,std::milli>(Clock::now()-started).count();result["metrics"]["io"]=io.snapshot();return result;
}

json Engine::inspect(const std::string& id,const json& request){
    std::string state="idle";
    try{
        {auto session=impl_->get(id);std::lock_guard<std::mutex> guard(session->mutex);state=impl_->state(*session);}
        input::check_json_value(request);if(!request.is_object())fail("INVALID_REQUEST","Inspection request must be an object");
        auto t=request_string(request,"table"),i=request_string(request,"index");if(t.empty()&&i.empty())fail("INVALID_REQUEST","Inspection requires table or index");
        std::uint64_t maximum=2048;if(request.contains("max_pages")){if(!request["max_pages"].is_number_integer()||request["max_pages"].get<std::int64_t>()<1||request["max_pages"].get<std::uint64_t>()>2048)fail("ARGUMENT","max_pages must be 1..2048");maximum=request["max_pages"].get<std::uint64_t>();}
        auto text=t.empty()?"INSPECT INDEX "+interchange::quote_identifier(i):"INSPECT TABLE "+interchange::quote_identifier(t)+(i.empty()?"":" INDEX "+interchange::quote_identifier(i));
        return execute(id,text+" LIMIT "+std::to_string(maximum));
    }catch(const DbError& e){return interchange_error(state,e.code,e.what(),e.position);}
}
