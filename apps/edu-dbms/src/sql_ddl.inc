// DDL and catalog commands. Catalog X protects all metadata/layout changes.
json run(Session& s,Statement& st){
    auto result=blank_result();
    bool ddl=schema_statement(st)&&st.kind!="REFRESH_MATERIALIZED_VIEW";
    acquire(s,"catalog",ddl);
    if(ddl&&!st.table.empty())integrity_protocol(s,st.table);

    if(st.kind=="CREATE_MATERIALIZED_VIEW"||st.kind=="REFRESH_MATERIALIZED_VIEW"||st.kind=="DROP_MATERIALIZED_VIEW"||st.kind=="SHOW_MATERIALIZED_VIEWS")return materialized_command(s,st);
    if(auto tree_result=run_tree(s,st))return *tree_result;
    materialized_schema_guard(s,st);
    json write_features=json::object();
    if(st.kind=="INSERT"||st.kind=="UPDATE"||st.kind=="DELETE"||st.kind=="TRUNCATE")materialized_write_guard(s,st.table,&write_features);
    if(auto cluster_result=run_cluster(s,st))return *cluster_result;
    if(ddl&&!st.table.empty()&&exists(s,st.table))acquire(s,"table:"+st.table,true);
    if(st.kind=="VERIFY"){
        auto cat=storage.catalog(s.tx);std::vector<std::string> names;for(auto it=cat.begin();it!=cat.end();++it)names.push_back(it.key());std::sort(names.begin(),names.end());for(auto& name:names)acquire(s,"table:"+name,false);
        auto verification=storage.verify(s.tx);if(!verification.value("ok",false))fail("CORRUPT_DATABASE",verification.dump());
        result["verification"]=verification;result["columns"]={{{"name","verification"},{"type","VARCHAR"}}};result["rows"].push_back(json::array({verification.dump(2)}));return result;
    }
    if(st.kind=="SELECT")return select(s,st);
    if(st.kind=="ANALYZE")return analyze(s,st);
    if(st.kind=="CREATE_STATISTICS"||st.kind=="DROP_STATISTICS"||st.kind=="SHOW_STATISTICS")return statistics_command(s,st);
    if(st.kind=="SHOW_WAL_RECORDS"){
        const auto limit=st.limit==std::numeric_limits<std::size_t>::max()?4096:st.limit;
        result["diagnostic"]=storage.wal_records(limit);
        result["plan"]={{"node","ARIESLogRecords"},{"limit",limit}};return result;
    }
    if(st.kind=="SHOW_BUFFER"){auto result=blank_result();auto state=storage.diagnostics();auto view=state["buffer"];for(auto it=state.begin();it!=state.end();++it)if(it.key().rfind("buffer_",0)==0)view[it.key()]=it.value();result["diagnostic"]=view;result["plan"]={{"node","BufferPoolStatus"}};return result;}
    if(st.kind=="SHOW_MEDIA"||st.kind=="SCRUB_MEDIA"){
        auto result=blank_result();result["diagnostic"]=st.kind=="SHOW_MEDIA"?storage.diagnostics().at("media"):storage.media_scrub();result["plan"]={{"node",st.kind=="SHOW_MEDIA"?"ManagedMediaStatus":"ManagedMemberScrub"},{"scope","durable member files; transaction-private pages excluded"}};return result;
    }
    if(st.kind=="SHOW_TRANSACTIONS"||st.kind=="SHOW_LOCKS"||st.kind=="SHOW_WAL"){
        json view;
        if(st.kind=="SHOW_LOCKS")view=locks.snapshot();
        else{auto state=storage.diagnostics();view=state[st.kind=="SHOW_WAL"?"wal":"transactions"];}
        result["columns"]={{{"name","diagnostic"},{"type","VARCHAR"}}};result["rows"].push_back(json::array({view.dump(2)}));
        result["diagnostic"]=std::move(view);result["plan"]={{"node",st.kind=="SHOW_WAL"?"WALStatus":"TransactionDiagnostics"}};return result;
    }
    if(st.kind=="TRUNCATE"){
        auto graph=integrity_lock(s,st.table,"TRUNCATE",write_features);
        for(const auto& link:graph.edges)if(link.parent==st.table&&link.child!=st.table)
            fail("DEPENDENT_OBJECT","TRUNCATE is restricted by foreign key "+link.rule.at("name").get<std::string>()+"; use DELETE for row-level cascade");

        acquire(s,"table:"+st.table,true);auto metadata=table(s,st.table,false);
        auto affected=metadata.value("row_count",std::uint64_t(0));
        if(affected){metadata["rows"]=json::array();metadata["row_ids"]=json::array();storage.write_table(s.tx,st.table,metadata);}
        if(affected){result["materialized_refreshes"]=materialized_maintain(s,{st.table});}
        result["affected_rows"]=affected;result["plan"]={{"node","Truncate"},{"table",st.table},{"preserves_schema",true}};return result;
    }
    if(st.kind=="SHOW_TABLES"){
        result["columns"]={{{"name","table_name"},{"type","VARCHAR"}}};
        auto cat=storage.catalog(s.tx);const auto features=storage.schema_features(s.tx);for(auto it=cat.begin();it!=cat.end();++it)if(!features.value(it.key(),json::object()).contains("materialized_aux"))result["rows"].push_back(json::array({it.key()}));return result;
    }
    if(st.kind=="DESCRIBE"||st.kind=="SHOW_INDEXES"){
        acquire(s,"table:"+st.table,false);auto t=table(s,st.table,false);
        if(st.kind=="DESCRIBE"){
            result["columns"]={{{"name","name"},{"type","VARCHAR"}},{{"name","type"},{"type","VARCHAR"}},{{"name","not_null"},{"type","BOOLEAN"}},{{"name","primary"},{"type","BOOLEAN"}},{{"name","unique"},{"type","BOOLEAN"}},{{"name","default"},{"type","VARCHAR"}}};
            for(auto& c:t["columns"]){auto ty=c["type"].get<std::string>();if(ty=="VARCHAR"||ty=="BLOB")ty+="("+std::to_string(c["length"].get<std::uint64_t>())+")";result["rows"].push_back(json::array({c["name"],ty,c["not_null"],c["primary"],c["unique"],c["default"].is_null()?json(nullptr):json(c["default"].dump())}));}
            result["organization"]=t.value("organization",json{{"method","HEAP"}});
            result["constraints"]=t.value("integrity",json::array());
            result["record_layout"]=t.at("record_layout");if(t.contains("cluster"))result["cluster"]=t["cluster"];
            if(materialized_metadata(t)){result["materialized_view"]=t.at("materialized_view");result["materialized_refresh_state"]=t.value("materialized_refresh_state",json::object());}
        }else{
            result["columns"]={{{"name","index_name"},{"type","VARCHAR"}},{{"name","column"},{"type","VARCHAR"}},{{"name","unique"},{"type","BOOLEAN"}},{{"name","method"},{"type","VARCHAR"}},{{"name","columns"},{"type","VARCHAR"}},{{"name","include"},{"type","VARCHAR"}},{{"name","options"},{"type","VARCHAR"}}};
            for(auto& ix:t["indexes"])result["rows"].push_back(json::array({ix["name"],ix["column"],ix["unique"],ix.value("method",std::string("BPLUS")),index_columns(ix).dump(),ix.value("include",json::array()).dump(),ix.value("options",json::object()).dump()}));
        }return result;
    }
    if(st.kind=="CREATE_TABLE"){
        if(exists(s,st.table)){if(st.if_not_exists)return result;fail("TABLE_EXISTS","Table already exists: "+st.table);}
        if(st.table.rfind("__",0)==0)fail("INVALID_IDENTIFIER","Names beginning __ are reserved");
        json t={{"columns",st.columns},{"rows",json::array()},{"indexes",json::array()},{"organization",st.organization}};
        if(!st.record_format.empty()){t["record_format"]=st.record_format;t["record_options"]=st.record_options;}
        if(!st.cluster_name.empty()){acquire_named_cluster(s,st.cluster_name,true);t["cluster"]={{"name",st.cluster_name},{"columns",st.cluster_columns}};}
        add_constraint_indexes(t,st.table);std::size_t uq=0;
        for(auto& constraint:st.table_constraints){if(constraint.at("kind")!="primary"&&constraint.at("kind")!="unique")continue;auto ns=constraint["columns"];auto kind=constraint["kind"].get<std::string>();std::set<std::string> seen;
            if(kind=="primary")for(auto& x:t["indexes"])if(x.value("primary",false))fail("INVALID_SCHEMA","Only one PRIMARY KEY declaration is allowed");
            for(auto& n:ns){auto name=n.get<std::string>();if(!seen.insert(name).second)fail("DUPLICATE_COLUMN","Duplicate constraint column");auto c=column_id(t,name);if(kind=="primary")t["columns"][c]["not_null"]=true;}
            auto name=kind=="primary"?"__pk_"+st.table:"__uq_"+st.table+"_group"+std::to_string(++uq);
            t["indexes"].push_back({{"name",name},{"column",ns[0]},{"columns",ns},{"unique",true},{"primary",kind=="primary"}});
            if(ns.size()==1)t["columns"][column_id(t,ns[0].get<std::string>())][kind]=true;
        }
        validate_table(t);integrity_add(s,st.table,t,st.table_constraints);storage.write_table(s.tx,st.table,t);return result;
    }
    if(st.kind=="DROP_TABLE"){
        if(!exists(s,st.table)){if(st.if_exists)return result;fail("TABLE_NOT_FOUND","Table not found: "+st.table);}
        for(const auto& fk:integrity_graph(storage.schema_features(s.tx)).edges)if(fk.parent==st.table&&fk.child!=st.table)
            fail("DEPENDENT_OBJECT","Table is referenced by "+fk.child+"."+fk.rule.at("name").get<std::string>());
        storage.drop_table(s.tx,st.table);return result;
    }
    if(st.kind=="CREATE_INDEX"){
        if(st.index.rfind("__",0)==0)fail("INVALID_IDENTIFIER","Names beginning __ are reserved");
        if(index_exists(s,st.index)){if(st.if_not_exists)return result;fail("INDEX_EXISTS","Index already exists: "+st.index);}
        auto t=table(s,st.table,false);t["indexes"].push_back({{"name",st.index},{"column",st.column},{"columns",st.index_columns},{"method",st.method},{"include",st.include_columns},{"options",st.options},{"unique",st.unique}});
        validate_table(t);storage.set_indexes(s.tx,st.table,t["indexes"]);return result;
    }
    if(st.kind=="DROP_INDEX"){
        std::string owner;if(!index_exists(s,st.index,&owner)){if(st.if_exists)return result;fail("INDEX_NOT_FOUND","Index not found: "+st.index);}
        if(st.index.rfind("__",0)==0)fail("CONSTRAINT_INDEX","Constraint indexes cannot be dropped directly");
        auto t=table(s,owner,false);auto& xs=t["indexes"];
        integrity_protocol(s,owner);
        for(const auto& fk:integrity_graph(storage.schema_features(s.tx)).edges)if(fk.parent==owner){
            json copy=t;
            auto& available=copy["indexes"];
            available.erase(std::remove_if(available.begin(),available.end(),[&](const json& ix){return ix.at("name")==st.index;}),available.end());
            if(integrity_index(copy,fk.rule.at("keys"),true).empty())fail("DEPENDENT_OBJECT","Referenced unique index supports "+fk.rule.at("name").get<std::string>());
        }
        xs.erase(std::remove_if(xs.begin(),xs.end(),[&](const auto& x){return x["name"]==st.index;}),xs.end());storage.set_indexes(s.tx,owner,xs);return result;
    }
    if(st.kind=="REINDEX"){
        auto owner=st.table;if(owner.empty()&&!index_exists(s,st.index,&owner))fail("INDEX_NOT_FOUND","Index not found: "+st.index);acquire(s,"table:"+owner,true);table(s,owner,false);storage.reindex(s.tx,owner,st.index);result["plan"]={{"node","BulkReindex"},{"table",owner},{"index",st.index.empty()?json(nullptr):json(st.index)}};return result;
    }
    if(st.kind=="INSPECT"){
        auto owner=st.table;if(owner.empty()&&!index_exists(s,st.index,&owner))fail("INDEX_NOT_FOUND","Index not found: "+st.index);acquire(s,"table:"+owner,false);table(s,owner,false);result["inspection"]=storage.inspect(s.tx,owner,st.index,st.limit==std::numeric_limits<std::size_t>::max()?2048:st.limit);result["columns"]={{{"name","inspection"},{"type","VARCHAR"}}};result["rows"].push_back(json::array({result["inspection"].dump(2)}));return result;
    }
    if(st.kind=="REORGANIZE"||(st.kind=="ALTER_TABLE"&&st.action=="ORGANIZATION")){
        auto t=table(s,st.table);if(!st.organization.is_null())t["organization"]=st.organization;validate_table(t);storage.write_table(s.tx,st.table,t);result["plan"]={{"node","TableReorganization"},{"organization",t["organization"]}};return result;
    }
    if(st.kind=="ALTER_TABLE"){
        auto t=table(s,st.table);
        if(st.action=="ADD_CONSTRAINT"){
            integrity_add(s,st.table,t,st.table_constraints);
        }else if(st.action=="DROP_CONSTRAINT"){
            auto rules=t.value("integrity",json::array());
            auto it=std::find_if(rules.begin(),rules.end(),[&](const json& r){return r.at("name")==st.column;});
            if(it==rules.end())fail("CONSTRAINT_NOT_FOUND","Unknown CHECK or foreign key: "+st.column);
            rules.erase(it);if(rules.empty())t.erase("integrity");else t["integrity"]=rules;
        }else if(st.action=="RECORD_FORMAT"){
            t["record_format"]=st.record_format;t["record_options"]=st.record_options;
            validate_table(t);storage.write_table(s.tx,st.table,t);result["plan"]={{"node","TransactionalRecordRewrite"},{"record_format",st.record_format},{"record_options",st.record_options}};return result;
        }else if(st.action=="ADD"){
            auto c=st.columns[0];for(auto& old:t["columns"])if(old["name"]==c["name"])fail("DUPLICATE_COLUMN","Column already exists");
            t["columns"].push_back(c);for(auto& r:t["rows"])r.push_back(c["default"]);
            if(c.value("primary",false)){for(auto& ix:t["indexes"])if(ix.value("primary",false))fail("INVALID_SCHEMA","PRIMARY KEY already exists");t["indexes"].push_back({{"name","__pk_"+st.table},{"column",c["name"]},{"unique",true},{"primary",true}});}else if(c.value("unique",false))t["indexes"].push_back({{"name","__uq_"+st.table+"_"+c["name"].get<std::string>()},{"column",c["name"]},{"unique",true}});
            integrity_add(s,st.table,t,st.table_constraints);
        }else if(st.action=="DROP"){
            int c=column_id(t,st.column);if(t["columns"].size()==1)fail("INVALID_SCHEMA","Cannot drop final column");
            auto dependent=[&](const json& ns){return std::find(ns.begin(),ns.end(),json(st.column))!=ns.end();};
            for(const auto& rule:t.value("integrity",json::array()))if(dependent(rule.at("columns")))fail("DEPENDENT_OBJECT","Column participates in constraint "+rule.at("name").get<std::string>());
            for(const auto& fk:integrity_graph(storage.schema_features(s.tx)).edges)if(fk.parent==st.table&&dependent(fk.rule.at("keys")))fail("DEPENDENT_OBJECT","Column is referenced by foreign key "+fk.rule.at("name").get<std::string>());
            for(auto& x:t["indexes"])if(dependent(index_columns(x))||dependent(x.value("include",json::array())))fail("DEPENDENT_OBJECT","Column has an index/constraint dependency");
            if(t.contains("statistics_definitions"))for(const auto& entry:t["statistics_definitions"].items())
                if(dependent(entry.value()))fail("DEPENDENT_OBJECT","Drop statistics on this column before dropping the column");
            if(t.contains("cluster")&&dependent(t["cluster"]["columns"]))fail("DEPENDENT_OBJECT","Column is a clustering key; detach/recluster explicitly");
            if(t.contains("organization")&&dependent(t["organization"]["columns"]))fail("DEPENDENT_OBJECT","Column has a table organization dependency");
            t["columns"].erase(t["columns"].begin()+c);for(auto& r:t["rows"])r.erase(r.begin()+c);
        }else if(st.action=="RENAME_COLUMN"){
            integrity_rename(s,st,t);
            int c=column_id(t,st.column);for(auto& old:t["columns"])if(old["name"]==st.newname)fail("DUPLICATE_COLUMN","Column already exists: "+st.newname);t["columns"][c]["name"]=st.newname;
            auto rename=[&](json& ns){for(auto& n:ns)if(n==st.column)n=st.newname;};
            for(auto& x:t["indexes"]){auto cols=index_columns(x);rename(cols);x["columns"]=cols;x["column"]=cols[0];if(x.contains("include"))rename(x["include"]);if(x["name"]=="__uq_"+st.table+"_"+st.column)x["name"]="__uq_"+st.table+"_"+st.newname;}
            if(t.contains("statistics_definitions"))for(auto& cols:t["statistics_definitions"])rename(cols);
            if(t.contains("organization"))rename(t["organization"]["columns"]);
            if(t.contains("cluster"))rename(t["cluster"]["columns"]);
        }else{
            if(exists(s,st.newname))fail("TABLE_EXISTS","Table already exists: "+st.newname);
            if(st.newname.rfind("__",0)==0)fail("INVALID_IDENTIFIER","Names beginning __ are reserved");
            integrity_rename(s,st,t);
            for(auto& x:t["indexes"]){auto n=x["name"].get<std::string>();if(n=="__pk_"+st.table)x["name"]="__pk_"+st.newname;else if(n.rfind("__uq_"+st.table+"_",0)==0)x["name"]="__uq_"+st.newname+"_"+n.substr(st.table.size()+6);}
            validate_table(t);storage.write_table(s.tx,st.newname,t);storage.drop_table(s.tx,st.table);return result;
        }
        validate_table(t);storage.write_table(s.tx,st.table,t);return result;
    }
    auto mutated=mutate(s,st,write_features);
    if(mutated.value("affected_rows",std::uint64_t(0))){
        auto changed=mutated.value("cascade_tables",std::vector<std::string>{});changed.push_back(st.table);
        mutated["materialized_refreshes"]=materialized_maintain(s,changed);
    }
    return mutated;
}
