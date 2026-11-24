// Statistics definitions share table descriptors and transaction boundaries with DDL.
json statistics_command(Session& session,const Statement& statement){
    auto result=blank_result();
    if(statement.kind=="SHOW_STATISTICS"){
        acquire(session,"table:"+statement.table,false);
        auto metadata=storage.read_table(session.tx,statement.table,false,false);
        auto definitions=metadata.value("statistics_definitions",json::object());
        const auto& stats=metadata.at("statistics");
        result["columns"]={{{"name","name"},{"type","VARCHAR"}},{{"name","columns"},{"type","VARCHAR"}},{{"name","status"},{"type","VARCHAR"}}};
        for(const auto& item:definitions.items()){
            std::string status="not analyzed";
            if(stats.is_object()&&stats.contains("joint_mcv")&&stats.at("joint_mcv").contains(item.key()))
                status=stats.value("stale",false)?"stale":"fresh";
            result["rows"].push_back({item.key(),item.value().dump(),status});
        }
        result["statistics"]=stats;
        return result;
    }
    std::string owner;
    const auto features=storage.schema_features(session.tx);
    for(const auto& entry:features.items()){
        const auto& metadata=entry.value();
        if(metadata.contains("statistics_definitions")&&metadata.at("statistics_definitions").contains(statement.index)){
            owner=entry.key();
            break;
        }
    }
    if(statement.kind=="DROP_STATISTICS"){
        if(owner.empty()){
            if(statement.if_exists)return result;
            fail("STATISTICS_NOT_FOUND","Unknown statistics object: "+statement.index);
        }
        acquire(session,"table:"+owner,true);
        auto definitions=storage.read_table(session.tx,owner,false,false).at("statistics_definitions");
        definitions.erase(statement.index);
        storage.set_statistics_definitions(session.tx,owner,definitions);
        return result;
    }
    if(!owner.empty()){
        if(statement.if_not_exists)return result;
        fail("STATISTICS_EXISTS","Statistics name already exists: "+statement.index);
    }
    auto metadata=storage.read_table(session.tx,statement.table,false,false);
    if(materialized_metadata(metadata)||metadata.contains("materialized_aux"))
        fail("INVALID_STATISTICS","Define extended statistics on base tables");
    if(statement.names.size()!=2||statement.names[0]==statement.names[1])
        fail("INVALID_STATISTICS","Joint MCV statistics require two distinct columns");
    for(const auto& name:statement.names){
        if(metadata.at("columns").at(column_id(metadata,name)).at("type")=="BLOB")
            fail("INVALID_STATISTICS","Joint MCV statistics do not support BLOB");
    }
    auto definitions=metadata.value("statistics_definitions",json::object());
    for(const auto& item:definitions){
        if((item[0]==statement.names[0]&&item[1]==statement.names[1])||
           (item[1]==statement.names[0]&&item[0]==statement.names[1]))
            fail("STATISTICS_EXISTS","This column pair already has MCV statistics");
    }
    definitions[statement.index]=statement.names;
    storage.set_statistics_definitions(session.tx,statement.table,definitions);
    return result;
}
