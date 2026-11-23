// Inside Storage::Impl, before storage_v3.inc. Legacy JSON paths retain their
// byte format; binary records are enabled only by explicit table metadata.
void validate_record_table(const json& table){
    if(!table.contains("record_format")&&!table.contains("record_options"))return;
    try{auto schema=record::schema(table);auto org=table.value("organization",json{{"method","HEAP"}});auto method=org.is_string()?org.get<std::string>():org.value("method",std::string("HEAP"));
        if(schema.format!=record::Format::Json&&method!="HEAP")throw DbError("RECORD_LAYOUT","Binary record formats currently require HEAP organization; access-method payloads retain JSON records");
    }catch(const record::Error& e){throw DbError(e.code,e.what());}
}
std::string pack_heap_record(const json& d,std::uint64_t lid,const json& encoded){
    if(d.at("meta").value("record_format",std::string("JSON"))=="JSON")return json{{"id",lid},{"row",encoded}}.dump();
    try{return record::encode(record::schema(d.at("meta")),lid,encoded);}catch(const record::Error& e){throw DbError(e.code,e.what());}
}
json unpack_heap_record(const json& d,const std::string& bytes){
    if(bytes.size()<4||bytes.compare(0,4,"EDRC"))return parse_json(bytes);
    try{auto row=record::decode(record::schema(d.at("meta")),bytes);return {{"id",row.logical_id},{"row",row.row}};}catch(const record::Error& e){throw DbError(e.code,e.what());}
}
json heap_record_layout(const json& metadata){
    auto organization=metadata.value("organization",json{{"method","HEAP"}});auto method=organization.is_string()?organization.get<std::string>():organization.value("method",std::string("HEAP"));
    if(method!="HEAP")return {{"format","JSON"},{"scope","access-method payload; binary HEAP codec does not apply"},{"binary_header_bytes",0},{"fixed_record_bytes",nullptr}};
    if(metadata.value("record_format",std::string("JSON"))=="JSON")return {{"format","JSON"},{"binary_header_bytes",0},{"fixed_record_bytes",nullptr},{"maximum_slot_record_bytes",PAGE_SIZE-PAGE_HEADER-SLOT_SIZE}};
    try{return record::layout(record::schema(metadata));}catch(const record::Error& e){throw DbError(e.code,e.what());}
}
