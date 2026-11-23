// Inside Storage::Impl, after v3 helpers. Source rows are captured at the
// shared native mutation boundary, covering CLI/SQL/CSV and internal callers.
void feed_emit(const std::shared_ptr<Transaction>& tx,const std::string& name,const std::string& kind,std::uint64_t id,const json& before,const json& after){
    try{tx->state->changefeed.emit(name,kind,id,before,after);}
    catch(...){tx->state->aborted=true;throw;}
}
json feed_row(const std::shared_ptr<Transaction>& tx,const std::string& name,std::uint64_t id){auto d=descriptor(tx,name);return stored_at(tx,d.first,d.second,id);}
