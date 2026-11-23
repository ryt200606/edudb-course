// Explicit materialized scan reuse. Not an automatic global optimizer or a
// simultaneous pipelined shared scan. Scope exists only during one owned batch.
struct SharedScanState {
    std::string table;std::uint64_t tx=0;bool preflight=true,needs_run=false;
    QuerySpillStore* store=nullptr;QuerySpillStore::Run run;
    std::uint64_t reader_opens=0,spool_rows_read=0,matched_rows=0;
};
thread_local SharedScanState* shared_scan_state=nullptr;
struct SharedScanScope {
    SharedScanState* previous;
    explicit SharedScanScope(SharedScanState& state):previous(shared_scan_state){shared_scan_state=&state;}
    ~SharedScanScope(){shared_scan_state=previous;}
};
static bool shared_scan_matches(const std::shared_ptr<Transaction>& tx,const std::string& table){return shared_scan_state&&shared_scan_state->tx==tx->id&&shared_scan_state->table==table;}
// The storage candidate protocol must precede partial residual expressions in
// precisely the same way as the normal native TABLE/index cursor.
static json shared_candidate_truth(const json& predicate,const json& row){
    if(predicate.is_null())return true;
    query_work();const auto op=predicate.at("op").get<std::string>();
    if(op=="AND"||op=="OR"){
        const auto a=shared_candidate_truth(predicate.at("args").at(0),row),b=shared_candidate_truth(predicate.at("args").at(1),row);
        if(op=="AND"){if((a.is_boolean()&&!a.get<bool>())||(b.is_boolean()&&!b.get<bool>()))return false;if(a.is_null()||b.is_null())return nullptr;return true;}
        if((a.is_boolean()&&a.get<bool>())||(b.is_boolean()&&b.get<bool>()))return true;
        if(a.is_null()||b.is_null())return nullptr;
        return false;
    }
    if(op=="NOT"){auto a=shared_candidate_truth(predicate.at("args").at(0),row);return a.is_null()?json(nullptr):json(!a.get<bool>());}
    const auto& a=row.at(predicate.at("column").get<std::size_t>());const auto& b=predicate.at("value");
    if(op=="IS NULL")return a.is_null();
    if(op=="IS NOT NULL")return !a.is_null();
    if(a.is_null()||b.is_null())return nullptr;
    const int c=compare_values(a,b);
    if(op=="=")return c==0;
    if(op=="!=")return c!=0;
    if(op=="<")return c<0;
    if(op=="<=")return c<=0;
    if(op==">")return c>0;
    if(op==">=")return c>=0;
    fail("INTERNAL_ERROR","Unknown shared-scan candidate predicate");
}
class SharedSpoolCursor final:public RowCursor {
    SharedScanState& state;std::unique_ptr<QuerySpillStore::Reader> reader;json condition;
public:
    SharedSpoolCursor(SharedScanState& current,const ScanSpec& spec):state(current),condition(spec.condition){
        if(state.preflight||!state.store||!state.run.id)fail("INTERNAL_ERROR","Shared spool opened before successful construction");
        reader=state.store->open(state.run);++state.reader_opens;
    }
    bool next(StoredRow& row) override {
        json values;while(reader->next(values)){query_work();++state.spool_rows_read;auto truth=shared_candidate_truth(condition,values);if(!truth.is_boolean()||!truth.get<bool>())continue;++state.matched_rows;row.id={};row.values=std::move(values);return true;}return false;
    }
};
