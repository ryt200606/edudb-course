#include "query_semijoin.hpp"
#include <array>
#include <algorithm>
#include <limits>
#include <unordered_set>

namespace edudb::query {
namespace {
using Run=QuerySpillStore::Run;
constexpr std::size_t partitions=16;
std::size_t partition(const std::string& value){std::uint64_t hash=1469598103934665603ull;for(unsigned char c:value){hash^=c;hash*=1099511628211ull;}return std::size_t(hash%partitions);}
bool null_key(const json& key){if(!key.is_array())throw DbError("INTERNAL_ERROR","Semi join key must be an array");for(const auto& value:key)if(value.is_null())return true;return false;}
class Semi final:public ExecNode {
    std::unique_ptr<ExecNode> outer,inner;SemiJoinSpec spec;bool anti;
    ExecContext* context=nullptr;bool active=false,built=false,spilled=false,oversized=false;
    std::unordered_set<std::string> keys;std::size_t used=0,budget=0;int loaded=-1;
    std::array<Run,partitions> runs{};
    std::array<std::unique_ptr<QuerySpillStore::Writer>,partitions> writers;
    std::array<std::string,partitions> last;
    json profile;
    void work(){if(context->work)context->work();}
    void inc(const char* name,std::uint64_t n=1){profile[name]=profile.value(name,std::uint64_t(0))+n;}
    std::size_t estimate(const std::string& key)const{return key.size()+sizeof(std::string)+128;}
    void peak(std::size_t value){profile["memory_bytes_estimate_peak"]=std::max(profile.value("memory_bytes_estimate_peak",std::size_t(0)),value);}
    std::string canonical(const json& key){
        const auto bytes=context->row_bytes?context->row_bytes(key):key.dump().size()+128;
        if(bytes>context->memory_limit/8){throw DbError("QUERY_OPERATOR_MEMORY","One semi-join key exceeds the reserved key-frame memory estimate");}
        return equality_key(key);
    }
    void append(const std::string& key){
        const auto slot=partition(key);if(!last[slot].empty()&&last[slot]==key){inc("adjacent_partition_duplicates_suppressed");return;}
        if(key.size()<=context->memory_limit/(partitions*32))last[slot]=key;else last[slot].clear();
        if(!writers[slot]){writers[slot]=context->spill->create();}
        writers[slot]->append(key);if(context->materialize)context->materialize(json::array({key,std::string(128,' ')}));inc("partition_keys_written");
    }
    void spill_prefix(){
        if(!context->spill)throw DbError("QUERY_OPERATOR_MEMORY","Semi hash overflow needs a query spill store");
        spilled=true;profile["spilled"]=true;for(const auto& key:keys)append(key);keys.clear();keys.rehash(0);used=0;
    }
    void build(){
        inner->open(*context);inc("inner_loops");json row;
        while(inner->next(row)){work();inc("inner_rows");const auto key=spec.inner_key(row);if(null_key(key)){inc("inner_null_keys");continue;}auto value=canonical(key);
            if(spilled){append(value);continue;}
            if(keys.count(value)){inc("duplicate_keys_suppressed");continue;}
            const auto bytes=estimate(value);if(bytes>budget-used){spill_prefix();append(value);continue;}if(context->materialize)context->materialize(json::array({value,std::string(128,' ')}));used+=bytes;keys.insert(std::move(value));peak(used);
        }
        inner->close();if(spilled)for(std::size_t n=0;n<partitions;++n)if(writers[n]){runs[n]=writers[n]->finish();writers[n].reset();profile["spill_bytes"]=profile.value("spill_bytes",std::uint64_t(0))+runs[n].bytes;}
        for(auto& recent:last){recent.clear();}
        built=true;profile["built"]=true;profile["in_memory_distinct_keys"]=keys.size();if(spec.after_build)spec.after_build();
    }
    void load(std::size_t slot){
        keys.clear();keys.rehash(0);used=0;loaded=int(slot);oversized=false;inc("partition_loads");if(!runs[slot].id)return;
        auto reader=context->spill->open(runs[slot]);json value;
        while(reader->next(value)){work();inc("partition_keys_read");if(!value.is_string())throw DbError("QUERY_SPILL_CORRUPT","Semi partition contains a non-string key");auto key=value.get<std::string>();if(keys.count(key))continue;const auto bytes=estimate(key);
            if(bytes>budget-used){keys.clear();keys.rehash(0);used=0;oversized=true;inc("oversized_partition_fallbacks");return;}if(context->materialize)context->materialize(json::array({key,std::string(128,' ')}));used+=bytes;keys.insert(std::move(key));peak(used);
        }
    }
    bool contains(const std::string& key){
        if(!spilled){return keys.count(key)!=0;}
        const auto slot=partition(key);if(loaded!=int(slot))load(slot);if(!oversized)return keys.count(key)!=0;
        // Explicit bounded fallback for a highly skewed partition: preserve
        // outer order and LIMIT semantics, trading additional temporary reads.
        inc("skew_partition_probes");auto reader=context->spill->open(runs[slot]);json value;while(reader->next(value)){work();inc("partition_keys_read");if(!value.is_string())throw DbError("QUERY_SPILL_CORRUPT","Semi partition contains a non-string key");if(value.get_ref<const std::string&>()==key)return true;}return false;
    }
public:
    Semi(std::unique_ptr<ExecNode> left,std::unique_ptr<ExecNode> right,SemiJoinSpec specification,bool negative):outer(std::move(left)),inner(std::move(right)),spec(std::move(specification)),anti(negative),profile{{"node",negative?"HashAntiJoin":"HashSemiJoin"},{"loops",0},{"actual_rows",0},{"inner_loops",0},{"inner_rows",0},{"outer_rows",0},{"spilled",false},{"spill_bytes",0},{"memory_bytes_estimate_peak",0},{"memory_scope","bounded hash/key estimates, not process RSS"}}{}
    void open(ExecContext& c)override{
        close();context=&c;if(c.memory_limit<65536)throw DbError("QUERY_OPERATOR_MEMORY","Semi join requires at least64KiB configured working memory");budget=c.memory_limit/3;active=true;profile["loops"]=profile["loops"].get<std::uint64_t>()+1;profile["memory_limit"]=c.memory_limit;profile["hash_budget"]=budget;outer->open(c);
    }
    bool next(json& row)override{
        if(!active)return false;
        while(outer->next(row)){work();inc("outer_rows");if(spec.outer_filter&&!spec.outer_filter(row)){inc("outer_rows_filtered");continue;}const auto key=spec.outer_key(row);bool match=false;
            if(null_key(key)){inc("outer_null_keys");}
            else{if(!built)build();match=contains(canonical(key));}
            if(match!=anti){inc("actual_rows");return true;}
        }
        return false;
    }
    void close()noexcept override{
        outer->close();inner->close();for(auto& writer:writers)writer.reset();for(auto& run:runs){if(run.id&&context&&context->spill){try{context->spill->erase(run);}catch(...){}}run={};}
        std::unordered_set<std::string> empty;keys.swap(empty);for(auto& recent:last){recent.clear();}used=0;loaded=-1;built=spilled=oversized=active=false;
    }
    json diagnostics()const override{auto result=profile;result["children"]=json::array({outer->diagnostics(),inner->diagnostics()});return result;}
    ~Semi(){close();}
};
}
std::unique_ptr<ExecNode> semi_join(std::unique_ptr<ExecNode> outer,std::unique_ptr<ExecNode> inner,SemiJoinSpec spec,bool anti){if(!outer||!inner||!spec.outer_key||!spec.inner_key)throw DbError("INTERNAL_ERROR","Semi join lacks a child or key binding");return std::make_unique<Semi>(std::move(outer),std::move(inner),std::move(spec),anti);}
} // namespace edudb::query
