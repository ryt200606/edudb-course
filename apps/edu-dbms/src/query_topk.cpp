#include "query_topk.hpp"
#include <algorithm>
#include <limits>

namespace edudb::query {
namespace {
class TopK final:public ExecNode {
    struct Entry { json row,key;std::uint64_t sequence=0;std::size_t bytes=0; };
    std::unique_ptr<ExecNode> child,external;
    Key key;Compare compare;std::size_t k;
    ExecContext* context=nullptr;bool active=false;
    std::vector<Entry> heap;
    std::size_t used=0,position=0;
    std::uint64_t next_sequence=0;
    QuerySpillStore::Run unsorted;
    std::shared_ptr<QuerySpillStore::Reader> fallback_reader;
    json stats={{"node","BoundedTopK"},{"loops",0},{"actual_rows",0},
                {"input_rows",0},{"retained_peak",0},{"discarded_rows",0},
                {"comparisons",0},{"memory_bytes_estimate_peak",0},
                {"fallback_external_sort",false},{"spill_bytes",0},
                {"source_early_stop",false}};
    void work(){if(context->work)context->work();}
    void increment(const char* name,std::uint64_t value=1){stats[name]=stats.value(name,std::uint64_t(0))+value;}
    std::size_t bytes(const json& row)const{return context->row_bytes?context->row_bytes(row):row.dump().size()+128;}
    void charge(const json& row){if(context->materialize)context->materialize(row);}
    void peak(std::size_t value){stats["memory_bytes_estimate_peak"]=std::max(stats["memory_bytes_estimate_peak"].get<std::size_t>(),value);}
    bool less(const Entry& a,const Entry& b){
        work();increment("comparisons");const auto order=compare(a.key,b.key);
        return order?order<0:a.sequence<b.sequence;
    }
    Entry entry(json row,bool extract_key=true){
        if(next_sequence==std::numeric_limits<std::uint64_t>::max())throw DbError("QUERY_LIMIT_EXCEEDED","Top-K stable input sequence overflow");
        Entry value;value.row=std::move(row);if(extract_key)value.key=key(value.row);value.sequence=next_sequence++;
        const auto row_size=bytes(value.row),key_size=bytes(value.key);
        const auto maximum=std::numeric_limits<std::size_t>::max();
        if(key_size>maximum-sizeof(Entry)||row_size>maximum-key_size-sizeof(Entry))throw DbError("QUERY_OPERATOR_MEMORY","Top-K row allocation estimate overflow");
        value.bytes=row_size+key_size+sizeof(Entry);increment("input_rows");return value;
    }
    void write_wrapped(QuerySpillStore::Writer& writer,Entry& value){
        json wrapped=json::array();wrapped.push_back(std::move(value.row));wrapped.push_back(value.sequence);
        charge(wrapped);writer.append(wrapped);
    }
    void fallback(Entry* pending,const std::string& reason){
        if(!context->spill)throw DbError("QUERY_OPERATOR_MEMORY","Top-K external fallback requires a temporary store");
        stats["fallback_external_sort"]=true;stats["fallback_reason"]=reason;
        auto writer=context->spill->create();
        // Discarded rows were dominated by the retained k rows. Preserving the
        // retained rows plus unread input is enough for the first k results.
        // A native run releases the heap before external-sort buffering begins.
        for(auto& value:heap)write_wrapped(*writer,value);
        std::vector<Entry>().swap(heap);used=0;
        if(pending)write_wrapped(*writer,*pending);
        json row;
        while(child->next(row)){work();auto value=entry(std::move(row),false);write_wrapped(*writer,value);}
        child->close();unsorted=writer->finish();increment("spill_bytes",unsorted.bytes);
        auto source=query::scan([this](){
            fallback_reader=std::shared_ptr<QuerySpillStore::Reader>(context->spill->open(unsorted).release());
            return Pull([this](json& row){return fallback_reader->next(row);});
        },"RetainedCandidatesAndRemainingInput");
        auto stable_key=[this](const json& wrapped){return json::array({key(wrapped[0]),wrapped[1]});};
        auto stable_compare=[this](const json& a,const json& b){
            const int order=compare(a[0],b[0]);if(order)return order;
            const auto x=a[1].get<std::uint64_t>(),y=b[1].get<std::uint64_t>();return x<y?-1:x>y?1:0;
        };
        external=query::sort(std::move(source),stable_key,stable_compare);external->open(*context);
    }
public:
    static std::size_t candidate_capacity(std::size_t memory){return (memory/2)/(sizeof(Entry)+64);}
    TopK(std::unique_ptr<ExecNode> source,Key sort_key,Compare comparator,std::size_t count):child(std::move(source)),key(std::move(sort_key)),compare(std::move(comparator)),k(count){stats["k"]=k;}
    void open(ExecContext& execution)override{
        close();context=&execution;active=true;increment("loops");next_sequence=0;position=0;used=0;
        stats["fallback_external_sort"]=false;stats["retained_rows"]=0;stats.erase("fallback_reason");stats.erase("external_sort_actual");
        if(k==0){stats["child_opened"]=false;return;}
        if(context->memory_limit<4096)throw DbError("QUERY_OPERATOR_MEMORY","Top-K requires at least4096 estimated working bytes");
        child->open(execution);stats["child_opened"]=true;
        // Half remains available for the next row, extracted key, comparison
        // scratch and vector metadata. Estimates do not claim allocator RSS.
        const auto budget=context->memory_limit/2;
        if(k>candidate_capacity(context->memory_limit)){fallback(nullptr,"K exceeds a conservative minimum-entry memory bound");return;}
        json row;
        while(child->next(row)){
            work();const auto width=bytes(row);
            if(width>context->memory_limit/4)throw DbError("QUERY_OPERATOR_MEMORY","One sortable row exceeds bounded merge-head memory budget");
            if(width>context->memory_limit/8){auto wide=entry(std::move(row),false);fallback(&wide,"Actual row width needs external sort");return;}
            auto candidate=entry(std::move(row));
            if(candidate.bytes>context->memory_limit/2)throw DbError("QUERY_OPERATOR_MEMORY","Top-K row and extracted key exceed bounded scratch memory");
            peak(used+candidate.bytes);
            auto order=[this](const Entry& a,const Entry& b){return less(a,b);};
            if(heap.size()<k){
                if(candidate.bytes>budget-used){fallback(&candidate,"Actual retained heap exceeds memory budget");return;}
                charge(candidate.row);charge(candidate.key);used+=candidate.bytes;heap.push_back(std::move(candidate));std::push_heap(heap.begin(),heap.end(),order);
                stats["retained_peak"]=std::max(stats["retained_peak"].get<std::size_t>(),heap.size());
            }else if(less(candidate,heap.front())){
                const auto remaining=used-heap.front().bytes;
                if(candidate.bytes>budget-remaining){fallback(&candidate,"Replacement width exceeds memory budget");return;}
                charge(candidate.row);charge(candidate.key);std::pop_heap(heap.begin(),heap.end(),order);heap.pop_back();used=remaining+candidate.bytes;
                heap.push_back(std::move(candidate));std::push_heap(heap.begin(),heap.end(),order);increment("discarded_rows");
            }else increment("discarded_rows");
        }
        child->close();
        std::sort(heap.begin(),heap.end(),[this](const Entry& a,const Entry& b){return less(a,b);});
        stats["retained_rows"]=heap.size();
    }
    bool next(json& row)override{
        if(!active||position>=k)return false;
        work();
        if(external){json wrapped;if(!external->next(wrapped))return false;row=std::move(wrapped[0]);}
        else {if(position>=heap.size())return false;row=std::move(heap[position].row);}
        ++position;increment("actual_rows");return true;
    }
    void close()noexcept override{
        if(external){try{stats["external_sort_actual"]=external->diagnostics();}catch(...){}external->close();}
        external.reset();fallback_reader.reset();child->close();
        if(unsorted.id&&context&&context->spill){try{context->spill->erase(unsorted);}catch(...){}unsorted={};}
        std::vector<Entry>().swap(heap);used=0;position=0;active=false;
    }
    json diagnostics()const override{
        auto result=stats;result["children"]=json::array({child->diagnostics()});
        if(external)result["external_sort_actual"]=external->diagnostics();
        return result;
    }
    ~TopK(){close();}
};
}
std::size_t topk_maximum_candidates(std::size_t memory_limit){return TopK::candidate_capacity(memory_limit);}
std::unique_ptr<ExecNode> topk(std::unique_ptr<ExecNode> child,Key key,Compare compare,std::size_t k){return std::make_unique<TopK>(std::move(child),std::move(key),std::move(compare),k);}
}
