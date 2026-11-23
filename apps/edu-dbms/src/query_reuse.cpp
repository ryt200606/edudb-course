#include "query_reuse.hpp"
#include <algorithm>
namespace edudb::query {
namespace {
class AliasCopy final:public ExecNode {
    std::unique_ptr<ExecNode> source;AliasCopySpec specification;std::vector<bool> needed;
    ExecContext* context=nullptr;bool active=false;
    json stats={{"node","SelfJoinAliasCopy"},{"loops",0},{"actual_rows",0},{"copied_values",0},{"moved_values",0},{"pairs_examined",0},{"memory_bytes_estimate_peak",0}};
    void work(){if(context->work)context->work();}
public:
    AliasCopy(std::unique_ptr<ExecNode> child,AliasCopySpec spec):source(std::move(child)),specification(std::move(spec)){
        const auto& s=specification;
        if(!source||s.total_width>65536||!s.width||s.from>s.total_width||s.to>s.total_width||s.width>s.total_width-s.from||s.width>s.total_width-s.to||!(s.from+s.width<=s.to||s.to+s.width<=s.from))throw DbError("INVALID_OPERATOR","Alias-copy needs a child and distinct ranges inside at most65536 schema slots");
        needed.assign(s.total_width,false);
        for(int slot:s.required_slots){if(slot<0||std::size_t(slot)>=s.total_width)throw DbError("INVALID_OPERATOR","Alias-copy required slot is out of range");needed[slot]=true;}
    }
    void open(ExecContext& ctx)override{close();context=&ctx;active=true;stats["loops"]=stats["loops"].get<std::uint64_t>()+1;source->open(ctx);}
    bool next(json& row)override{
        if(!active||!source->next(row))return false;
        if(!row.is_array()||row.size()!=specification.total_width)throw DbError("INTERNAL_ERROR","Alias-copy input has an unexpected schema width");
        work();
        for(std::size_t col=0;col<specification.width;++col){
            const auto from=specification.from+col,to=specification.to+col;work();
            if(needed[to]){
                if(needed[from]){row[to]=row[from];stats["copied_values"]=stats["copied_values"].get<std::uint64_t>()+1;}
                else{row[to]=std::move(row[from]);stats["moved_values"]=stats["moved_values"].get<std::uint64_t>()+1;}
            }
            if(!needed[from])row[from]=nullptr;
        }
        const auto bytes=context->row_bytes?context->row_bytes(row):row.dump().size()+128;
        if(bytes>context->memory_limit/2)throw DbError("QUERY_OPERATOR_MEMORY","One alias-copy row exceeds bounded operator scratch estimate");
        stats["memory_bytes_estimate_peak"]=std::max(stats["memory_bytes_estimate_peak"].get<std::size_t>(),bytes);
        stats["actual_rows"]=stats["actual_rows"].get<std::uint64_t>()+1;return true;
    }
    void close()noexcept override{source->close();active=false;}
    json diagnostics()const override{auto result=stats;result["source_operators"]=1;result["children"]=json::array({source->diagnostics()});return result;}
    ~AliasCopy(){close();}
};
}
std::unique_ptr<ExecNode> alias_copy(std::unique_ptr<ExecNode> source,AliasCopySpec spec){return std::make_unique<AliasCopy>(std::move(source),std::move(spec));}
}
