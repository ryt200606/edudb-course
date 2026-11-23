#include "page_version_graph.hpp"
#include <algorithm>
#include <functional>
#include <stdexcept>
#include <tuple>

namespace edudb {
bool PageVersionGraph::Edge::operator<(const Edge& b)const{return std::tie(kind,page,source_lsn,target_lsn)<std::tie(b.kind,b.page,b.source_lsn,b.target_lsn);}
PageVersionGraph::PageVersionGraph(Limits limits):limits_(limits){
    if(!limits.transactions||!limits.versions||!limits.readers||!limits.edges)throw std::invalid_argument("Version graph limits must be positive");
}
void PageVersionGraph::failed()noexcept{truncated_=true;disabled_=true;}
void PageVersionGraph::erase_node(Id id){
    nodes_.erase(id);
    for(auto& v:versions_)if(v.second.readers.erase(id))--readers_;
    for(auto i=edges_.begin();i!=edges_.end();)if(i->first.first==id||i->first.second==id){edge_count_-=i->second.size();i=edges_.erase(i);}else ++i;
}
void PageVersionGraph::begin(Id tx,Id csn)noexcept{
    try{std::lock_guard<std::mutex> lock(mutex_);if(disabled_)return;
        if(!tx||nodes_.count(tx)){++invalid_events_;failed();return;}
        if(nodes_.size()>=limits_.transactions){
            auto old=std::find_if(nodes_.begin(),nodes_.end(),[](const auto& item){return item.second.outcome=="COMMITTED"||item.second.outcome=="ABORTED";});
            if(old==nodes_.end()){failed();return;}erase_node(old->first);++pruned_transactions_;truncated_=true;
        }
        nodes_.emplace(tx,Node{csn,0,"ACTIVE"});
    }catch(...){failed();}
}
PageVersionGraph::Version& PageVersionGraph::version(Id page,Id lsn,std::uint16_t type){
    auto key=VersionKey{page,lsn};auto found=versions_.find(key);if(found!=versions_.end())return found->second;
    if(versions_.size()>=limits_.versions)throw std::length_error("version observation budget");
    auto position=versions_.emplace(key,Version{}).first;position->second.type=type;return position->second;
}
void PageVersionGraph::edge(Id from,Id to,const char* kind,Id page,Id from_lsn,Id to_lsn){
    if(!from||!to||from==to)return;
    if(!nodes_.count(from)||!nodes_.count(to)){truncated_=true;return;}
    if(edge_count_>=limits_.edges)throw std::length_error("dependency edge budget");
    auto& items=edges_[{from,to}];if(items.insert(Edge{kind,page,from_lsn,to_lsn}).second)++edge_count_;
}
void PageVersionGraph::read(Id tx,Id page,Id lsn,std::uint16_t type)noexcept{
    try{std::lock_guard<std::mutex> lock(mutex_);if(disabled_)return;
        auto node=nodes_.find(tx);if(node==nodes_.end()||node->second.outcome!="ACTIVE"){++invalid_events_;failed();return;}
        auto& visible=version(page,lsn,type);++read_events_;
        if(visible.creator==tx)return; // Never infer an inter-transaction edge from RYW.
        if(visible.creator_known)edge(visible.creator,tx,"WR",page,lsn,lsn);
        if(!visible.readers.count(tx)){if(readers_>=limits_.readers)throw std::length_error("reader budget");visible.readers.insert(tx);++readers_;}
        // A fixed snapshot can first read an old page AFTER a newer version has
        // committed. Register its anti-dependency on every retained successor.
        auto at=versions_.lower_bound({page,0});
        for(;at!=versions_.end()&&at->first.first==page;++at)
            if(at->second.creator_known&&at->second.csn>visible.csn)edge(tx,at->second.creator,"RW",page,lsn,at->first.second);
    }catch(...){failed();}
}
void PageVersionGraph::publish(Id tx,Id csn,const std::vector<Write>& pages)noexcept{
    try{std::lock_guard<std::mutex> lock(mutex_);if(disabled_)return;
        auto node=nodes_.find(tx);if(node==nodes_.end()||node->second.outcome!="ACTIVE"||!csn){++invalid_events_;failed();return;}
        for(const auto& page:pages){
            if(page.new_lsn<=page.previous_lsn){++invalid_events_;failed();return;}
            auto& previous=version(page.page,page.previous_lsn,page.page_type);
            if(previous.creator_known)edge(previous.creator,tx,"WW",page.page,page.previous_lsn,page.new_lsn);
            // Readers of ALL old retained versions conflict with this writer,
            // including readers of a version older than the immediate parent.
            auto at=versions_.lower_bound({page.page,0});
            for(;at!=versions_.end()&&at->first.first==page.page;++at)
                if(at->second.csn<csn)for(auto reader:at->second.readers)edge(reader,tx,"RW",page.page,at->first.second,page.new_lsn);
            auto& next=version(page.page,page.new_lsn,page.page_type);
            if(next.creator_known||!next.readers.empty()){++invalid_events_;failed();return;}
            next.csn=csn;next.creator=tx;next.creator_known=true;next.type=page.page_type;++published_pages_;
        }
        node->second.commit=csn;
    }catch(...){failed();}
}
void PageVersionGraph::finish(Id tx,const std::string& outcome)noexcept{
    try{std::lock_guard<std::mutex> lock(mutex_);if(disabled_)return;auto node=nodes_.find(tx);if(node==nodes_.end())return;
        if(outcome=="ABORTED"){erase_node(tx);return;}
        if(outcome=="COMMITTED"){node->second.outcome=outcome;return;}
        // An I/O failure after WAL force is not a proven rollback. Keep the
        // uncertain node; every definite conclusion is disabled until restart.
        node->second.outcome="UNKNOWN";truncated_=true;
    }catch(...){failed();}
}
void PageVersionGraph::restore_alias(Id page,Id base_lsn,Id restored_lsn,std::uint16_t type)noexcept{
    try{std::lock_guard<std::mutex> lock(mutex_);if(disabled_||base_lsn==restored_lsn)return;
        if(restored_lsn<base_lsn||versions_.count({page,restored_lsn})){++invalid_events_;failed();return;}
        auto& base=version(page,base_lsn,type);
        if(base.readers.size()>limits_.readers-readers_)throw std::length_error("restored reader budget");
        auto restored=base;restored.type=type;restored.restored_from=base_lsn;restored.system_restored=true;
        auto& alias=version(page,restored_lsn,type);alias=std::move(restored);readers_+=alias.readers.size();
    }catch(...){failed();}
}
void PageVersionGraph::reset_epoch(const std::string& reason)noexcept{
    try{std::lock_guard<std::mutex> lock(mutex_);nodes_.clear();versions_.clear();edges_.clear();readers_=edge_count_=0;read_events_=published_pages_=pruned_transactions_=invalid_events_=0;epoch_reset_reason_=reason;++epoch_resets_;truncated_=false;disabled_=false;}catch(...){failed();}
}
PageVersionGraph::json PageVersionGraph::diagnostics()const{
    std::lock_guard<std::mutex> lock(mutex_);json nodes=json::array(),edges=json::array(),versions=json::array();
    for(const auto& item:nodes_)if(nodes.size()<512)nodes.push_back({{"tx",item.first},{"start_csn",item.second.start},{"commit_csn",item.second.commit?json(item.second.commit):json(nullptr)},{"outcome",item.second.outcome}});
    for(const auto& item:edges_)for(const auto& reason:item.second)if(edges.size()<2048)edges.push_back({{"from",item.first.first},{"to",item.first.second},{"kind",reason.kind},{"page",reason.page},{"source_page_lsn",reason.source_lsn},{"target_page_lsn",reason.target_lsn}});
    bool reader_lists_truncated=false;std::size_t unknown=0;for(const auto& item:versions_){if(!item.second.creator_known)++unknown;if(versions.size()<512){json readers=json::array();for(auto reader:item.second.readers)if(readers.size()<64)readers.push_back(reader);reader_lists_truncated=reader_lists_truncated||readers.size()<item.second.readers.size();versions.push_back({{"page",item.first.first},{"page_lsn",item.first.second},{"csn",item.second.csn},{"creator",item.second.creator_known?json(item.second.creator):json(nullptr)},{"creator_known",item.second.creator_known},{"page_type",item.second.type},{"readers",readers},{"reader_count",item.second.readers.size()},{"system_restored",item.second.system_restored},{"restored_from_page_lsn",item.second.system_restored?json(item.second.restored_from):json(nullptr)}});}}
    std::map<Id,std::vector<Id>> adjacency;for(const auto& item:edges_){auto a=nodes_.find(item.first.first),b=nodes_.find(item.first.second);if(a!=nodes_.end()&&b!=nodes_.end()&&a->second.outcome=="COMMITTED"&&b->second.outcome=="COMMITTED")adjacency[a->first].push_back(b->first);}
    // Iterative DFS avoids recursion proportional to the transaction budget.
    std::map<Id,int> color;std::vector<Id> cycle;
    for(const auto& start:adjacency){if(color[start.first])continue;std::vector<std::pair<Id,std::size_t>> stack{{start.first,0}};color[start.first]=1;
        while(!stack.empty()&&cycle.empty()){
            auto& top=stack.back();auto& next=adjacency[top.first];
            if(top.second==next.size()){color[top.first]=2;stack.pop_back();continue;}
            auto child=next[top.second++];if(color[child]==1){auto at=std::find_if(stack.begin(),stack.end(),[&](const auto& frame){return frame.first==child;});for(;at!=stack.end();++at)cycle.push_back(at->first);cycle.push_back(child);break;}
            if(!color[child]){color[child]=1;stack.push_back({child,0});}
        }
        if(!cycle.empty())break;
    }
    return {{"scope","actual committed page versions; current process epoch only"},{"level","PAGE_CONFLICT"},{"epoch_resets",epoch_resets_},{"last_epoch_reset_reason",epoch_reset_reason_},{"nodes",nodes},{"edges",edges},{"versions",versions},{"committed_cycle",cycle},{"truncated",truncated_.load()},{"disabled",disabled_.load()},{"is_serializability_proof",false},{"inspection_truncated",reader_lists_truncated||nodes.size()<nodes_.size()||edges.size()<edge_count_||versions.size()<versions_.size()},{"counts",{{"transactions",nodes_.size()},{"versions",versions_.size()},{"readers",readers_},{"edges",edge_count_}}},{"unknown_initial_versions",unknown},{"unknown_creator_versions",unknown},{"read_events",read_events_},{"published_pages",published_pages_},{"pruned_transactions",pruned_transactions_},{"invalid_events",invalid_events_},{"limits",{{"transactions",limits_.transactions},{"versions",limits_.versions},{"readers",limits_.readers},{"edges",limits_.edges}}},{"interpretation","Observation only. Page-level conflicts include metadata/index layout. No predicate/row serializability proof; unknown initial creators, UNKNOWN outcomes and pruning are explicit."}};
}
}
