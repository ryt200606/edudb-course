#include "mvto_control.hpp"
#include "edudb.hpp"
#include <algorithm>
namespace edudb {
std::size_t PageMvto::slots()const{return version_count_;}
void PageMvto::reset(){pages_.clear();version_count_=reserved_slots_=0;reads_=published_=0;}
void PageMvto::set_limit(std::size_t limit){if(!limit||limit<slots()+reserved_slots_)throw DbError("MVTO_METADATA_LIMIT","Timestamp slot bound cannot be below live/reserved version count");limit_=limit;}
const PageMvto::Version& PageMvto::version(std::uint64_t page,std::uint64_t lsn)const{
    auto i=pages_.find(page);if(i!=pages_.end())for(const auto& v:i->second.versions)if(v.lsn==lsn)return v;
    throw DbError("MVTO_VERSION_MISSING","Timestamp metadata for committed page version is unavailable");
}
void PageMvto::ensure_current(std::uint64_t page,std::uint64_t lsn){
    auto found=pages_.find(page);if(found!=pages_.end()&&found->second.current_known){
        if(found->second.current!=lsn)throw DbError("MVTO_VERSION_MISSING","Physical latest page changed without MVTO publication");return;
    }
    if(slots()+reserved_slots_>=limit_)throw DbError("MVTO_METADATA_LIMIT","Version timestamp metadata slot limit reached before page read");
    auto& p=pages_[page];p.versions.push_back({lsn,0,0});++version_count_;p.current=lsn;p.current_known=true;
}
std::optional<std::uint64_t> PageMvto::select(std::uint64_t page,const std::vector<std::uint64_t>& available,std::uint64_t ts)const{
    std::optional<std::uint64_t> result;std::uint64_t best=0;
    for(auto lsn:available){const auto& v=version(page,lsn);if(v.wts<=ts&&(!result||v.wts>best||(v.wts==best&&lsn>*result))){result=lsn;best=v.wts;}}
    return result;
}
void PageMvto::read(std::uint64_t page,std::uint64_t lsn,std::uint64_t ts){
    (void)version(page,lsn);for(auto& v:pages_.at(page).versions)if(v.lsn==lsn){if(v.wts>ts)throw DbError("MVTO_VERSION_NOT_VISIBLE","Version WTS exceeds reader timestamp");v.rts=std::max(v.rts,ts);++reads_;return;}
}
void PageMvto::validate_write(std::uint64_t page,std::uint64_t base,std::uint64_t current,std::uint64_t ts)const{
    const auto& v=version(page,base);
    if(v.wts>ts)throw DbError("MVTO_WRITE_CONFLICT","Writer cannot modify a version newer than its timestamp");
    if(v.rts>ts)throw DbError("MVTO_RTS_CONFLICT","A younger transaction read the physical base version; old writer rejected before WAL");
    if(base!=current)throw DbError("MVTO_WRITE_CONFLICT","Physical base changed; conservative rejection prevents overwriting newer whole-page versions");
}
void PageMvto::prepare_publish(const std::vector<std::uint64_t>& pages,bool no_other_readers){
    if(reserved_slots_)throw DbError("INTERNAL_ERROR","Previous MVTO publication reservation is still active");
    std::size_t extra=0;for(auto id:pages){auto i=pages_.find(id);if(!no_other_readers||i==pages_.end()||!i->second.current_known)++extra;}
    auto used=slots();if(extra>limit_-std::min(limit_,used))throw DbError("MVTO_METADATA_LIMIT","Publication timestamp slots must be reserved before WAL");
    for(auto id:pages){auto& p=pages_[id];p.replace_on_publish=no_other_readers&&p.current_known;if(!p.replace_on_publish){p.pending.push_back({});++reserved_slots_;}}
}
void PageMvto::publish(std::uint64_t page,std::uint64_t lsn,std::uint64_t ts){
    auto& p=pages_.at(page);if(!p.replace_on_publish&&p.pending.empty())throw DbError("INTERNAL_ERROR","MVTO publication lacks pre-WAL list-node reservation");
    if(p.current_known&&version(page,p.current).wts>=ts)throw DbError("INTERNAL_ERROR","MVTO page WTS must increase on physical publication");
    if(p.replace_on_publish){
        auto current=std::find_if(p.versions.begin(),p.versions.end(),[&](const Version& v){return v.lsn==p.current;});if(current==p.versions.end())throw DbError("INTERNAL_ERROR","MVTO reusable current stamp missing");
        *current={lsn,ts,ts};for(auto i=p.versions.begin();i!=p.versions.end();)if(i!=current){i=p.versions.erase(i);--version_count_;}else ++i;p.replace_on_publish=false;
    }else{auto pending=p.pending.begin();*pending={lsn,ts,ts};p.versions.splice(p.versions.end(),p.pending,pending);--reserved_slots_;++version_count_;}
    p.current=lsn;p.current_known=true;++published_;
}
void PageMvto::collect(const std::map<std::uint64_t,std::set<std::uint64_t>>& keep){
    for(auto i=pages_.begin();i!=pages_.end();){auto& p=i->second;reserved_slots_-=p.pending.size();p.pending.clear();auto retained=keep.find(i->first);for(auto v=p.versions.begin();v!=p.versions.end();)if((!p.current_known||v->lsn!=p.current)&&(retained==keep.end()||!retained->second.count(v->lsn))){v=p.versions.erase(v);--version_count_;}else ++v;if(p.versions.empty())i=pages_.erase(i);else ++i;}
}
nlohmann::json PageMvto::diagnostics()const{
    nlohmann::json versions=nlohmann::json::array();bool truncated=false;
    for(const auto& p:pages_)for(const auto& v:p.second.versions){if(versions.size()>=4096){truncated=true;continue;}versions.push_back({{"page",p.first},{"lsn",v.lsn},{"wts",v.wts},{"rts",v.rts},{"current",p.second.current_known&&p.second.current==v.lsn}});}
    return {{"versions",versions},{"metadata_slots",slots()},{"reserved_metadata_slots",reserved_slots_},{"metadata_slot_limit",limit_},{"read_observations",reads_},{"published_versions",published_},{"diagnostic_truncated",truncated},{"scope","actual per-physical-page-version WTS/RTS; current-base FCW may conservatively reject standard MVTO historical insertion; metadata resets only when no version readers remain"}};
}
}
