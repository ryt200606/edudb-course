#include "cluster_heap.hpp"
#include <algorithm>
#include <cstring>
#include <limits>
#include <set>
namespace edudb::cluster {
namespace {
[[noreturn]]void bad(const char* m){throw Error("CORRUPT_CLUSTER",m);}
void put(Page& p,std::size_t at,std::uint64_t n,unsigned width){for(unsigned i=0;i<width;++i)p[at+i]=static_cast<unsigned char>(n>>(8*i));}
std::uint64_t readle(const Page& p,std::size_t at,unsigned width){std::uint64_t n=0;for(unsigned i=0;i<width;++i)n|=std::uint64_t(p[at+i])<<(8*i);return n;}
std::uint32_t crc(const Page& p){std::uint32_t c=~0u;for(auto v:p){c^=v;for(unsigned j=0;j<8;++j)c=(c>>1)^((c&1)?0xedb88320u:0);}return ~c;}
void seal(Page& p){put(p,60,0,4);put(p,60,crc(p),4);}
Page empty(Id id,Id owner){Page p{};put(p,0,0x45445047,4);put(p,4,3,2);put(p,6,4,2);put(p,8,id,8);put(p,34,64,2);put(p,36,8192,2);put(p,40,owner,8);std::memcpy(p.data()+56,"CLST",4);seal(p);return p;}
std::uint16_t length(const Page& p,std::size_t slot){if(slot>=readle(p,32,2))bad("Slot outside directory");return static_cast<std::uint16_t>(readle(p,64+8*slot+2,2));}
std::uint32_t generation(const Page& p,std::size_t slot){auto n=readle(p,64+8*slot+4,4);if(!n)bad("Zero slot generation");return static_cast<std::uint32_t>(n);}
std::string record_bytes(const Page& p,std::size_t slot){auto n=length(p,slot);auto at=readle(p,64+8*slot,2);if(at<readle(p,36,2)||at+n>8192)bad("Slot record outside payload");return std::string(reinterpret_cast<const char*>(p.data()+at),n);}
Id table_tag(const std::string& bytes){if(bytes.size()<8)bad("Cluster slot misses tableOID tag");Id n=0;for(unsigned i=0;i<8;++i)n|=Id(static_cast<unsigned char>(bytes[i]))<<(8*i);if(!n)bad("Zero tableOID tag");return n;}
std::string tagged(Id table,const std::string& record){if(record.size()>record::maximum_record_bytes-8)throw Error("ROW_TOO_LARGE","Cluster tableOID prefix plus record cannot fit one heap slot");std::string b(8,0);for(unsigned i=0;i<8;++i)b[i]=static_cast<char>(table>>(8*i));return b+record;}
bool repack(Page& p,std::size_t target,const std::string& bytes,bool append){
    auto count=readle(p,32,2);auto n=count+(append?1:0);std::vector<std::string> values;std::size_t used=64+8*n;
    for(std::size_t i=0;i<n;++i){auto value=i==target?bytes:(i<count&&length(p,i)?record_bytes(p,i):std::string());used+=value.size();values.push_back(std::move(value));}if(used>8192)return false;
    std::size_t end=8192;for(std::size_t i=0;i<n;++i){end-=values[i].size();std::copy(values[i].begin(),values[i].end(),p.begin()+end);put(p,64+8*i,end,2);put(p,64+8*i+2,values[i].size(),2);}put(p,32,n,2);put(p,34,64+8*n,2);put(p,36,end,2);return true;
}
std::optional<Rid> append(Page& p,Id table,Id lid,const std::string& bytes){std::size_t count=readle(p,32,2),target=count;std::uint32_t gen=1;
    for(std::size_t i=0;i<count;++i)if(!length(p,i)&&generation(p,i)<std::numeric_limits<std::uint32_t>::max()){target=i;gen=generation(p,i)+1;break;}
    if(!repack(p,target,bytes,target==count))return {};
    put(p,64+8*target+4,gen,4);return Rid{readle(p,8,8),static_cast<std::uint16_t>(target),gen,table,lid};
}
}
Heap::Heap(Pager pager,json& descriptor,Id oid):pager_(std::move(pager)),descriptor_(descriptor),oid_(oid){
    if(!oid||oid>=(Id(1)<<48)||!pager_.read||!pager_.write||!pager_.allocate||!pager_.locate||!pager_.map||!pager_.unmap)throw Error("CLUSTER_CONFIG","Valid owner and all transactional pager/map callbacks are required");
    if(descriptor_.is_null()||descriptor_.empty())descriptor_={{"cluster_oid",oid_},{"members",json::object()},{"groups",json::object()}};
    if(descriptor_.at("cluster_oid")!=oid_||!descriptor_.at("members").is_object()||!descriptor_.at("groups").is_object())bad("Invalid cluster descriptor identity");
    for(auto i=descriptor_["members"].begin();i!=descriptor_["members"].end();++i){auto id=std::stoull(i.key());auto& m=i.value();auto definition=record::schema(m.at("definition"));auto columns=m.at("columns").get<std::vector<unsigned>>();std::set<unsigned> seen;json types=json::array();
        if(!id||id==oid_||id>=(Id(1)<<48)||std::to_string(id)!=i.key()||columns.empty())bad("Invalid persisted member identity/key");
        auto shape=record::layout(definition);if(definition.format==record::Format::Fixed&&shape["fixed_record_bytes"].get<std::size_t>()>record::maximum_record_bytes-8)bad("Persisted fixed member cannot fit tableOID plus record");
        for(auto column:columns){if(column>=definition.columns.size()||!seen.insert(column).second||definition.columns[column].type=="BLOB")bad("Invalid persisted clustering key columns");types.push_back(definition.columns[column].type);}
        if(!descriptor_.contains("key_types")||descriptor_["key_types"]!=types)bad("Persisted member key types differ");
        members_.emplace(id,Member{std::move(definition),columns});}
    for(auto i=descriptor_["groups"].begin();i!=descriptor_["groups"].end();++i)for(auto id:i.value().at("pages"))if(!page_groups_.emplace(id.get<Id>(),i.key()).second)bad("Page belongs to multiple cluster groups");
}
Heap::Member& Heap::member(Id table){auto i=members_.find(table);if(i==members_.end())throw Error("CLUSTER_MEMBER","Table is not a cluster member");return i->second;}
void Heap::attach(Id table,const json& definition,const std::vector<unsigned>& columns){
    if(!table||table>=(Id(1)<<48)||table==oid_||members_.count(table)||columns.empty())throw Error("CLUSTER_MEMBER","Table identity must be new and clustering key nonempty");
    auto schema=record::schema(definition);auto shape=record::layout(schema);if(schema.format==record::Format::Fixed&&shape["fixed_record_bytes"].get<std::size_t>()>record::maximum_record_bytes-8)throw Error("ROW_TOO_LARGE","Fixed cluster row plus tableOID cannot fit a heap slot");
    std::set<unsigned> used;json types=json::array();for(auto col:columns){if(col>=schema.columns.size()||!used.insert(col).second||schema.columns[col].type=="BLOB")throw Error("CLUSTER_KEY","Invalid/duplicate/BLOB cluster key column");types.push_back(schema.columns[col].type);}
    if(descriptor_.contains("key_types")&&descriptor_["key_types"]!=types)throw Error("CLUSTER_KEY","Member clustering key types must match exactly");
    descriptor_["key_types"]=types;descriptor_["members"][std::to_string(table)]={{"definition",definition},{"columns",columns}};members_.emplace(table,Member{std::move(schema),columns});
}
void Heap::detach_empty(Id table){if(!scan(table).empty())throw Error("CLUSTER_NOT_EMPTY","Delete a member's own rows before detaching it");members_.erase(table);descriptor_["members"].erase(std::to_string(table));}
std::string Heap::key(Member& m,const json& row){
    if(!row.is_array()||row.size()!=m.schema.columns.size())throw Error("INVALID_ROW","Cluster row width differs");
    json k=json::array();for(auto column:m.columns){const auto& value=row[column];const auto& type=m.schema.columns[column].type;
        if(value.is_null())k.push_back(nullptr);else if(type=="REAL"){double n=value.get<double>();k.push_back(n==0?0.0:n);}else if(type=="INTEGER")k.push_back(value.get<std::int64_t>());else k.push_back(value);
    }auto text=k.dump();if(text.size()>2048)throw Error("CLUSTER_KEY","Clustering key exceeds2048 serialized bytes");return text;
}
Page Heap::read(Id id){if(!page_groups_.count(id))bad("Row points outside this cluster");auto p=pager_.read(id);auto checksum=readle(p,60,4);put(p,60,0,4);auto count=readle(p,32,2),lower=readle(p,34,2),upper=readle(p,36,2);
    if(readle(p,0,4)!=0x45445047||readle(p,4,2)!=3||readle(p,6,2)!=4||readle(p,8,8)!=id||readle(p,40,8)!=oid_||std::memcmp(p.data()+56,"CLST",4)||count>(8192-64)/8||lower!=64+8*count||lower>upper||upper>8192||crc(p)!=checksum)bad("Invalid cluster page owner/layout/CRC");
    if(descriptor_["groups"].at(page_groups_.at(id)).at("free").at(std::to_string(id))!=upper-lower)bad("Cluster free-space map differs from actual page");
    put(p,60,checksum,4);return p;
}
void Heap::write(Page& p,json& group){seal(p);pager_.write(readle(p,8,8),p);group["free"][std::to_string(readle(p,8,8))]=readle(p,36,2)-readle(p,34,2);}
Rid Heap::insert(Id table,Id lid,const json& row){auto& m=member(table);if(pager_.locate(table,lid))throw Error("DUPLICATE_LID","Logical id already exists in this table");auto data=tagged(table,record::encode(m.schema,lid,row));auto group_key=key(m,row);
    if(!descriptor_["groups"].contains(group_key)){if(descriptor_["groups"].size()>=1024)throw Error("CLUSTER_LIMIT","Prototype group directory limit is1024");descriptor_["groups"][group_key]={{"key",json::parse(group_key)},{"pages",json::array()},{"free",json::object()}};}
    auto& group=descriptor_["groups"][group_key];
    for(auto id_value:group["pages"]){auto id=id_value.get<Id>();if(group["free"].at(std::to_string(id)).get<std::size_t>()<data.size())continue;auto p=read(id);auto rid=append(p,table,lid,data);if(rid){write(p,group);pager_.map(*rid);return *rid;}}
    auto id=pager_.allocate();if(!id||page_groups_.count(id))bad("Allocator reused a live cluster page");auto p=empty(id,oid_);auto rid=append(p,table,lid,data);if(!rid)throw Error("ROW_TOO_LARGE","Tagged row cannot fit empty shared page");
    if(!group["pages"].empty()){auto previous=group["pages"].back().get<Id>();auto before=read(previous);put(before,24,id,8);write(before,group);put(p,48,previous,8);}
    page_groups_[id]=group_key;group["pages"].push_back(id);write(p,group);pager_.map(*rid);return *rid;
}
Row Heap::get(Id table,Id lid){auto& m=member(table);auto rid=pager_.locate(table,lid);if(!rid||rid->table!=table||rid->lid!=lid)throw Error("STALE_ROW_ID","Table-scoped logical identity missing");auto p=read(rid->page);if(rid->slot>=readle(p,32,2)||generation(p,rid->slot)!=rid->generation||!length(p,rid->slot))throw Error("STALE_ROW_ID","Deleted/reused cluster slot");auto bytes=record_bytes(p,rid->slot);if(table_tag(bytes)!=table)bad("Row map points to a different member table");auto decoded=record::decode(m.schema,bytes.substr(8));if(decoded.logical_id!=lid||key(m,decoded.row)!=page_groups_.at(rid->page))bad("Cluster slot LID/key differs from group/map");return {*rid,decoded.row};}
Row Heap::get_by_rid(const Rid& id){auto row=get(id.table,id.lid);if(!(row.id==id))throw Error("STALE_ROW_ID","Cluster physical identity was moved or reused");return row;}
void Heap::erase(Id table,Id lid){auto row=get(table,lid);auto p=read(row.id.page);if(!repack(p,row.id.slot,"",false))bad("Removing a cluster slot unexpectedly grew it");auto& group=descriptor_["groups"][page_groups_.at(row.id.page)];write(p,group);pager_.unmap(table,lid);}
Rid Heap::update(Id table,Id lid,const json& row){auto before=get(table,lid);if(before.values==row)return before.id;auto& m=member(table);auto data=tagged(table,record::encode(m.schema,lid,row));auto next_key=key(m,row);if(page_groups_.at(before.id.page)==next_key){auto p=read(before.id.page);if(repack(p,before.id.slot,data,false)){write(p,descriptor_["groups"][next_key]);return before.id;}}
    erase(table,lid);return insert(table,lid,row);
}
std::vector<Row> Heap::scan(Id table){auto& m=member(table);std::vector<Row> rows;
    for(const auto& pg:page_groups_){auto p=read(pg.first);for(std::size_t i=0;i<readle(p,32,2);++i)if(length(p,i)){auto bytes=record_bytes(p,i);auto oid=table_tag(bytes);if(!members_.count(oid))bad("Unknown tableOID in shared page");if(oid!=table)continue;auto value=record::decode(m.schema,bytes.substr(8));Rid rid{pg.first,static_cast<std::uint16_t>(i),generation(p,i),table,value.logical_id};auto mapped=pager_.locate(table,value.logical_id);if(!mapped||!(*mapped==rid)||key(m,value.row)!=pg.second)bad("Scan disagrees with independent member row map/group");rows.push_back({rid,value.row});}}
    return rows;
}
std::function<bool(Row&)> Heap::cursor(Id table){
    member(table);struct Cursor {std::vector<Id> pages;std::size_t page=0,slot=0;std::optional<Page> loaded;};auto state=std::make_shared<Cursor>();for(const auto& item:page_groups_)state->pages.push_back(item.first);
    return [this,table,state](Row& row){auto& m=member(table);while(state->page<state->pages.size()){
        auto id=state->pages[state->page];if(!state->loaded)state->loaded=read(id);auto& p=*state->loaded;
        while(state->slot<readle(p,32,2)){auto slot=state->slot++;if(!length(p,slot))continue;auto bytes=record_bytes(p,slot);auto oid=table_tag(bytes);if(!members_.count(oid))bad("Unknown tableOID in shared page");if(oid!=table)continue;
            auto value=record::decode(m.schema,bytes.substr(8));Rid rid{id,static_cast<std::uint16_t>(slot),generation(p,slot),table,value.logical_id};auto mapped=pager_.locate(table,value.logical_id);if(!mapped||!(*mapped==rid)||key(m,value.row)!=page_groups_.at(id))bad("Cluster cursor differs from member map/group");row={rid,std::move(value.row)};return true;
        }state->loaded.reset();++state->page;state->slot=0;
    }return false;};
}
json Heap::inspect(std::size_t maximum){if(!maximum||maximum>2048)throw Error("CLUSTER_LIMIT","Inspect limit must be1..2048 pages");json pages=json::array();for(const auto& pg:page_groups_){if(pages.size()==maximum)break;auto p=read(pg.first);json tables=json::object();for(std::size_t i=0;i<readle(p,32,2);++i)if(length(p,i)){auto oid=table_tag(record_bytes(p,i));auto name=std::to_string(oid);tables[name]=tables.value(name,std::uint64_t(0))+1;}pages.push_back({{"page_id",pg.first},{"owner_cluster_oid",readle(p,40,8)},{"key",json::parse(pg.second)},{"table_row_counts",tables},{"free_bytes",readle(p,36,2)-readle(p,34,2)}});}return {{"implementation_scope","independent shared-page pager prototype; kernel catalog/locks/WAL not integrated"},{"cluster_oid",oid_},{"groups",descriptor_["groups"].size()},{"members",members_.size()},{"total_pages",page_groups_.size()},{"pages",pages},{"truncated",pages.size()!=page_groups_.size()}};}
} // namespace edudb::cluster
