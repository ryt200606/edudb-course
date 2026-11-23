#pragma once
#include "record_codec.hpp"
#include <array>
#include <functional>
#include <map>
#include <optional>
namespace edudb::cluster {
using json=nlohmann::json;using Id=std::uint64_t;using Page=std::array<unsigned char,8192>;
struct Error:std::runtime_error{std::string code;Error(std::string c,std::string m):std::runtime_error(std::move(m)),code(std::move(c)){};};
struct Rid {Id page=0;std::uint16_t slot=0;std::uint32_t generation=0;Id table=0,lid=0;bool operator==(const Rid& r)const{return page==r.page&&slot==r.slot&&generation==r.generation&&table==r.table&&lid==r.lid;}};
struct Row {Rid id;json values;};
struct Pager {
    std::function<Page(Id)> read;
    std::function<void(Id,const Page&)> write;
    std::function<Id()> allocate;
    // The production adapter must use each table's OWN persistent B+ row map,
    // not a cluster-wide LID namespace. These callbacks participate in its tx.
    std::function<std::optional<Rid>(Id table,Id lid)> locate;
    std::function<void(const Rid&)> map;
    std::function<void(Id table,Id lid)> unmap;
};
// Prototype pager module, not yet wired into EduDB catalog/locks/WAL. Every
// data page is real8KiB storage with owner=clusterOID and a tableOID prefix on
// each live slot. Descriptor and row-map callbacks must be atomic in a tx.
class Heap {
public:
    Heap(Pager pager,json& descriptor,Id cluster_oid);
    void attach(Id table_oid,const json& definition,const std::vector<unsigned>& key_columns);
    void detach_empty(Id table_oid);
    Rid insert(Id table_oid,Id lid,const json& row);
    Row get(Id table_oid,Id lid);
    Row get_by_rid(const Rid& id);
    Rid update(Id table_oid,Id lid,const json& row);
    void erase(Id table_oid,Id lid);
    std::vector<Row> scan(Id table_oid);
    std::function<bool(Row&)> cursor(Id table_oid);
    json inspect(std::size_t maximum_pages=256);
private:
    struct Member {record::Schema schema;std::vector<unsigned> columns;};
    Pager pager_;json& descriptor_;Id oid_;std::map<Id,Member> members_;std::map<Id,std::string> page_groups_;
    Member& member(Id table);
    std::string key(Member& member,const json& row);
    Page read(Id id);
    void write(Page& page,json& group);
};
} // namespace edudb::cluster
