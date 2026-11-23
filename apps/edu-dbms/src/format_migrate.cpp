#include "edudb.hpp"
#include "format_copy.hpp"
#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
namespace edudb {
namespace {
namespace fs=std::filesystem;
constexpr std::uint64_t bag_limit=64u*1024u*1024u;
struct TableImage {json structure;std::map<std::string,std::uint64_t> bag;std::uint64_t rows=0;};
struct DatabaseImage {std::map<std::string,TableImage> tables;std::set<std::string> clusters;std::string uuid;};
json schema_image(const json& metadata){
    json columns=json::array();for(const auto& c:metadata.at("columns"))columns.push_back({{"name",c.at("name")},{"type",c.at("type")},{"length",c.value("length",json(0))},{"not_null",c.value("not_null",false)},{"primary",c.value("primary",false)},{"unique",c.value("unique",false)},{"default",c.value("default",json())}});
    std::map<std::string,json> ordered;for(const auto& x:metadata.at("indexes")){auto names=x.contains("columns")?x.at("columns"):json::array({x.at("column")});ordered[x.at("name").get<std::string>()]={{"name",x.at("name")},{"columns",names},{"method",x.value("method",std::string("BPLUS"))},{"unique",x.value("unique",false)},{"primary",x.value("primary",false)},{"include",x.value("include",json::array())},{"options",x.value("options",json::object())}};}
    json indexes=json::array();for(const auto& x:ordered)indexes.push_back(x.second);
    auto organization=metadata.value("organization",json{{"method","HEAP"},{"columns",json::array()},{"options",json::object()}});
    json out={{"columns",columns},{"indexes",indexes},{"organization",organization},{"record_format",metadata.value("record_format",json("JSON"))},{"record_options",metadata.value("record_options",json::object())}};
    if(metadata.contains("cluster"))out["cluster"]={{"name",metadata["cluster"].at("name")},{"columns",metadata["cluster"].at("columns")}};
    if(metadata.contains("materialized_view")){const auto& mv=metadata.at("materialized_view");out["materialized_view"]={{"sql",mv.at("sql")},{"dependencies",mv.at("dependencies")},{"refresh",mv.value("refresh",std::string("manual"))},{"maintenance_policy",mv.value("maintenance_policy",json("auto"))}};}
    return out;
}
DatabaseImage capture(const fs::path& path){Storage db(path.u8string());TransactionOptions options;options.read_only=true;auto tx=db.begin(options);DatabaseImage image;image.uuid=db.query_workspace().second;std::uint64_t accounted=0;auto clusters=db.clusters(tx);for(auto i=clusters.begin();i!=clusters.end();++i)image.clusters.insert(i.key());auto catalog=db.catalog(tx);
    try{for(auto i=catalog.begin();i!=catalog.end();++i){TableImage table;auto metadata=db.read_table(tx,i.key(),false,false);table.structure=schema_image(metadata);auto cursor=db.scan(tx,i.key());StoredRow row;while(cursor->next(row)){auto encoded=row.values.dump();auto found=table.bag.find(encoded);if(found==table.bag.end()){if(encoded.size()+128>bag_limit-accounted)throw DbError("MIGRATION_LIMIT","Exact typed-row verification exceeds64MiB memory accounting");accounted+=encoded.size()+128;table.bag.emplace(std::move(encoded),1);}else ++found->second;if(++table.rows>1000000)throw DbError("MIGRATION_LIMIT","Exact row verification exceeds1000000 rows per table");}if(table.rows!=metadata.at("row_count").get<std::uint64_t>())throw DbError("MIGRATION_VERIFY","Source row counter differs from actual heap/index scan");image.tables.emplace(i.key(),std::move(table));}auto verification=db.verify(tx);if(!verification.value("ok",false))throw DbError("MIGRATION_VERIFY",verification.dump());db.commit(tx);}catch(...){db.rollback(tx);throw;}return image;
}
json compare(const DatabaseImage& original,const fs::path& destination){Storage db(destination.u8string());TransactionOptions options;options.read_only=true;auto tx=db.begin(options);json report=json::array();
    try{auto names=db.catalog(tx);if(names.size()!=original.tables.size())throw DbError("MIGRATION_VERIFY","Table count differs");auto clusters=db.clusters(tx);std::set<std::string> actual;for(auto i=clusters.begin();i!=clusters.end();++i)actual.insert(i.key());if(actual!=original.clusters)throw DbError("MIGRATION_VERIFY","Cluster names differ");for(const auto& pair:original.tables){if(!names.contains(pair.first))throw DbError("MIGRATION_VERIFY","Missing table: "+pair.first);auto metadata=db.read_table(tx,pair.first,false,false);auto schema=schema_image(metadata);if(schema!=pair.second.structure)throw DbError("MIGRATION_SCHEMA_MISMATCH","Logical structure differs for "+pair.first+"; constraint/index/organization/record parameters must be representable without loss");auto expected=pair.second.bag;auto cursor=db.scan(tx,pair.first);StoredRow row;std::uint64_t count=0;while(cursor->next(row)){auto encoded=row.values.dump();auto found=expected.find(encoded);if(found==expected.end()||!found->second)throw DbError("MIGRATION_VERIFY","Typed row bag mismatch in "+pair.first);--found->second;++count;}if(count!=pair.second.rows||metadata.at("row_count")!=count)throw DbError("MIGRATION_VERIFY","Actual row count differs for "+pair.first);for(const auto& entry:expected)if(entry.second)throw DbError("MIGRATION_VERIFY","Source rows missing from target");json bag=json::array();for(const auto& entry:pair.second.bag)bag.push_back(json::array({entry.first,entry.second}));report.push_back({{"table",pair.first},{"rows",count},{"distinct_typed_rows",pair.second.bag.size()},{"schema_sha256",format_copy::sha256(schema.dump())},{"typed_row_bag_sha256",format_copy::sha256(bag.dump())},{"comparison","complete canonical typed-JSON row multiset equality"}});}auto verification=db.verify(tx);if(!verification.value("ok",false))throw DbError("MIGRATION_VERIFY",verification.dump());db.commit(tx);}catch(...){db.rollback(tx);throw;}return report;
}
void inject(const char* phase){const char* chosen=std::getenv("EDUDB_TEST_MIGRATE_V3_FAIL");if(chosen&&std::string(chosen)==phase)throw DbError("MIGRATION_TEST_FAILURE",std::string("Controlled migration failure at ")+phase);}
StorageOptions options_from(const json& c){StorageOptions result;result.recovery_mode=c.value("recovery_mode",std::string("REDO_PRIVATE"));if(result.recovery_mode=="ARIES_PAGE"){result.aries_steal=c.value("aries_steal",true);result.aries_force_at_commit=c.value("aries_force_at_commit",false);}auto layout=c.value("data_layout",json{{"kind","single"}});if(layout.value("kind",std::string())=="managed"){result.data_layout=layout.at("layout").get<std::string>();result.media_members=layout.at("members").get<std::size_t>();}return result;}
}
json Storage::migrate_v3(const std::string& source,const std::string& destination){
    format_copy::Destination target(fs::u8path(destination));
    format_copy::LegacySnapshot snapshot(fs::u8path(source),target.parent(),{},&target);snapshot.promote_owned_copy();inject("after_copy");
    auto options=options_from(snapshot.control());std::string sql;
    {Engine input(snapshot.path().u8string());auto sid=input.open_session();auto exported=input.export_data(sid,{{"format","sql"}});input.close_session(sid);if(!exported.value("ok",false))throw DbError("MIGRATION_EXPORT",exported.at("error").dump());sql=exported.at("content").get<std::string>();}
    inject("after_export");const auto original=capture(snapshot.path());if(original.uuid!=snapshot.control().at("uuid").get<std::string>())throw DbError("MIGRATION_VERIFY","Recovered source-copy UUID changed");
    format_copy::OwnedDirectory staging(target.parent(),"staging");
    {Engine output(staging.path().u8string(),128,32u*1024u*1024u,options);auto sid=output.open_session();auto imported=output.import_data(sid,{{"format","sql"},{"content",sql}});output.close_session(sid);if(!imported.value("ok",false))throw DbError("MIGRATION_IMPORT",imported.at("error").dump());}
    inject("after_import");json first=compare(original,staging.path());std::string fresh_uuid;
    {Storage reopened(staging.path().u8string());fresh_uuid=reopened.query_workspace().second;if(fresh_uuid==original.uuid)throw DbError("MIGRATION_VERIFY","New UUID must differ from original");reopened.checkpoint();}
    auto second=compare(original,staging.path());if(first!=second)throw DbError("MIGRATION_VERIFY","Offline reopen verification differs");snapshot.verify_source_unchanged();auto c=format_copy::read_control(staging.path()/"control.json");if(c.value("version",0)!=4||c.value("page_format",0)!=3)throw DbError("MIGRATION_VERIFY","Target control/page formats differ");
    json receipt={{"status","PASS"},{"source",snapshot.source().u8string()},{"destination",target.path().u8string()},{"source_format",3},{"destination_format",4},{"page_format",3},{"source_uuid",original.uuid},{"destination_uuid",fresh_uuid},{"source_unchanged",true},{"source_files",snapshot.source_files()},{"logical_sql_sha256",format_copy::sha256(sql)},{"logical_sql_bytes",sql.size()},{"tables",second},{"recovery_mode",c.at("recovery_mode")},{"data_layout",c.at("data_layout")},{"validation","independent native structure plus complete typed row bag; checkpoint, close and offline reopen"},{"power_loss","NOT_RUN"}};
    {std::ofstream out(staging.path()/"migration-v3-receipt.json",std::ios::binary);out<<receipt.dump(2);out.flush();if(!out)throw DbError("MIGRATION_IO","Cannot write owned migration receipt");}
    inject("before_publish");snapshot.verify_source_unchanged();staging.publish(target);return receipt;
}
}
