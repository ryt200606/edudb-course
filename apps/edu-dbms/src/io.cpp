#include "io.hpp"
#include <map>
#include <mutex>
namespace edudb { namespace {
thread_local IoScope* scope=nullptr;
// Scope-owned strings have ordinary stack lifetimes. TLS itself is trivial:
// no dynamic TLS destructor is registered when a short worker thread exits.
thread_local const IoOwnerScope* active_owner=nullptr;
std::mutex metrics_mutex;
std::map<std::string,std::array<IoCounter,4>> metrics;
const char* roles[]={"data","wal","control","temp"};
std::string group(const std::filesystem::path& path){auto prepared=path;
#ifdef _WIN32
auto text=prepared.make_preferred().u8string();if(text.rfind("\\\\?\\UNC\\",0)==0)text="\\\\"+text.substr(8);else if(text.rfind("\\\\?\\",0)==0)text=text.substr(4);prepared=std::filesystem::u8path(text);
#endif
auto value=(prepared.is_absolute()?prepared:std::filesystem::absolute(prepared)).lexically_normal();while(value.has_relative_path()&&value.filename().empty())value=value.parent_path();return value.make_preferred().u8string();}
std::size_t role_of(const std::filesystem::path& path){auto n=path.filename().u8string();return (n=="data.pages"||(n.rfind("data-member-",0)==0&&n.size()>6&&n.substr(n.size()-6)==".pages"))?0:n.rfind("wal-",0)==0?1:(n.rfind("control",0)==0||n=="database.lock")?2:3;}
void add(IoCounter& c,const std::string& op,std::uint64_t bytes,std::uint64_t ns,bool failed){if(op=="read"){++c.reads;c.read_bytes+=bytes;c.read_ns+=ns;}else if(op=="write"){++c.writes;c.write_bytes+=bytes;c.write_ns+=ns;}else if(op=="sync"){++c.syncs;c.sync_ns+=ns;}else if(op=="truncate")++c.truncates;if(failed)++c.failures;}
json describe(const std::array<IoCounter,4>& a){json j=json::object();for(std::size_t i=0;i<4;++i){const auto& c=a[i];j[roles[i]]={{"read_calls",c.reads},{"write_calls",c.writes},{"read_bytes",c.read_bytes},{"write_bytes",c.write_bytes},{"read_ns",c.read_ns},{"write_ns",c.write_ns},{"sync_calls",c.syncs},{"sync_ns",c.sync_ns},{"failures",c.failures},{"truncate_calls",c.truncates}};}return j;}
}
IoOwnerScope::IoOwnerScope(const std::filesystem::path& directory):previous_(active_owner){if(directory.empty())throw DbError("ARGUMENT","I/O owner directory must be explicit");owner_=group(directory);active_owner=this;}
IoOwnerScope::~IoOwnerScope() noexcept{active_owner=previous_;}
IoScope::IoScope(std::string purpose):previous_(scope),purpose_(std::move(purpose)){scope=this;}
IoScope::~IoScope(){scope=previous_;}
void IoScope::record(std::size_t role,const std::string& op,std::uint64_t bytes,std::uint64_t ns,bool failed,const std::filesystem::path& path,std::uint64_t offset){add(counters_[role],op,bytes,ns,failed);++trace_total_;if(trace_.size()<8192)trace_.push_back({{"role",roles[role]},{"operation",op},{"file",path.filename().u8string()},{"offset",offset},{"bytes",bytes},{"duration_ns",ns},{"failed",failed}});}
json IoScope::snapshot()const{return {{"scope",purpose_},{"measurement","operating-system file calls; not physical media I/O"},{"files",describe(counters_)},{"trace",trace_},{"trace_total",trace_total_},{"trace_truncated",trace_total_>trace_.size()}};}
void io_record(const std::filesystem::path& path,const std::string& op,std::uint64_t bytes,std::uint64_t ns,bool failed,std::uint64_t offset){auto role=role_of(path);{std::lock_guard<std::mutex> l(metrics_mutex);add(metrics[active_owner?active_owner->owner_:group(path.parent_path())][role],op,bytes,ns,failed);}if(scope)scope->record(role,op,bytes,ns,failed,path,offset);}
json io_snapshot(const std::filesystem::path& directory){std::lock_guard<std::mutex> l(metrics_mutex);return {{"scope","instance cumulative"},{"measurement","operating-system file calls; not physical media I/O"},{"files",describe(metrics[group(directory)])}};}
void io_reset(const std::filesystem::path& directory){std::lock_guard<std::mutex> l(metrics_mutex);metrics[group(directory)]={};}
}
