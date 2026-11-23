#include "access.hpp"
#include <algorithm>
#include <cmath>
#include <cctype>
#include <cstring>
#include <map>
#include <set>
#include <sstream>
#include <unordered_set>

namespace edudb::access {
namespace {
using Id=std::uint64_t;
[[noreturn]] void bad(const std::string& s){throw DbError("CORRUPT_INDEX",s);}
std::string canonical(std::string s){
    for(auto& c:s)c=static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if(s=="DENSE")return "ORDERED_DENSE";
    if(s=="SPARSE"||s=="MULTILEVEL")return "ORDERED_SPARSE";
    if(s=="BITMAP_HYBRID")return "BITMAP";
    return s;
}
int numeric_compare(const json& a,const json& b){
    const bool ai=a.is_number_integer(),bi=b.is_number_integer();
    if(ai&&bi){
        if(a.is_number_unsigned()&&b.is_number_unsigned()){auto x=a.get<Id>(),y=b.get<Id>();return (x>y)-(x<y);}
        if(!a.is_number_unsigned()&&!b.is_number_unsigned()){auto x=a.get<std::int64_t>(),y=b.get<std::int64_t>();return (x>y)-(x<y);}
        if(a.is_number_unsigned()){auto y=b.get<std::int64_t>();if(y<0)return 1;auto x=a.get<Id>(),z=static_cast<Id>(y);return (x>z)-(x<z);}
        return -numeric_compare(b,a);
    }
    if(!ai&&!bi){auto x=a.get<double>(),y=b.get<double>();return (x>y)-(x<y);}
    if(!ai)return -numeric_compare(b,a);
    auto y=b.get<double>();
    if(a.is_number_unsigned()){
        if(y<0)return 1;
        if(y>=18446744073709551616.0)return -1;
        auto yi=static_cast<Id>(y),x=a.get<Id>();if(x!=yi)return (x>yi)-(x<yi);
        return y>static_cast<double>(yi)?-1:0;
    }
    if(y<-9223372036854775808.0)return 1;
    if(y>=9223372036854775808.0)return -1;
    auto yi=static_cast<std::int64_t>(y),x=a.get<std::int64_t>();if(x!=yi)return (x>yi)-(x<yi);
    return (static_cast<double>(yi)>y)-(static_cast<double>(yi)<y);
}
int scalar_compare(const json& a,const json& b){
    auto rank=[](const json& v){return v.is_null()?0:v.is_boolean()?1:v.is_number()?2:v.is_string()?3:4;};
    auto x=rank(a),y=rank(b);if(x==4||y==4)throw DbError("INDEX_KEY","Index key components must be scalar");
    if(x!=y)return (x>y)-(x<y);
    if(!x)return 0;
    if(x==1){bool p=a.get<bool>(),q=b.get<bool>();return (p>q)-(p<q);}
    if(x==2)return numeric_compare(a,b);
    const auto& p=a.get_ref<const std::string&>();const auto& q=b.get_ref<const std::string&>();return (p>q)-(p<q);
}
void valid_key(const Key& k){if(!k.is_array()||k.empty())throw DbError("INDEX_KEY","Nonempty composite key array required");for(const auto& x:k){scalar_compare(x,x);if(x.is_number_float()&&!std::isfinite(x.get<double>()))throw DbError("INDEX_KEY","Nonfinite index key");}}
int cmp(const Entry& a,const Entry& b){int k=compare_keys(a.key,b.key);return k?k:(a.id>b.id)-(a.id<b.id);}
bool less(const Entry& a,const Entry& b){return cmp(a,b)<0;}
Entry fence(const Entry& e){return {e.key,e.id,nullptr};}
json raw_entry(const Entry& e){return json::array({e.key,e.id,e.payload});}
Entry parse_entry(const json& j){if(!j.is_array()||j.size()!=3)bad("Malformed index entry");Entry e{j[0],j[1].get<Id>(),j[2]};valid_key(e.key);return e;}
Entry shortened_separator(const Entry& left_max,const Entry& right_min){
    auto full=fence(right_min);for(std::size_t i=0;i<std::min(left_max.key.size(),right_min.key.size());++i){if(!scalar_compare(left_max.key[i],right_min.key[i]))continue;if(!left_max.key[i].is_string()||!right_min.key[i].is_string())break;const auto& left=left_max.key[i].get_ref<const std::string&>();const auto& right=right_min.key[i].get_ref<const std::string&>();if(left>=right)break;std::size_t common=0;while(common<left.size()&&common<right.size()&&left[common]==right[common])++common;auto count=common+1;if(count>right.size())break;while(count<right.size()&&(static_cast<unsigned char>(right[count])&0xc0)==0x80)++count;
        Entry candidate=full;candidate.id=0;candidate.key.erase(candidate.key.begin()+i+1,candidate.key.end());candidate.key[i]=right.substr(0,count);if(cmp(left_max,candidate)<0&&cmp(candidate,right_min)<=0&&raw_entry(candidate).dump().size()<raw_entry(full).dump().size())return candidate;break;
    }return full;
}
json pack_entries(const std::vector<Entry>& entries,bool compression){
    json out=json::array();Key previous=json::array();
    for(const auto& e:entries){json key=e.key;if(compression)for(std::size_t i=0;i<key.size();++i)if(key[i].is_string()){
        auto value=key[i].get<std::string>();std::string prior=i<previous.size()&&previous[i].is_string()?previous[i].get<std::string>():std::string();
        std::size_t n=0;while(n<value.size()&&n<prior.size()&&value[n]==prior[n])++n;
        // JSON strings remain valid UTF-8, including the suffix.
        while(n<value.size()&&n&&((static_cast<unsigned char>(value[n])&0xc0)==0x80))--n;
        if(n>12){json encoded={{"p",n},{"s",value.substr(n)}};if(encoded.dump().size()<key[i].dump().size())key[i]=std::move(encoded);}
    }out.push_back(json::array({key,e.id,e.payload}));previous=e.key;}
    return out;
}
std::vector<Entry> unpack_entries(const json& data){
    if(!data.is_array())bad("Entry list is not an array");
    std::vector<Entry> out;Key prior=json::array();
    for(const auto& j:data){if(!j.is_array()||j.size()!=3)bad("Malformed encoded entry");Key key=j[0];if(!key.is_array())bad("Key is not an array");
        for(std::size_t i=0;i<key.size();++i)if(key[i].is_object()){
            auto count=key[i].at("p").get<std::size_t>();if(i>=prior.size()||!prior[i].is_string())bad("Missing prefix predecessor");const auto& s=prior[i].get_ref<const std::string&>();if(count>s.size())bad("Invalid prefix length");key[i]=s.substr(0,count)+key[i].at("s").get<std::string>();
        }Entry e{key,j[1].get<Id>(),j[2]};valid_key(e.key);out.push_back(e);prior=key;
    }return out;
}
struct Node {bool leaf=true;std::vector<Entry> entries;std::vector<Id> children;Id next=0,prev=0;Entry minimum;};
}
int compare_keys(const Key& a,const Key& b){
    if(!a.is_array()||!b.is_array())throw DbError("INDEX_KEY","Composite key arrays required");
    for(std::size_t i=0;i<std::min(a.size(),b.size());++i){int c=scalar_compare(a[i],b[i]);if(c)return c;}
    return (a.size()>b.size())-(a.size()<b.size());
}
std::uint64_t hash_key(const Key& key){
    valid_key(key);Id h=14695981039346656037ULL;auto byte=[&](unsigned char b){h^=b;h*=1099511628211ULL;};
    auto text=[&](const std::string& s){for(unsigned char c:s)byte(c);byte(255);};
    for(const auto& v:key){if(v.is_null()){byte(0);continue;}if(v.is_boolean()){byte(1);byte(v.get<bool>());continue;}if(v.is_string()){byte(3);text(v.get<std::string>());continue;}
        byte(2);if(v.is_number_integer()){text(v.dump());continue;}auto d=v.get<double>();if(d==0){text("0");continue;}
        if(d>=-9223372036854775808.0&&d<9223372036854775808.0&&std::trunc(d)==d){text(std::to_string(static_cast<std::int64_t>(d)));continue;}
        if(d>=0&&d<18446744073709551616.0&&std::trunc(d)==d){text(std::to_string(static_cast<Id>(d)));continue;}
        text(v.dump());
    }return h;
}
std::uint64_t hash_key(const Key& key,const std::string& variant){
    valid_key(key);auto name=canonical(variant);if(name=="FNV1A64")return hash_key(key);if(name=="CONSTANT")return 0;
    if(name!="POLY31"&&name!="FIRST_BYTE")throw DbError("INDEX_CONFIG","Hash must be fnv1a64, poly31, first_byte, or constant");
    if(name=="FIRST_BYTE"){if(!key.empty()&&key[0].is_string()){const auto& text=key[0].get_ref<const std::string&>();return text.empty()?0:static_cast<unsigned char>(text[0]);}return hash_key(key);}
    // For a single string this is exactly the textbook polynomial recurrence,
    // including defined unsigned 64-bit wrap. Typed nonstring components use
    // canonical FNV so numerically equal values retain equal hashes.
    Id hash=0;for(std::size_t i=0;i<key.size();++i){if(key[i].is_string()){for(unsigned char byte:key[i].get_ref<const std::string&>())hash=hash*31+byte;}else{auto code=hash_key(json::array({key[i]}));for(unsigned byte=0;byte<8;++byte)hash=hash*31+static_cast<unsigned char>(code>>(byte*8));}if(key.size()>1)hash=hash*31+255;}
    return hash;
}
json capabilities(const std::string& method){auto m=canonical(method);static const std::set<std::string> known={"BPLUS","BTREE","ORDERED_DENSE","ORDERED_SPARSE","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR","BITMAP"};if(!known.count(m))throw DbError("INDEX_METHOD","Unknown access method: "+method);bool hash=m.rfind("HASH_",0)==0;return {{"method",m},{"equality",true},{"range",!hash},{"ordered",!hash&&m!="BITMAP"},{"composite",true},{"null_keys",true},{"covering",true},{"persistent_pages",true},{"cursor","page_streaming"}};}

struct Index::Impl {
    Pager p;json& d;std::string method;json options;bool compression;std::size_t max_keys;bool sorted_static_stream=false;std::map<Id,Id> bulk_tails;
    Impl(Pager pager,json& desc):p(std::move(pager)),d(desc),method(canonical(d.value("method","BPLUS"))),options(d.value("options",json::object())),compression(options.value("prefix_compression",true)),max_keys(options.value("max_keys",(method=="HASH_STATIC"&&options.value("probing",false))||(method=="BPLUS"&&options.value("bstar",false))?31u:255u)){
        if(!p.read||!p.write||!p.allocate||!p.retire||p.page_size_budget<512||max_keys<3||max_keys>255)throw DbError("INDEX_CONFIG","Invalid pager, page budget, or max_keys");
        static const std::set<std::string> methods={"BPLUS","BTREE","ORDERED_DENSE","ORDERED_SPARSE","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR","BITMAP"};if(!methods.count(method))throw DbError("INDEX_METHOD","Unknown access method: "+method);
        if(method.rfind("HASH_",0)==0){(void)hash_key(json::array({0}),options.value("hash",std::string("fnv1a64")));auto depth=options.value("max_depth",20);if(depth<1||depth>20)throw DbError("INDEX_CONFIG","Hash max_depth must be 1..20");if(d.value("root",Id(0))){auto count=d.at("directory_size").get<std::size_t>();if(!count||count>(std::size_t(1)<<20)||!d.at("directory_pages").is_array()||d.at("directory_pages").size()!=(count+127)/128)bad("Invalid persisted hash directory size");if(method=="HASH_EXTENDIBLE"){auto global=d.at("global_depth").get<unsigned>();if(global>20||count!=(std::size_t(1)<<global))bad("Invalid persisted hash global depth");}if(method=="HASH_LINEAR"){auto level=d.at("level").get<unsigned>();auto base=d.at("base_buckets").get<std::size_t>();if(level>20||!base||base>(std::size_t(1)<<20)||base>((std::size_t(1)<<20)>>level))bad("Invalid persisted linear-hash level");auto capacity=base<<level,split=d.at("split_pointer").get<std::size_t>();if(split>=capacity||count!=capacity+split)bad("Invalid persisted linear-hash split pointer");}}}
        options["max_keys"]=max_keys;
        if(method.rfind("HASH_",0)==0){options["hash"]=options.value("hash",std::string("fnv1a64"));if(method=="HASH_STATIC"||method=="HASH_LINEAR")options["buckets"]=options.value("buckets",16u);if(method=="HASH_STATIC")options["probing"]=options.value("probing",false);if(method=="HASH_EXTENDIBLE")options["max_depth"]=options.value("max_depth",20);}
        else if(method=="BITMAP")options["hybrid"]=options.value("hybrid",true);
        else{options["prefix_compression"]=compression;if(method=="BPLUS"){options["bstar"]=options.value("bstar",false);options["dup_layout"]=options.value("dup_layout",options.value("duplicates",std::string("rid")));}if(method.rfind("ORDERED_",0)==0)options["multilevel"]=options.value("multilevel",method=="ORDERED_SPARSE");}
        d["options"]=options;
    }
    Id key_hash(const Key& key)const{return hash_key(key,options.value("hash",std::string("fnv1a64")));}
    void stat(const std::string& name,Id add=1){if(!d.contains("stats"))d["stats"]=json::object();d["stats"][name]=d["stats"].value(name,Id(0))+add;}
    std::size_t size(const json& page)const{return page.dump().size();}
    std::size_t planned_size(json page)const{
        // Decimal JSON pointers can grow when a later split links this page.
        // Reserve their full uint64 width before filling the page; actual
        // writes still obey the unchanged, strict serialized-byte budget.
        for(const char* field:{"next","prev"})if(page.contains(field))page[field]=std::numeric_limits<Id>::max();
        return size(page);
    }
    json read(Id id){if(!id)bad("Zero page pointer");auto j=p.read(id);if(!j.is_object())bad("Nonobject access page");return j;}
    Id alloc(const json& j){if(size(j)>p.page_size_budget)throw DbError("INDEX_PAGE_TOO_LARGE","Access page exceeds byte budget");return p.allocate(j);}
    void write(Id id,const json& j){if(size(j)>p.page_size_budget)throw DbError("INDEX_PAGE_TOO_LARGE","Access page exceeds byte budget");p.write(id,j);}
    json encode(const Node& n,const std::string& type="tree"){json j={{"kind",type},{"leaf",n.leaf},{"entries",pack_entries(n.entries,compression)},{"next",n.next},{"prev",n.prev}};if(!n.leaf){j["children"]=n.children;if(!n.minimum.key.empty())j["minimum"]=raw_entry(fence(n.minimum));}return j;}
    Node node(Id id){auto j=read(id);Node n;n.leaf=j.at("leaf");n.entries=unpack_entries(j.at("entries"));n.next=j.value("next",Id(0));n.prev=j.value("prev",Id(0));if(!n.leaf)n.children=j.at("children").get<std::vector<Id>>();if(j.contains("minimum"))n.minimum=parse_entry(j["minimum"]);if(n.leaf&&!n.entries.empty())n.minimum=fence(n.entries.front());if(!n.leaf&&n.children.size()!=n.entries.size()+1)bad("Invalid child count");if(!std::is_sorted(n.entries.begin(),n.entries.end(),less))bad("Unordered entries");return n;}
    void save(Id id,Node& n){if(n.leaf)n.minimum=n.entries.empty()?Entry{}:fence(n.entries.front());write(id,encode(n));}
    bool fits(const Node& n){return n.entries.size()<=max_keys&&planned_size(encode(n))<=p.page_size_budget;}
    void check_entry(const Entry& e){valid_key(e.key);if(size(e.key)>(p.page_size_budget-384)/3)throw DbError("INDEX_KEY_TOO_LARGE","Serialized composite key exceeds one-third-page structural bound");Node n;n.entries={e};n.minimum=fence(e);if(size(encode(n))>p.page_size_budget-100)throw DbError("INDEX_ENTRY_TOO_LARGE","One entry cannot fit an access page");if(method=="BTREE"&&size(raw_entry(e))>(p.page_size_budget-256)/3)throw DbError("INDEX_ENTRY_TOO_LARGE","B-tree entry exceeds one-third-page bound");if(method=="HASH_STATIC"&&options.value("probing",false)&&size(raw_entry(e))+2>(p.page_size_budget-128)/max_keys)throw DbError("INDEX_ENTRY_TOO_LARGE","Linear-probing entry exceeds configured fixed-slot byte bound");}
    void init_tree(){Node root;d["root"]=alloc(encode(root));d["height"]=1;}
    Entry minimum(Id id,unsigned depth=0){if(depth>=128)bad("Minimum-key traversal depth/cycle limit");auto n=node(id);if(!n.minimum.key.empty())return fence(n.minimum);if(n.leaf)return n.entries.empty()?Entry{}:fence(n.entries.front());return minimum(n.children.front(),depth+1);}
    void fences(Node& n){if(n.leaf){n.minimum=n.entries.empty()?Entry{}:fence(n.entries.front());return;}n.minimum=minimum(n.children.front());n.entries.clear();for(std::size_t i=1;i<n.children.size();++i)n.entries.push_back(minimum(n.children[i]));}
    std::size_t split_point(const Node& n){std::size_t best=0;std::size_t distance=std::numeric_limits<std::size_t>::max();
        for(std::size_t k=1;k<n.entries.size();++k){Node a=n,b=n;a.entries.assign(n.entries.begin(),n.entries.begin()+k);b.entries.assign(n.entries.begin()+k,n.entries.end());a.minimum=fence(a.entries.front());b.minimum=fence(b.entries.front());if(fits(a)&&fits(b)){auto x=size(encode(a)),y=size(encode(b));auto diff=x>y?x-y:y-x;if(diff<distance){distance=diff;best=k;}}}if(!best)throw DbError("INDEX_ENTRY_TOO_LARGE","Cannot split index page within byte budget");return best;}
    bool repartition(Node& parent,std::size_t start,std::size_t old_count,std::size_t new_count,std::optional<std::pair<std::size_t,Node>> replacement={}){
        std::vector<Id> ids(parent.children.begin()+start,parent.children.begin()+start+old_count);std::vector<Node> old;for(std::size_t i=0;i<old_count;++i)old.push_back(replacement&&replacement->first==start+i?replacement->second:node(ids[i]));bool leaf=old.front().leaf;std::vector<Entry> values;std::vector<Id> child_values;
        for(const auto& n:old){if(n.leaf!=leaf)bad("Mixed levels in redistribution");if(leaf)values.insert(values.end(),n.entries.begin(),n.entries.end());else child_values.insert(child_values.end(),n.children.begin(),n.children.end());}
        std::size_t total=leaf?values.size():child_values.size(),offset=0;std::vector<Node> nodes;
        for(std::size_t i=0;i<new_count;++i){Node n;n.leaf=leaf;auto amount=(total-offset+new_count-i-1)/(new_count-i);if(leaf)n.entries.assign(values.begin()+offset,values.begin()+offset+amount);else{n.children.assign(child_values.begin()+offset,child_values.begin()+offset+amount);fences(n);}offset+=amount;if(!fits(n))return false;nodes.push_back(std::move(n));}
        for(std::size_t i=old_count;i<new_count;++i)ids.push_back(alloc(encode(nodes[i])));
        if(leaf){for(std::size_t i=0;i<new_count;++i){nodes[i].prev=i?ids[i-1]:old.front().prev;nodes[i].next=i+1<new_count?ids[i+1]:old.back().next;}if(old.back().next){auto after=node(old.back().next);after.prev=ids[new_count-1];save(old.back().next,after);}}
        for(std::size_t i=0;i<new_count;++i)save(ids[i],nodes[i]);
        for(std::size_t i=new_count;i<old_count;++i)p.retire(ids[i]);
        parent.children.erase(parent.children.begin()+start,parent.children.begin()+start+old_count);parent.children.insert(parent.children.begin()+start,ids.begin(),ids.begin()+new_count);fences(parent);return true;
    }
    bool bstar_insert_repair(Node& parent,std::size_t at,const Node& over){
        for(int dir:{-1,1}){if((dir<0&&!at)||(dir>0&&at+1>=parent.children.size()))continue;auto first=dir<0?at-1:at;auto other=node(parent.children[dir<0?at-1:at+1]);auto count=[&](const Node& n){return n.leaf?n.entries.size():n.children.size();};auto capacity=over.leaf?max_keys:max_keys+1;auto total=count(over)+count(other);if(total<=2*capacity&&repartition(parent,first,2,2,std::make_pair(at,over))){stat("bstar_redistributions");return true;}}
        if(parent.children.size()<2)return false;
        auto first=at?at-1:at;if(repartition(parent,first,2,3,std::make_pair(at,over))){stat("bstar_two_to_three");stat("splits");return true;}return false;
    }
    bool bstar_delete_repair(Node& parent,std::size_t at){auto n=node(parent.children[at]);auto count=n.leaf?n.entries.size():n.children.size(),capacity=n.leaf?max_keys:max_keys+1;if(count>=(2*capacity)/3)return false;
        if(parent.children.size()>=3){auto first=at?at-1:0;if(first+3>parent.children.size())first=parent.children.size()-3;std::size_t total=0;for(std::size_t i=first;i<first+3;++i){auto child=node(parent.children[i]);total+=child.leaf?child.entries.size():child.children.size();}auto target=total<=2*capacity?2u:3u;if(repartition(parent,first,3,target)){stat(target==2?"bstar_three_to_two":"bstar_redistributions");if(target==2)stat("merges");return true;}}
        return false;
    }
    struct Change {Id right;Entry minimum,separator;std::optional<Node> overflow;
        Change(Id r=0,Entry m={},Entry s={},std::optional<Node> o={}):right(r),minimum(std::move(m)),separator(std::move(s)),overflow(std::move(o)){}
    };
    Change plus_insert(Id id,const Entry& e,bool allow_split=true,unsigned depth=0){if(depth>=128)bad("B+ insertion depth/cycle limit");auto n=node(id);if(n.leaf){auto it=std::lower_bound(n.entries.begin(),n.entries.end(),e,less);if(it!=n.entries.end()&&cmp(*it,e)==0)throw DbError("DUPLICATE_INDEX_ENTRY","Duplicate B+ key/row id");n.entries.insert(it,e);n.minimum=fence(n.entries.front());}
        else{auto at=std::upper_bound(n.entries.begin(),n.entries.end(),e,less)-n.entries.begin();auto change=plus_insert(n.children[at],e,!options.value("bstar",false),depth+1);if(change.overflow){if(!bstar_insert_repair(n,at,*change.overflow))throw DbError("INDEX_CONFIG","B* mode requires fixed-size keys fitting the configured fanout");}else{if(at){if(cmp(change.minimum,n.entries[at-1])<0)n.entries[at-1]=change.minimum;}else n.minimum=change.minimum;if(change.right){n.children.insert(n.children.begin()+at+1,change.right);n.entries.insert(n.entries.begin()+at,change.separator);}}}
        if(fits(n)){save(id,n);return {0,n.minimum,{}};}
        if(!allow_split)return {0,n.minimum,{},n};
        Node right;right.leaf=n.leaf;if(n.leaf){auto mid=split_point(n);right.entries.assign(n.entries.begin()+mid,n.entries.end());n.entries.resize(mid);right.next=n.next;right.prev=id;right.minimum=fence(right.entries.front());Id rid=alloc(encode(right));n.next=rid;if(right.next){auto next=node(right.next);next.prev=rid;save(right.next,next);}save(id,n);stat("splits");return {rid,n.minimum,right.minimum};}
        std::size_t mid=0,distance=std::numeric_limits<std::size_t>::max();for(std::size_t k=2;k+1<n.children.size();++k){auto a=n;Node b;b.leaf=false;a.children.assign(n.children.begin(),n.children.begin()+k);b.children.assign(n.children.begin()+k,n.children.end());fences(a);fences(b);if(fits(a)&&fits(b)){auto as=size(encode(a)),bs=size(encode(b)),delta=as>bs?as-bs:bs-as;if(delta<distance){distance=delta;mid=k;}}}if(!mid)throw DbError("INDEX_ENTRY_TOO_LARGE","Cannot partition internal child pages");right.children.assign(n.children.begin()+mid,n.children.end());n.children.resize(mid);fences(n);fences(right);
        if(!fits(n)||!fits(right))throw DbError("INDEX_ENTRY_TOO_LARGE","Internal index split exceeds page budget");
        Id rid=alloc(encode(right));save(id,n);stat("splits");return {rid,n.minimum,right.minimum};
    }
    void plus_add(const Entry& e){auto root=d.at("root").get<Id>();auto c=plus_insert(root,e);if(c.right){Node n;n.leaf=false;n.minimum=c.minimum;n.entries={c.separator};n.children={root,c.right};d["root"]=alloc(encode(n));d["height"]=d.value("height",1)+1;stat("root_grows");}}
    bool plus_repair(Node& parent,std::size_t at){auto id=parent.children[at];auto n=node(id);std::size_t minimum_count=n.leaf?(max_keys+1)/2:(max_keys+2)/2;auto count=[&](const Node& x){return x.leaf?x.entries.size():x.children.size();};if(count(n)>=minimum_count)return false;
        for(int direction:{-1,1}){if((direction<0&&at==0)||(direction>0&&at+1==parent.children.size()))continue;auto pos=direction<0?at-1:at+1;auto sid=parent.children[pos];auto sibling=node(sid);if(count(sibling)<=minimum_count)continue;auto a=n,b=sibling;
            if(a.leaf){if(direction<0){a.entries.insert(a.entries.begin(),b.entries.back());b.entries.pop_back();}else{a.entries.push_back(b.entries.front());b.entries.erase(b.entries.begin());}}
            else{if(direction<0){a.children.insert(a.children.begin(),b.children.back());b.children.pop_back();}else{a.children.push_back(b.children.front());b.children.erase(b.children.begin());}fences(a);fences(b);}
            if(fits(a)&&fits(b)){save(id,a);save(sid,b);fences(parent);stat("redistributions");return true;}}
        if(parent.children.size()<2)return false;
        std::size_t right=at?at:1,left=right-1;auto lid=parent.children[left],rid=parent.children[right];auto a=node(lid),b=node(rid);if(a.leaf){a.entries.insert(a.entries.end(),b.entries.begin(),b.entries.end());a.next=b.next;}else{a.children.insert(a.children.end(),b.children.begin(),b.children.end());fences(a);}
        if(!fits(a))return false;
        if(a.leaf&&b.next){auto after=node(b.next);after.prev=lid;save(b.next,after);}save(lid,a);p.retire(rid);parent.children.erase(parent.children.begin()+right);fences(parent);stat("merges");return true;
    }
    struct Removal {bool found=false;Change change;};
    Removal plus_remove(Id id,const Entry& target,unsigned depth=0){if(depth>=128)bad("B+ deletion depth/cycle limit");auto n=node(id);if(n.leaf){auto it=std::lower_bound(n.entries.begin(),n.entries.end(),target,less);if(it==n.entries.end()||cmp(*it,target))return {};n.entries.erase(it);save(id,n);return {true,{0,n.minimum,{}}};}
        auto at=std::upper_bound(n.entries.begin(),n.entries.end(),target,less)-n.entries.begin();auto removed=plus_remove(n.children[at],target,depth+1);if(!removed.found)return {};if(removed.change.right){n.children.insert(n.children.begin()+at+1,removed.change.right);n.entries.insert(n.entries.begin()+at,removed.change.separator);}bool repaired=options.value("bstar",false)&&bstar_delete_repair(n,at);if(!repaired)repaired=plus_repair(n,at);if(repaired)fences(n);
        // A variable-length separator can grow even during deletion. Rebalance
        // it by bytes and propagate a split, rather than overflowing the page.
        if(fits(n)){save(id,n);return {true,{0,n.minimum,{}}};}
        std::size_t best=0,difference=std::numeric_limits<std::size_t>::max();for(std::size_t k=2;k+1<n.children.size();++k){Node a=n,b;b.leaf=false;a.children.assign(n.children.begin(),n.children.begin()+k);b.children.assign(n.children.begin()+k,n.children.end());fences(a);fences(b);if(fits(a)&&fits(b)){auto x=size(encode(a)),y=size(encode(b)),delta=x>y?x-y:y-x;if(delta<difference){best=k;difference=delta;}}}if(!best)throw DbError("INDEX_ENTRY_TOO_LARGE","Cannot split grown variable-length separator");Node right;right.leaf=false;right.children.assign(n.children.begin()+best,n.children.end());n.children.resize(best);fences(n);fences(right);auto rid=alloc(encode(right));save(id,n);stat("splits");stat("delete_splits");return {true,{rid,n.minimum,right.minimum}};
    }
    bool plus_erase(Id id,const Entry& target){auto removed=plus_remove(id,target);if(removed.change.right){Node root;root.leaf=false;root.children={id,removed.change.right};root.entries={removed.change.separator};root.minimum=removed.change.minimum;d["root"]=alloc(encode(root));d["height"]=d.value("height",1)+1;stat("root_grows");}return removed.found;}
    Id plus_leaf(const Entry& target){Id id=d.at("root");for(int depth=0;depth<128;++depth){auto n=node(id);if(n.leaf)return id;auto at=std::upper_bound(n.entries.begin(),n.entries.end(),target,less)-n.entries.begin();id=n.children[at];}bad("Index height limit");}
    std::optional<Entry> predecessor(const Entry& target){auto id=plus_leaf(target);auto n=node(id);auto it=std::upper_bound(n.entries.begin(),n.entries.end(),target,less);if(it!=n.entries.begin())return *--it;if(n.prev){auto prev=node(n.prev);if(!prev.entries.empty())return prev.entries.back();}return {};}
    std::vector<Entry> plus_range(const std::optional<Key>& low,const std::optional<Key>& high,bool li,bool ui,std::size_t limit){std::vector<Entry> out;if(!limit)return out;Id id=d.at("root");if(low)id=plus_leaf({*low,0,nullptr});else{for(int depth=0;depth<128;++depth){auto n=node(id);if(n.leaf)break;id=n.children.front();}}
        std::set<Id> seen;while(id){if(!seen.insert(id).second)bad("Leaf cycle");auto n=node(id);for(const auto& e:n.entries){int lo=low?compare_keys(e.key,*low):1,hi=high?compare_keys(e.key,*high):-1;if(low&&(lo<0||(!li&&!lo)))continue;if(high&&(hi>0||(!ui&&!hi)))return out;out.push_back(e);if(out.size()==limit)return out;}id=n.next;}return out;}
    void collapse_plus(){auto root=d.at("root").get<Id>();auto n=node(root);while(!n.leaf&&n.children.size()==1){d["root"]=n.children.front();p.retire(root);root=d["root"];n=node(root);d["height"]=std::max(1,d.value("height",1)-1);stat("root_shrinks");}}

    Change btree_insert(Id id,const Entry& e,unsigned depth=0){if(depth>=128)bad("B-tree insertion depth/cycle limit");auto n=node(id);auto it=std::lower_bound(n.entries.begin(),n.entries.end(),e,less);auto at=static_cast<std::size_t>(it-n.entries.begin());if(it!=n.entries.end()&&!cmp(*it,e))throw DbError("DUPLICATE_INDEX_ENTRY","Duplicate B-tree key/row id");
        if(n.leaf)n.entries.insert(it,e);else{auto c=btree_insert(n.children[at],e,depth+1);if(c.right){n.entries.insert(n.entries.begin()+at,c.separator);n.children.insert(n.children.begin()+at+1,c.right);}}
        if(fits(n)){save(id,n);return {};}
        std::size_t median=0,distance=std::numeric_limits<std::size_t>::max();for(std::size_t k=1;k+1<n.entries.size();++k){auto a=n;Node b;b.leaf=n.leaf;a.entries.assign(n.entries.begin(),n.entries.begin()+k);b.entries.assign(n.entries.begin()+k+1,n.entries.end());if(!n.leaf){a.children.assign(n.children.begin(),n.children.begin()+k+1);b.children.assign(n.children.begin()+k+1,n.children.end());}if(fits(a)&&fits(b)){auto as=size(encode(a)),bs=size(encode(b)),delta=as>bs?as-bs:bs-as;if(delta<distance){distance=delta;median=k;}}}if(!median)throw DbError("INDEX_ENTRY_TOO_LARGE","B-tree split cannot fit");Node right;right.leaf=n.leaf;Entry promoted=n.entries[median];right.entries.assign(n.entries.begin()+median+1,n.entries.end());n.entries.resize(median);if(!n.leaf){right.children.assign(n.children.begin()+median+1,n.children.end());n.children.resize(median+1);}Id rid=alloc(encode(right));save(id,n);stat("splits");return {rid,{},promoted};
    }
    void btree_add(const Entry& e){auto root=d.at("root").get<Id>();auto c=btree_insert(root,e);if(c.right){Node n;n.leaf=false;n.entries={c.separator};n.children={root,c.right};d["root"]=alloc(encode(n));d["height"]=d.value("height",1)+1;stat("root_grows");}}
    Entry extreme(Id id,bool maximum){for(int depth=0;depth<128;++depth){auto n=node(id);if(n.leaf){if(n.entries.empty())bad("Empty B-tree child");return maximum?n.entries.back():n.entries.front();}id=maximum?n.children.back():n.children.front();}bad("B-tree depth limit");}
    void btree_repair(Node& parent,std::size_t at){auto id=parent.children[at];auto n=node(id);if(n.entries.size()>=max_keys/2)return;
        for(int dir:{-1,1}){if((dir<0&&!at)||(dir>0&&at+1==parent.children.size()))continue;auto pos=dir<0?at-1:at+1;auto sid=parent.children[pos];auto sib=node(sid);if(sib.entries.size()<=(n.entries.empty()?1:max_keys/2))continue;Node a=n,b=sib;auto sep=dir<0?at-1:at;Entry replacement;
            if(dir<0){a.entries.insert(a.entries.begin(),parent.entries[sep]);replacement=b.entries.back();b.entries.pop_back();if(!a.leaf){a.children.insert(a.children.begin(),b.children.back());b.children.pop_back();}}
            else{a.entries.push_back(parent.entries[sep]);replacement=b.entries.front();b.entries.erase(b.entries.begin());if(!a.leaf){a.children.push_back(b.children.front());b.children.erase(b.children.begin());}}
            if(fits(a)&&fits(b)){parent.entries[sep]=replacement;save(id,a);save(sid,b);stat("redistributions");return;}}
        if(parent.children.size()<2)return;
        auto right=at?at:std::size_t(1),left=right-1;auto lid=parent.children[left],rid=parent.children[right];auto a=node(lid),b=node(rid);a.entries.push_back(parent.entries[left]);a.entries.insert(a.entries.end(),b.entries.begin(),b.entries.end());if(!a.leaf)a.children.insert(a.children.end(),b.children.begin(),b.children.end());if(!fits(a))return;save(lid,a);p.retire(rid);parent.entries.erase(parent.entries.begin()+left);parent.children.erase(parent.children.begin()+right);stat("merges");
    }
    Removal btree_remove(Id id,const Entry& e,unsigned depth=0){if(depth>=128)bad("B-tree deletion depth/cycle limit");auto n=node(id);auto it=std::lower_bound(n.entries.begin(),n.entries.end(),e,less);auto at=static_cast<std::size_t>(it-n.entries.begin());bool found=it!=n.entries.end()&&!cmp(*it,e);
        if(n.leaf){if(!found)return {};n.entries.erase(it);save(id,n);return {true,{}};}
        Removal removed;std::size_t child=at;if(found){auto predecessor=extreme(n.children[at],true),successor=extreme(n.children[at+1],false);bool use_predecessor=size(raw_entry(predecessor))<=size(raw_entry(successor));auto replacement=use_predecessor?predecessor:successor;child=use_predecessor?at:at+1;n.entries[at]=replacement;removed=btree_remove(n.children[child],replacement,depth+1);if(!removed.found)bad("Missing B-tree replacement");}
        else{removed=btree_remove(n.children[child],e,depth+1);if(!removed.found)return {};}
        if(removed.change.right){n.entries.insert(n.entries.begin()+child,removed.change.separator);n.children.insert(n.children.begin()+child+1,removed.change.right);}btree_repair(n,child);if(fits(n)){save(id,n);return {true,{}};}
        std::size_t median=0,difference=std::numeric_limits<std::size_t>::max();for(std::size_t k=1;k+1<n.entries.size();++k){auto a=n;Node b;b.leaf=false;a.entries.assign(n.entries.begin(),n.entries.begin()+k);b.entries.assign(n.entries.begin()+k+1,n.entries.end());a.children.assign(n.children.begin(),n.children.begin()+k+1);b.children.assign(n.children.begin()+k+1,n.children.end());if(fits(a)&&fits(b)){auto x=size(encode(a)),y=size(encode(b)),delta=x>y?x-y:y-x;if(delta<difference){median=k;difference=delta;}}}if(!median)throw DbError("INDEX_ENTRY_TOO_LARGE","Cannot split B-tree after variable-length replacement");auto promoted=n.entries[median];Node right;right.leaf=false;right.entries.assign(n.entries.begin()+median+1,n.entries.end());right.children.assign(n.children.begin()+median+1,n.children.end());n.entries.resize(median);n.children.resize(median+1);auto rid=alloc(encode(right));save(id,n);stat("splits");stat("delete_splits");return {true,{rid,{},promoted}};
    }
    bool btree_erase(Id id,const Entry& e){auto removed=btree_remove(id,e);if(removed.change.right){Node root;root.leaf=false;root.entries={removed.change.separator};root.children={id,removed.change.right};d["root"]=alloc(encode(root));d["height"]=d.value("height",1)+1;stat("root_grows");}return removed.found;}
    void btree_all(Id id,std::vector<Entry>& out,std::set<Id>& seen){if(!seen.insert(id).second)bad("B-tree cycle");auto n=node(id);for(std::size_t i=0;i<n.entries.size();++i){if(!n.leaf)btree_all(n.children[i],out,seen);out.push_back(n.entries[i]);}if(!n.leaf)btree_all(n.children.back(),out,seen);}

    void initialize();
    void insert(const Entry& e);
    bool erase(const Entry& e);
    std::vector<Entry> all();
    std::vector<Entry> find(const Key& key);
    void bulk(std::vector<Entry> entries);
    void bulk_sorted(std::function<bool(Entry&)> next);
    std::vector<Entry> hash_find(const Key& key);
    void hash_init();void hash_add(const Entry& e);bool hash_erase(const Entry& e);std::vector<Entry> hash_all();
    void ordered_init();void ordered_add(const Entry& e);bool ordered_erase(const Entry& e);std::vector<Entry> ordered_all();std::vector<Entry> ordered_range(const std::optional<Key>& low,const std::optional<Key>& high,bool li,bool ui,std::size_t limit);
    void bitmap_init();void bitmap_add(const Entry& e);bool bitmap_erase(const Entry& e);std::vector<Entry> bitmap_all();std::vector<Entry> bitmap_find(const Key& key);
    bool posting()const{return method=="BPLUS"&&canonical(options.value("dup_layout",options.value("duplicates",std::string("rid"))))=="POSTING";}
    void posting_init();void posting_add(const Entry& e);bool posting_erase(const Entry& e);std::vector<Entry> posting_all();std::vector<Entry> posting_find(const Key& key);
    json inspect(std::size_t limit);json verify();
};

#include "access_hash.inc"
#include "access_ordered.inc"
#include "access_bitmap.inc"
#include "access_posting.inc"

namespace {
void collect_descriptor(Index::Impl& x,const json& desc,std::set<Id>& ids,std::size_t maximum,bool& truncated){
    auto walk=[&](Id first){std::vector<Id> pending{first};while(!pending.empty()){auto id=pending.back();pending.pop_back();if(!id||ids.count(id))continue;if(ids.size()>=maximum){truncated=true;return;}ids.insert(id);auto page=x.read(id);if(page.contains("children"))for(auto child:page["children"])pending.push_back(child.get<Id>());if(page.value("next",Id(0)))pending.push_back(page["next"].get<Id>());}};
    walk(desc.value("root",Id(0)));walk(desc.value("head",Id(0)));
    if(desc.contains("directory_pages")){for(auto id:desc["directory_pages"]){walk(id.get<Id>());auto page=x.read(id);for(auto bucket:page.at("values"))walk(bucket.get<Id>());}}
    for(const char* nested:{"ordered_directory","bitmap_dictionary","bitmap_masks","posting_dictionary"})if(desc.contains(nested))collect_descriptor(x,desc[nested],ids,maximum,truncated);
    if(desc.contains("posting_dictionary")){json copy=desc["posting_dictionary"];Index dict(x.p,copy);for(const auto& entry:dict.all())walk(entry.payload.at("head").get<Id>());}
    if(desc.contains("bitmap_dictionary")){json copy=desc["bitmap_dictionary"];Index dict(x.p,copy);for(const auto& entry:dict.all())walk(entry.payload.at("head").get<Id>());}
    if(desc.contains("bitmap_masks")){json copy=desc["bitmap_masks"];Index dict(x.p,copy);for(const auto& entry:dict.all())walk(entry.payload.at("page").get<Id>());}
}
}
json Index::Impl::inspect(std::size_t maximum){if(!maximum||maximum>2000000)throw DbError("INDEX_CONFIG","Inspect page limit must be 1..2000000");std::set<Id> ids;bool truncated=false;collect_descriptor(*this,d,ids,maximum,truncated);json pages=json::array();std::size_t leaves=0;for(auto id:ids){auto page=read(id);auto bytes=size(page);if(page.contains("entries")){auto decoded=unpack_entries(page["entries"]);page["encoded_entries"]=page["entries"];page["entries"]=json::array();for(const auto& entry:decoded)page["entries"].push_back(raw_entry(entry));}page["page_id"]=id;page["serialized_bytes"]=bytes;if(page.value("leaf",false)||page.value("kind","")=="hash_bucket"||page.value("kind","")=="bitmap_posting")++leaves;pages.push_back(page);}return {{"method",method},{"root",d.value("root",Id(0))},{"descriptor",d},{"pages",pages},{"nodes",ids.size()},{"leaf_pages",leaves},{"truncated",truncated},{"page_size_budget",p.page_size_budget},{"capabilities",capabilities(method)}};}
json Index::Impl::verify(){try{auto values=all();if(values.size()!=d.value("count",Id(0)))bad("Descriptor entry count mismatch");if(!d.value("root",Id(0)))return {{"ok",true},{"status","PASS"},{"entries",0},{"nodes",0},{"leaf_pages",0},{"height",0},{"method",method}};if(!std::is_sorted(values.begin(),values.end(),less))bad("Unordered full scan");for(std::size_t i=1;i<values.size();++i)if(!cmp(values[i-1],values[i]))bad("Duplicate key/identifier");
        std::set<Id> ids;bool truncated=false;collect_descriptor(*this,d,ids,2000000,truncated);if(truncated)bad("Verification page budget exceeded");for(auto id:ids)if(size(read(id))>p.page_size_budget)bad("Oversized index page");
        int height=0;std::size_t leaves=0;if((method=="BPLUS"&&!posting())||method=="BTREE"){
            std::set<Id> children,root_exceptions;std::vector<std::pair<Id,Node>> leaf_nodes;int leaf_depth=-1;auto root_node=node(d["root"]);if(!root_node.leaf&&root_node.children.size()==2)root_exceptions.insert(root_node.children.begin(),root_node.children.end());
            auto visit=[&](Id id,int depth,std::optional<Entry> lower,std::optional<Entry> upper,auto&& self)->void{if(!children.insert(id).second)bad("Shared tree child/cycle");auto n=node(id);if(n.entries.empty()&&id!=d.value("root",Id(0)))bad("Empty nonroot tree node");if(method=="BPLUS"&&options.value("bstar",false)&&id!=d.value("root",Id(0))){auto capacity=n.leaf?max_keys:max_keys+1,count=n.leaf?n.entries.size():n.children.size(),minimum_count=root_exceptions.count(id)?(capacity+1)/2:(2*capacity)/3;if(count<minimum_count)bad("B* node violates fixed-count occupancy (root-child exception applied)");}for(const auto& e:n.entries){if(lower&&cmp(e,*lower)<0)bad("Tree entry below lower fence");if(upper&&cmp(e,*upper)>=0)bad("Tree entry above upper fence");}if(n.leaf){if(leaf_depth<0)leaf_depth=depth;if(leaf_depth!=depth)bad("Unbalanced leaf depth");leaf_nodes.emplace_back(id,n);return;}if(n.children.size()<2&&id==d.value("root",Id(0)))bad("Uncollapsed root");for(std::size_t i=0;i<n.children.size();++i)self(n.children[i],depth+1,i?std::optional<Entry>(n.entries[i-1]):lower,i<n.entries.size()?std::optional<Entry>(n.entries[i]):upper,self);};
            visit(d["root"],1,{},{},visit);height=leaf_depth;leaves=leaf_nodes.size();if(method=="BPLUS")for(std::size_t i=0;i<leaf_nodes.size();++i){if(leaf_nodes[i].second.next!=(i+1<leaf_nodes.size()?leaf_nodes[i+1].first:0))bad("B+ next-leaf mismatch");if(leaf_nodes[i].second.prev!=(i?leaf_nodes[i-1].first:0))bad("B+ previous-leaf mismatch");}
        }
        if(method.rfind("HASH_",0)==0&&!(method=="HASH_STATIC"&&options.value("probing",false))){std::set<Id> visited;for(std::size_t i=0;i<d["directory_size"].get<std::size_t>();++i){auto root=dir_at(*this,i);if(!visited.insert(root).second)continue;for(const auto& e:bucket_entries(*this,root))if(dir_at(*this,hash_slot(*this,key_hash(e.key)))!=root)bad("Hash record routed to incorrect bucket");}leaves=visited.size();}
        if(method=="HASH_EXTENDIBLE"){auto global=d.at("global_depth").get<unsigned>();if(global>20||d.at("directory_size").get<std::size_t>()!=(std::size_t(1)<<global))bad("Invalid extensible directory depth/size");auto dirs=directory(*this);std::map<Id,std::vector<std::size_t>> refs;for(std::size_t i=0;i<dirs.size();++i)refs[dirs[i]].push_back(i);for(const auto& pair:refs){auto local=read(pair.first).at("depth").get<unsigned>();if(local>global)bad("Bucket local depth exceeds global depth");auto width=std::size_t(1)<<(global-local);if(pair.second.size()!=width||pair.second.front()%width)bad("Invalid directory bucket reference count/prefix");for(std::size_t i=0;i<width;++i)if(pair.second[i]!=pair.second.front()+i)bad("Noncontiguous high-bit prefix directory");}}
        if(method.rfind("ORDERED_",0)==0){std::vector<Entry> expected,actual;Id id=d.value("head",Id(0)),previous=0;std::set<Id> seen;while(id){if(!seen.insert(id).second)bad("Ordered data cycle");auto n=node(id);if(n.prev!=previous||n.entries.empty())bad("Invalid ordered page links/empty data page");if(method=="ORDERED_SPARSE")expected.push_back({n.entries.front().key,n.entries.front().id,{{"page",id}}});else for(const auto& e:n.entries)expected.push_back({e.key,e.id,{{"page",id}}});previous=id;id=n.next;++leaves;}
            auto& dir=d["ordered_directory"];if(options.value("multilevel",method=="ORDERED_SPARSE")){Index index(p,dir);auto report=index.verify();if(!report.value("ok",false))bad("Ordered directory: "+report.dump());actual=index.all();}else{Id page=dir.value("root",Id(0)),prior=0;std::set<Id> visited;while(page){if(!visited.insert(page).second)bad("Flat directory cycle");auto n=node(page);if(n.prev!=prior)bad("Flat directory back-link mismatch");actual.insert(actual.end(),n.entries.begin(),n.entries.end());prior=page;page=n.next;}}
            if(actual.size()!=expected.size())bad("Dense/sparse directory cardinality mismatch");
            for(std::size_t i=0;i<actual.size();++i)if(cmp(actual[i],expected[i])||actual[i].payload!=expected[i].payload)bad("Dense/sparse directory reference mismatch");}
        for(const char* nested:{"posting_dictionary","bitmap_dictionary","bitmap_masks"})if(d.contains(nested)){Index index(p,d[nested]);auto report=index.verify();if(!report.value("ok",false))bad(std::string(nested)+": "+report.dump());}
        if(method=="BITMAP"){std::map<Id,bool> expected;for(const auto& e:values){bool null=false;for(const auto& v:e.key)null=null||v.is_null();if(!expected.emplace(e.id,null).second)bad("Bitmap row id appears under multiple keys");}Index masks(p,d["bitmap_masks"]);for(const auto& block:masks.all()){auto page=read(block.payload.at("page"));auto base=page.at("base").get<Id>();for(unsigned w=0;w<4;++w){auto live=page["existence"][w].get<Id>(),nulls=page["nulls"][w].get<Id>();if(nulls&~live)bad("NULL bitmap contains deleted row");for(unsigned b=0;b<64;++b){auto id=base+w*64+b;bool present=(live&(Id(1)<<b))!=0;auto it=expected.find(id);if(present!=(it!=expected.end()))bad("Existence bitmap mismatch");if(present&&((nulls&(Id(1)<<b))!=0)!=it->second)bad("NULL bitmap mismatch");if(present)expected.erase(it);}}}if(!expected.empty())bad("Missing existence mask");}
        return {{"ok",true},{"status","PASS"},{"entries",values.size()},{"nodes",ids.size()},{"leaf_pages",leaves},{"height",height},{"method",method}};
    }catch(const std::exception& e){return {{"ok",false},{"status","FAIL"},{"error",e.what()},{"method",method}};}}

void Index::Impl::initialize(){if(d.value("root",Id(0)))return;d["method"]=method;d["count"]=Id(0);d["stats"]=json::object();if(posting())posting_init();else if(method.rfind("HASH_",0)==0)hash_init();else if(method.rfind("ORDERED_",0)==0)ordered_init();else if(method=="BITMAP")bitmap_init();else init_tree();}
void Index::Impl::insert(const Entry& e){check_entry(e);initialize();if(posting())posting_add(e);else if(method.rfind("HASH_",0)==0)hash_add(e);else if(method.rfind("ORDERED_",0)==0)ordered_add(e);else if(method=="BITMAP")bitmap_add(e);else if(method=="BTREE")btree_add(e);else plus_add(e);d["count"]=d.value("count",Id(0))+1;}
bool Index::Impl::erase(const Entry& e){if(!d.value("root",Id(0)))return false;bool found;if(posting())found=posting_erase(e);else if(method.rfind("HASH_",0)==0)found=hash_erase(e);else if(method.rfind("ORDERED_",0)==0)found=ordered_erase(e);else if(method=="BITMAP")found=bitmap_erase(e);else if(method=="BTREE"){found=btree_erase(d["root"],e);collapse_plus();}else{found=plus_erase(d["root"],e);collapse_plus();}if(found)d["count"]=d.value("count",Id(0))-1;return found;}
std::vector<Entry> Index::Impl::all(){if(!d.value("root",Id(0)))return {};if(posting())return posting_all();if(method.rfind("HASH_",0)==0)return hash_all();if(method.rfind("ORDERED_",0)==0)return ordered_all();if(method=="BITMAP")return bitmap_all();if(method=="BTREE"){std::vector<Entry> out;std::set<Id> seen;btree_all(d["root"],out,seen);return out;}return plus_range({},{},true,true,std::numeric_limits<std::size_t>::max());}
std::vector<Entry> Index::Impl::find(const Key& key){if(!d.value("root",Id(0)))return {};if(posting())return posting_find(key);if(method.rfind("HASH_",0)==0)return hash_find(key);if(method=="BITMAP")return bitmap_find(key);if(method.rfind("ORDERED_",0)==0)return ordered_range(key,key,true,true,std::numeric_limits<std::size_t>::max());if(method=="BPLUS")return plus_range(key,key,true,true,std::numeric_limits<std::size_t>::max());auto values=all();values.erase(std::remove_if(values.begin(),values.end(),[&](const Entry& e){return compare_keys(e.key,key)!=0;}),values.end());return values;}
void Index::Impl::bulk(std::vector<Entry> entries){
    if(d.value("count",Id(0)))throw DbError("INDEX_NOT_EMPTY","bulk_load requires empty index");
    for(const auto& e:entries)check_entry(e);
    std::sort(entries.begin(),entries.end(),less);std::size_t at=0;bulk_sorted([&](Entry& out){if(at==entries.size())return false;out=entries[at++];return true;});
}
void Index::Impl::bulk_sorted(std::function<bool(Entry&)> next){
    if(!next)throw DbError("INDEX_CONFIG","Sorted bulk input callback required");
    if(d.value("count",Id(0)))throw DbError("INDEX_NOT_EMPTY","bulk_load_sorted requires empty index");
    initialize();
    std::optional<Entry> prior;Id total=0;auto receive=[&](Entry& e){if(!next(e))return false;check_entry(e);if(prior&&cmp(*prior,e)>=0)throw DbError("INDEX_SORT_ORDER","Sorted bulk input must strictly increase by composite key and row id");prior=fence(e);if(total==std::numeric_limits<Id>::max())throw DbError("INDEX_RESOURCE","Entry count exhausted");++total;return true;};
    if(method!="BPLUS"||posting()||options.value("bstar",false)){sorted_static_stream=method=="HASH_STATIC"&&!options.value("probing",false);try{Entry e;while(receive(e))insert(e);}catch(...){sorted_static_stream=false;bulk_tails.clear();throw;}sorted_static_stream=false;bulk_tails.clear();stat("bulk_stream_entries",total);stat("bulk_incremental_streams");return;}
    auto old=d["root"].get<Id>();std::vector<Id> level;std::vector<Entry> minima;Node current;Id prev=0;Entry e;std::map<Id,std::pair<Entry,Entry>> bounds;
    auto remember=[&](Id id,const Node& n){if(n.leaf)bounds[id]={fence(n.entries.front()),fence(n.entries.back())};else bounds[id]={bounds.at(n.children.front()).first,bounds.at(n.children.back()).second};};
    auto bulk_fences=[&](Node& n){n.minimum=bounds.at(n.children.front()).first;n.entries.clear();for(std::size_t i=1;i<n.children.size();++i){const auto& a=bounds.at(n.children[i-1]).second;const auto& b=bounds.at(n.children[i]).first;n.entries.push_back(compression?shortened_separator(a,b):fence(b));}};
    while(receive(e)){auto candidate=current;candidate.entries.push_back(e);candidate.minimum=fence(candidate.entries.front());if(!current.entries.empty()&&!fits(candidate)){current.minimum=fence(current.entries.front());auto id=alloc(encode(current));remember(id,current);if(prev){auto n=node(prev);n.next=id;save(prev,n);}level.push_back(id);minima.push_back(current.minimum);prev=id;current=Node{};current.prev=id;}current.entries.push_back(e);}
    if(!total)return;
    current.minimum=fence(current.entries.front());auto id=alloc(encode(current));remember(id,current);if(prev){auto n=node(prev);n.next=id;save(prev,n);}level.push_back(id);minima.push_back(current.minimum);int height=1;
    if(level.size()>1){auto tail=node(level.back());if(tail.entries.size()<(max_keys+1)/2){auto before=node(level[level.size()-2]);auto combined=before.entries;combined.insert(combined.end(),tail.entries.begin(),tail.entries.end());auto middle=combined.size()/2;Node a=before,b=tail;a.entries.assign(combined.begin(),combined.begin()+middle);b.entries.assign(combined.begin()+middle,combined.end());a.minimum=fence(a.entries.front());b.minimum=fence(b.entries.front());if(fits(a)&&fits(b)){save(level[level.size()-2],a);save(level.back(),b);remember(level[level.size()-2],a);remember(level.back(),b);minima[minima.size()-2]=a.minimum;minima.back()=b.minimum;}}}
    while(level.size()>1){std::vector<Id> next_level;std::vector<Entry> nextmin;std::size_t i=0;while(i<level.size()){Node n;n.leaf=false;n.minimum=minima[i];n.children.push_back(level[i++]);while(i<level.size()){auto candidate=n;const auto& left=bounds.at(candidate.children.back()).second;const auto& right=bounds.at(level[i]).first;candidate.children.push_back(level[i]);candidate.entries.push_back(compression?shortened_separator(left,right):fence(right));if(!fits(candidate))break;n=std::move(candidate);++i;}if(n.children.size()==1&&i==level.size()&&!next_level.empty()){auto before=node(next_level.back());if(before.children.size()>2){n.children.insert(n.children.begin(),before.children.back());before.children.pop_back();bulk_fences(before);bulk_fences(n);save(next_level.back(),before);remember(next_level.back(),before);}}auto parent=alloc(encode(n));remember(parent,n);next_level.push_back(parent);nextmin.push_back(n.minimum);}
        if(next_level.size()>1){auto tail=node(next_level.back());if(tail.children.size()<(max_keys+2)/2){auto before=node(next_level[next_level.size()-2]);auto children=before.children;children.insert(children.end(),tail.children.begin(),tail.children.end());auto mid=children.size()/2;Node a=before,b=tail;a.children.assign(children.begin(),children.begin()+mid);b.children.assign(children.begin()+mid,children.end());bulk_fences(a);bulk_fences(b);if(fits(a)&&fits(b)){save(next_level[next_level.size()-2],a);save(next_level.back(),b);remember(next_level[next_level.size()-2],a);remember(next_level.back(),b);nextmin[nextmin.size()-2]=a.minimum;nextmin.back()=b.minimum;}}}
        level=std::move(next_level);minima=std::move(nextmin);++height;}
    Id shortened=0;for(const auto& pair:bounds){auto page=node(pair.first);if(!page.leaf)for(std::size_t i=0;i<page.entries.size();++i)if(page.entries[i].key!=bounds.at(page.children[i+1]).first.key)++shortened;}
    p.retire(old);d["root"]=level.front();d["height"]=height;d["count"]=total;stat("bulk_builds");stat("bulk_stream_entries",total);stat("bulk_truncated_separators",shortened);
}
Index::Index(Pager p,json& d):impl_(std::make_unique<Impl>(std::move(p),d)){}
Index::~Index()=default;Index::Index(Index&&) noexcept=default;Index& Index::operator=(Index&&) noexcept=default;
void Index::initialize(){impl_->initialize();}
void Index::insert(const Entry& e){impl_->insert(e);}
bool Index::erase(const Key& key,Id id){valid_key(key);return impl_->erase({key,id,nullptr});}
std::vector<Entry> Index::find(const Key& key){valid_key(key);if(impl_->method=="BTREE"){auto next=cursor(key,key);std::vector<Entry> rows;Entry e;while(next(e))rows.push_back(e);return rows;}return impl_->find(key);}
std::vector<Entry> Index::all(){return impl_->all();}
std::vector<Entry> Index::range(std::optional<Key> low,std::optional<Key> high,bool li,bool ui,std::size_t limit){if(low)valid_key(*low);if(high)valid_key(*high);if(!limit)return {};if(low&&high&&compare_keys(*low,*high)>0)return {};if(!impl_->d.value("root",Id(0)))return {};if(low&&high&&li&&ui&&!compare_keys(*low,*high)){auto rows=find(*low);if(rows.size()>limit)rows.resize(limit);return rows;}if(impl_->method=="BPLUS"&&!impl_->posting())return impl_->plus_range(low,high,li,ui,limit);if(impl_->method.rfind("ORDERED_",0)==0)return impl_->ordered_range(low,high,li,ui,limit);auto out=impl_->all();std::sort(out.begin(),out.end(),less);out.erase(std::remove_if(out.begin(),out.end(),[&](const Entry& e){int a=low?compare_keys(e.key,*low):1,b=high?compare_keys(e.key,*high):-1;return (low&&(a<0||(!li&&!a)))||(high&&(b>0||(!ui&&!b)));}),out.end());if(out.size()>limit)out.resize(limit);return out;}
void Index::bulk_load(std::vector<Entry> entries){impl_->bulk(std::move(entries));}
void Index::bulk_load_sorted(std::function<bool(Entry&)> next){impl_->bulk_sorted(std::move(next));}
void Index::clear(){std::set<Id> ids;bool truncated=false;collect_descriptor(*impl_,impl_->d,ids,2000000,truncated);if(truncated)throw DbError("INDEX_RESOURCE","Cannot clear more than two million access pages");for(auto id:ids)impl_->p.retire(id);for(const char* state:{"root","root_page","count","entries","height","stats","head","ordered_directory","directory_pages","directory_size","global_depth","level","split_pointer","base_buckets","bitmap_dictionary","bitmap_masks","posting_dictionary"})impl_->d.erase(state);impl_->d["root"]=Id(0);impl_->d["count"]=Id(0);}
std::optional<Id> Index::locate(const Key& key,Id row){valid_key(key);auto& x=*impl_;if(!x.d.value("root",Id(0)))return {};Entry target{key,row,nullptr};
    if(x.posting()){Index dictionary(x.p,x.d["posting_dictionary"]);auto found=dictionary.find(key);if(found.empty())return {};Id id=found.front().payload.at("head");while(id){auto page=x.read(id);for(const auto& value:page.at("values"))if(value[0].get<Id>()==row)return id;id=page.value("next",Id(0));}return {};}
    if(x.method=="BPLUS"||x.method=="BTREE"){Id id=x.d["root"];for(int depth=0;depth<128;++depth){auto n=x.node(id);auto at=std::lower_bound(n.entries.begin(),n.entries.end(),target,less);if(x.method=="BTREE"&&at!=n.entries.end()&&!cmp(*at,target))return id;if(n.leaf)return at!=n.entries.end()&&!cmp(*at,target)?std::optional<Id>(id):std::optional<Id>();auto child=x.method=="BPLUS"?std::upper_bound(n.entries.begin(),n.entries.end(),target,less)-n.entries.begin():at-n.entries.begin();id=n.children[child];}bad("Locate depth limit");}
    if(x.method.rfind("ORDERED_",0)==0){auto id=ordered_seek(x,target);if(!id)return {};auto n=x.node(id);for(const auto& e:n.entries)if(!cmp(e,target))return id;return {};}
    if(x.method.rfind("HASH_",0)==0){auto slot=hash_slot(x,x.key_hash(key));auto search=[&](Id id)->std::optional<Id>{std::set<Id> seen;while(id){if(!seen.insert(id).second)bad("Locate hash cycle");auto page=x.read(id);if(page.contains("slots")){for(const auto& cell:page["slots"])if(cell.is_array()&&!cmp(parse_entry(cell),target))return id;}else for(const auto& e:unpack_entries(page.at("entries")))if(!cmp(e,target))return id;id=page.value("next",Id(0));}return {};};if(x.method=="HASH_STATIC"&&x.options.value("probing",false)){for(std::size_t i=0;i<x.d["directory_size"].get<std::size_t>();++i){auto result=search(dir_at(x,i));if(result)return result;}return {};}return search(dir_at(x,slot));}
    Index dict(x.p,x.d["bitmap_dictionary"]);auto found=dict.find(key);if(found.empty())return {};Id id=found.front().payload.at("head");while(id){auto page=x.read(id);for(const auto& e:bitmap_values(page,key))if(e.id==row)return id;id=page.value("next",Id(0));}return {};
}
std::optional<Entry> Index::read_at(Id page,const Key& key,Id row){valid_key(key);auto value=impl_->read(page);Entry target{key,row,nullptr};if(value.contains("entries")){if(value.value("kind","")=="tree"&&!value.value("leaf",true)&&impl_->method!="BTREE")return {};for(const auto& e:unpack_entries(value["entries"]))if(!cmp(e,target))return e;}else if(value.value("kind","")=="hash_probe"){for(const auto& cell:value.at("slots"))if(cell.is_array()){auto e=parse_entry(cell);if(!cmp(e,target))return e;}}return {};}
std::vector<Entry> Index::page_heads(){auto& x=*impl_;std::vector<Entry> heads;auto detail=[&](Id id,const std::vector<Entry>& values){if(!values.empty())heads.push_back({values.front().key,values.front().id,{{"page",id},{"count",values.size()}}});};
    if(!x.d.value("root",Id(0)))return heads;
    if((x.method=="BPLUS"&&!x.posting())||x.method=="BTREE"){auto visit=[&](Id id,auto&& self)->void{auto n=x.node(id);if(n.leaf)detail(id,n.entries);else for(auto child:n.children)self(child,self);};visit(x.d["root"],visit);}
    else if(x.method.rfind("ORDERED_",0)==0){Id id=x.d.value("head",Id(0));std::set<Id> seen;while(id){if(!seen.insert(id).second)bad("Page head cycle");auto n=x.node(id);detail(id,n.entries);id=n.next;}}
    else if(x.method.rfind("HASH_",0)==0){std::set<Id> seen;for(auto root:directory(x)){Id id=root;while(id&&seen.insert(id).second){auto page=x.read(id);std::vector<Entry> values;if(page.contains("slots")){for(const auto& cell:page["slots"])if(cell.is_array())values.push_back(parse_entry(cell));}else values=unpack_entries(page["entries"]);std::sort(values.begin(),values.end(),less);detail(id,values);id=page.value("next",Id(0));}}}
    return heads;
}
std::function<bool(Entry&)> Index::cursor(std::optional<Key> low,std::optional<Key> high,bool li,bool ui){
    if(low)valid_key(*low);
    if(high)valid_key(*high);
    auto x=std::make_shared<Impl>(impl_->p,impl_->d);
    if(!x->d.value("root",Id(0))||(low&&high&&compare_keys(*low,*high)>0))return [](Entry&){return false;};
    auto acceptable=[low,high,li,ui](const Entry& e){auto a=low?compare_keys(e.key,*low):1,b=high?compare_keys(e.key,*high):-1;return !(low&&(a<0||(!li&&!a)))&&!(high&&(b>0||(!ui&&!b)));};
    auto beyond=[high,ui](const Entry& e){auto c=high?compare_keys(e.key,*high):-1;return high&&(c>0||(!ui&&!c));};
    if(x->posting()){Index dictionary(x->p,x->d["posting_dictionary"]);auto dictionary_cursor=dictionary.cursor(low,high,li,ui);struct State{Id page=0;Key key;std::vector<Entry> values;std::size_t at=0;std::set<Id> seen;};auto state=std::make_shared<State>();return [x,state,dictionary_cursor](Entry& out)mutable{for(;;){if(state->at<state->values.size()){out=state->values[state->at++];return true;}if(state->page){if(!state->seen.insert(state->page).second)bad("Posting streaming cycle");auto page=x->read(state->page);state->values.clear();for(const auto& value:page.at("values"))state->values.push_back({state->key,value[0].get<Id>(),value[1]});state->at=0;state->page=page.value("next",Id(0));continue;}Entry key;if(!dictionary_cursor(key))return false;state->key=key.key;state->page=key.payload.at("head");}};}
    if(x->method=="BPLUS"||x->method.rfind("ORDERED_",0)==0){struct State{Id next=0;std::optional<Node> loaded;std::size_t at=0;std::set<Id> seen;bool done=false;};auto state=std::make_shared<State>();
        if(x->method=="BPLUS"){if(low)state->next=x->plus_leaf({*low,0,nullptr});else{state->next=x->d["root"];for(unsigned depth=0;depth<128;++depth){auto n=x->node(state->next);if(n.leaf)break;state->next=n.children.front();}}}else state->next=low?ordered_seek(*x,{*low,0,nullptr}):x->d.value("head",Id(0));
        return [x,state,acceptable,beyond](Entry& out){while(state->next&&!state->done){if(!state->loaded){if(!state->seen.insert(state->next).second)bad("Streaming leaf cycle");state->loaded=x->node(state->next);state->at=0;}auto& n=*state->loaded;while(state->at<n.entries.size()){auto e=n.entries[state->at++];if(beyond(e)){state->done=true;return false;}if(acceptable(e)){out=std::move(e);return true;}}state->next=n.next;state->loaded.reset();}return false;};
    }
    if(x->method=="BTREE"){struct Frame{Node n;std::size_t at;};struct State{std::vector<Frame> stack;bool done=false;};auto state=std::make_shared<State>();auto n=x->node(x->d["root"]);std::size_t at=0;if(low)while(at<n.entries.size()&&compare_keys(n.entries[at].key,*low)<0)++at;state->stack.push_back({std::move(n),at*2});
        return [x,state,low,acceptable,beyond](Entry& out){while(!state->stack.empty()&&!state->done){auto& f=state->stack.back();if(f.n.leaf){auto i=f.at/2;if(i>=f.n.entries.size()){state->stack.pop_back();continue;}auto e=f.n.entries[i];f.at+=2;if(beyond(e)){state->done=true;return false;}if(acceptable(e)){out=std::move(e);return true;}continue;}if(f.at>f.n.entries.size()*2){state->stack.pop_back();continue;}if(!(f.at%2)){auto child=f.n.children[f.at/2];++f.at;auto n=x->node(child);std::size_t at=0;if(low)while(at<n.entries.size()&&compare_keys(n.entries[at].key,*low)<0)++at;state->stack.push_back({std::move(n),at*2});continue;}auto e=f.n.entries[f.at/2];++f.at;if(beyond(e)){state->done=true;return false;}if(acceptable(e)){out=std::move(e);return true;}}return false;};
    }
    if(x->method=="BITMAP"){Index dictionary(x->p,x->d["bitmap_dictionary"]);auto next_key=dictionary.cursor(low,high,li,ui);struct State{Id page=0;Key key;std::vector<Entry> rows;std::size_t at=0;std::set<Id> seen;};auto state=std::make_shared<State>();return [x,state,next_key](Entry& out)mutable{for(;;){if(state->at<state->rows.size()){out=state->rows[state->at++];return true;}if(state->page){if(!state->seen.insert(state->page).second)bad("Bitmap streaming cycle");auto page=x->read(state->page);state->rows=bitmap_values(page,state->key);state->at=0;state->page=page.value("next",Id(0));continue;}Entry key;if(!next_key(key))return false;state->key=key.key;state->page=key.payload.at("head");}};}
    if(x->method.rfind("HASH_",0)==0){bool point=low&&high&&li&&ui&&!compare_keys(*low,*high),probing=x->method=="HASH_STATIC"&&x->options.value("probing",false);struct State{Id page=0;std::size_t directory_at=0,at=0,probe_at=0;std::vector<Entry> rows;std::set<Id> roots,chain;bool point_started=false,done=false;};auto state=std::make_shared<State>();
        if(probing){auto total=x->d["directory_size"].get<std::size_t>()*x->max_keys,start=point?x->key_hash(*low)%total:0;return [x,state,point,total,start,acceptable](Entry& out){while(state->probe_at<total&&!state->done){auto position=(start+state->probe_at++)%total;auto page=x->read(dir_at(*x,position/x->max_keys));auto cell=page["slots"][position%x->max_keys];if(cell.is_null()&&point){state->done=true;return false;}if(cell.is_array()){auto e=parse_entry(cell);if(acceptable(e)){out=std::move(e);return true;}}}return false;};}
        return [x,state,point,low,acceptable](Entry& out){for(;;){while(state->at<state->rows.size()){auto e=state->rows[state->at++];if(acceptable(e)){out=std::move(e);return true;}}if(state->page){auto id=state->page;if(!state->chain.insert(id).second)bad("Hash streaming overflow cycle");auto page=x->read(id);state->rows=unpack_entries(page.at("entries"));state->at=0;state->page=page.value("next",Id(0));continue;}if(point){if(state->point_started)return false;state->point_started=true;state->page=dir_at(*x,hash_slot(*x,x->key_hash(*low)));continue;}bool found=false;while(state->directory_at<x->d["directory_size"].get<std::size_t>()){auto root=dir_at(*x,state->directory_at++);if(state->roots.insert(root).second){state->page=root;found=true;break;}}if(!found)return false;}};
    }
    throw DbError("INDEX_METHOD","No cursor for access method");
}
json Index::inspect(std::size_t max_pages){return impl_->inspect(max_pages);}
json Index::verify(){return impl_->verify();}
} // namespace edudb::access
