#include "managed_block_device.hpp"
#include "media_native.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <iomanip>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>

namespace edudb {
namespace {
using namespace media_detail;
constexpr std::size_t BLOCK=8192,HEADER=96,CELL_HEADER=32,CELL=CELL_HEADER+BLOCK,JHEADER=128,JFOOT=32;
constexpr std::uint64_t MAX_BYTES=1ull<<40;
using Block=std::array<unsigned char,BLOCK>;
using Lanes=std::vector<Block>;
enum Layout:unsigned {Stripe=0,Mirror=1,Hamming=2,ByteParity=3,BlockParity=4,RotatingParity=5,DualParity=6};
Layout layout_of(const std::string& s){for(unsigned i=0;i<=6;++i)if(s=="RAID"+std::to_string(i))return static_cast<Layout>(i);fail("MEDIA_OPTION","Unknown managed layout");}
void fault(const char* point){const auto* p=std::getenv("EDUDB_CRASH_AT");if(p&&std::string(p)==point)std::_Exit(86);}
unsigned char gf_mul(unsigned char a,unsigned char b){unsigned char r=0;for(unsigned i=0;i<8;++i){if(b&1)r^=a;bool high=(a&128)!=0;a=static_cast<unsigned char>(a<<1);if(high)a^=0x1d;b>>=1;}return r;}
unsigned char gf_pow(unsigned char a,unsigned n){unsigned char r=1;while(n){if(n&1)r=gf_mul(r,a);a=gf_mul(a,a);n>>=1;}return r;}
unsigned char gf_div(unsigned char a,unsigned char b){if(!b)fail("MEDIA_INTERNAL","Singular parity equations");return gf_mul(a,gf_pow(b,254));}
std::string unique_suffix(){static std::atomic<std::uint64_t> sequence{0};return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())+"-"+std::to_string(++sequence);}
json read_json_file(const fs::path& path){File f(path);auto n=f.size();if(n<2||n>1024*1024)fail("MEDIA_CONTROL","Invalid manifest length");std::string bytes(static_cast<std::size_t>(n),'\0');f.read(0,reinterpret_cast<unsigned char*>(bytes.data()),bytes.size());try{auto j=json::parse(bytes);auto sum=j.at("checksum").get<std::uint32_t>();j.erase("checksum");if(crc(j.dump())!=sum)fail("MEDIA_CONTROL","Manifest checksum mismatch");return j;}catch(const DbError&){throw;}catch(const std::exception&){fail("MEDIA_CONTROL","Malformed media manifest");}}
void atomic_json(const fs::path& directory,const json& value){json j=value;j.erase("checksum");j["checksum"]=crc(j.dump());auto bytes=j.dump();if(bytes.size()>1024*1024)fail("MEDIA_LIMIT","Allocation map exceeds 1MiB manifest budget");auto temporary=directory/("control-media-"+unique_suffix()+".tmp");{File f(temporary,true,true);f.write(0,reinterpret_cast<const unsigned char*>(bytes.data()),bytes.size());f.sync();}replace(temporary,directory/"control-media.json");}
}

struct ManagedBlockDevice::Impl {
    fs::path directory;std::string identity;ManagedMediaOptions options;Layout layout;std::size_t data_members;
    mutable std::mutex mutex;std::unique_ptr<File> lock,journal;std::vector<std::unique_ptr<File>> members;
    std::map<std::uint64_t,std::uint64_t> ranges;std::uint64_t logical_size=0,generation=0;
    std::uint64_t stripe_reads=0,stripe_writes=0,reconstructed=0,replay_count=0,member_failures=0;bool failed=false;
    Impl(std::string path,std::string id,ManagedMediaOptions cfg,bool create):directory(fs::absolute(fs::u8path(path)).lexically_normal()),identity(std::move(id)),options(std::move(cfg)),layout(layout_of(options.layout)),data_members(0){
        if(identity.size()!=32||!std::all_of(identity.begin(),identity.end(),[](char c){return (c>='0'&&c<='9')||(c>='a'&&c<='f');}))fail("MEDIA_OPTION","Database identity must contain 32 lowercase hex characters");
        auto n=options.members;if(n<2||n>16||(layout==Hamming&&n!=7&&n!=8)||(layout==DualParity&&n<4)||(layout>=ByteParity&&layout<=RotatingParity&&n<3))fail("MEDIA_OPTION","Invalid member count for layout");
        data_members=layout==Mirror?1:layout==Hamming?4:layout==DualParity?n-2:layout>=ByteParity?n-1:n;
        fs::create_directories(directory);lock=std::make_unique<File>(directory/"control-media.lock",true,false,true);
        if(create){
            if(fs::exists(directory/"control-media.json")||fs::exists(directory/"control-media-redo.log"))fail("MEDIA_EXISTS","Media already exists; creation will not overwrite it");
            for(std::size_t i=0;i<n;++i)if(fs::exists(member_path(i)))fail("MEDIA_EXISTS","Member already exists; creation will not overwrite it");
            members.resize(n);for(std::size_t i=0;i<n;++i){members[i]=std::make_unique<File>(member_path(i),true,true);auto h=member_header(i);members[i]->write(0,h.data(),h.size());members[i]->sync();}
            journal=std::make_unique<File>(directory/"control-media-redo.log",true,true);journal->sync();atomic_json(directory,manifest());
        }else{
            auto j=read_json_file(directory/"control-media.json");load_manifest(j);members.resize(n);
            for(std::size_t i=0;i<n;++i){if(!fs::exists(member_path(i)))continue;members[i]=std::make_unique<File>(member_path(i));if(members[i]->size()<HEADER)fail("MEDIA_IDENTITY","Truncated member header");std::array<unsigned char,HEADER> h{};members[i]->read(0,h.data(),h.size());if(h!=member_header(i))fail("MEDIA_IDENTITY","Member identity/layout/header mismatch; refusing foreign member");}
            journal=std::make_unique<File>(directory/"control-media-redo.log",false);recover();
        }
    }
    fs::path member_path(std::size_t i)const{std::ostringstream s;s<<"data-member-"<<std::setfill('0')<<std::setw(2)<<i<<".pages";return directory/s.str();}
    std::array<unsigned char,HEADER> member_header(std::size_t i)const{std::array<unsigned char,HEADER> h{};std::memcpy(h.data(),"EDUMEDIA",8);put32(h.data()+8,1);put32(h.data()+12,layout);put32(h.data()+16,static_cast<std::uint32_t>(options.members));put32(h.data()+20,static_cast<std::uint32_t>(i));put32(h.data()+24,BLOCK);std::memcpy(h.data()+32,identity.data(),32);put32(h.data()+92,crc(h.data(),92));return h;}
    json manifest()const{json a=json::array();for(auto [b,e]:ranges)a.push_back({b,e});return {{"format","edudb-managed-media-v1"},{"identity",identity},{"layout",options.layout},{"members",options.members},{"block_bytes",BLOCK},{"logical_bytes",logical_size},{"generation",generation},{"allocated_stripes",a}};}
    void load_manifest(const json& j){try{
        if(j.at("format")!="edudb-managed-media-v1"||j.at("identity")!=identity||j.at("layout")!=options.layout||j.at("members")!=options.members||j.at("block_bytes")!=BLOCK)fail("MEDIA_IDENTITY","Manifest configuration does not match requested device");
        auto bytes=j.at("logical_bytes").get<std::uint64_t>(),seq=j.at("generation").get<std::uint64_t>();if(bytes>MAX_BYTES)fail("MEDIA_CONTROL","Logical file too large");
        std::map<std::uint64_t,std::uint64_t> restored;std::uint64_t prior=0;bool first=true;const auto limit=(bytes+stripe_bytes()-1)/stripe_bytes();
        for(const auto& pair:j.at("allocated_stripes")){if(!pair.is_array()||pair.size()!=2)fail("MEDIA_CONTROL","Invalid allocation interval");auto b=pair.at(0).get<std::uint64_t>(),e=pair.at(1).get<std::uint64_t>();if(b>=e||e>limit||(!first&&b<=prior))fail("MEDIA_CONTROL","Allocation intervals overlap or exceed file");restored[b]=e;prior=e;first=false;}
        ranges=std::move(restored);logical_size=bytes;generation=seq;
    }catch(const DbError&){throw;}catch(const std::exception&){fail("MEDIA_CONTROL","Invalid media manifest fields");}}
    std::uint64_t stripe_bytes()const{return data_members*BLOCK;}
    bool allocated(std::uint64_t s)const{auto it=ranges.upper_bound(s);return it!=ranges.begin()&&s<(--it)->second;}
    void allocate(std::uint64_t s){auto next=ranges.upper_bound(s);if(next!=ranges.begin()){auto prior=std::prev(next);if(s<prior->second)return;if(s==prior->second){++prior->second;if(next!=ranges.end()&&prior->second==next->first){prior->second=next->second;ranges.erase(next);}return;}}if(next!=ranges.end()&&next->first==s+1){auto end=next->second;ranges.erase(next);ranges[s]=end;}else ranges[s]=s+1;}
    std::vector<std::size_t> data_positions(std::uint64_t stripe)const{std::vector<std::size_t> p;if(layout==Hamming)return {2,4,5,6};if(layout==Mirror)return {0};for(std::size_t i=0;i<options.members;++i){if(layout==BlockParity||layout==ByteParity){if(i==options.members-1)continue;}else if(layout==RotatingParity||layout==DualParity){if(i==stripe%options.members||(layout==DualParity&&i==(stripe+1)%options.members))continue;}p.push_back(i);}return p;}
    std::pair<std::size_t,std::size_t> parity_positions(std::uint64_t stripe)const{return {layout==ByteParity||layout==BlockParity?options.members-1:static_cast<std::size_t>(stripe%options.members),static_cast<std::size_t>((stripe+1)%options.members)};}
    Lanes encode(const std::vector<unsigned char>& logical,std::uint64_t stripe)const{
        if(logical.size()!=stripe_bytes())fail("MEDIA_INTERNAL","Wrong logical stripe size");
        Lanes lanes(options.members);auto pos=data_positions(stripe);
        if(layout==Mirror){for(auto& b:lanes)std::copy(logical.begin(),logical.end(),b.begin());return lanes;}
        if(layout==Hamming){
            for(std::size_t k=0;k<logical.size();++k)for(unsigned half=0;half<2;++half){auto nibble=(logical[k]>>(4*half))&15;auto bit=k*2+half;for(unsigned d=0;d<4;++d)if(nibble&(1u<<d))lanes[pos[d]][bit/8]|=static_cast<unsigned char>(1u<<(bit%8));}
            for(std::size_t i=0;i<BLOCK;++i){for(unsigned r=0;r<3;++r){unsigned char p=0;for(unsigned d=0;d<4;++d)if((pos[d]+1)&(1u<<r))p^=lanes[pos[d]][i];lanes[(1u<<r)-1][i]=p;}if(options.members==8){for(unsigned d=0;d<7;++d)lanes[7][i]^=lanes[d][i];}}return lanes;
        }
        for(std::size_t d=0;d<data_members;++d)for(std::size_t i=0;i<BLOCK;++i)lanes[pos[d]][i]=logical[layout==ByteParity?i*data_members+d:d*BLOCK+i];
        if(layout>=ByteParity){auto [p,q]=parity_positions(stripe);for(std::size_t d=0;d<data_members;++d){auto coefficient=gf_pow(2,static_cast<unsigned>(d));for(std::size_t i=0;i<BLOCK;++i){lanes[p][i]^=lanes[pos[d]][i];if(layout==DualParity)lanes[q][i]^=gf_mul(lanes[pos[d]][i],coefficient);}}}return lanes;
    }
    std::vector<unsigned char> decode(std::vector<std::optional<Block>> available,std::uint64_t stripe,bool count=true){
        std::vector<std::size_t> missing;for(std::size_t i=0;i<available.size();++i)if(!available[i])missing.push_back(i);if(count&&!missing.empty()){++reconstructed;member_failures+=missing.size();}
        auto pos=data_positions(stripe);Lanes lanes(options.members);for(std::size_t i=0;i<available.size();++i)if(available[i])lanes[i]=*available[i];
        if(layout==Mirror){auto it=std::find_if(available.begin(),available.end(),[](const auto& x){return x.has_value();});if(it==available.end())fail("MEDIA_UNRECOVERABLE","All mirror copies are missing/corrupt");for(const auto& b:available)if(b&&*b!=**it)fail("MEDIA_INCONSISTENT","CRC-valid mirror copies disagree");for(auto& b:lanes)b=**it;}
        else if(layout==Stripe){if(!missing.empty())fail("MEDIA_UNRECOVERABLE","RAID0 has no redundancy");}
        else if(layout==Hamming){
            if(missing.size()>4)fail("MEDIA_UNRECOVERABLE","Too many Hamming erasures");
            struct Equation{unsigned mask=0;Block rhs{};};std::vector<Equation> equations(options.members==8?4:3);
            for(unsigned r=0;r<equations.size();++r)for(std::size_t i=0;i<options.members;++i){bool coefficient=r==3?true:(i<7&&((i+1)&(1u<<r)));if(!coefficient)continue;auto u=std::find(missing.begin(),missing.end(),i);if(u!=missing.end())equations[r].mask|=1u<<std::distance(missing.begin(),u);else for(std::size_t k=0;k<BLOCK;++k)equations[r].rhs[k]^=lanes[i][k];}
            std::size_t row=0;for(std::size_t col=0;col<missing.size();++col){auto pivot=row;while(pivot<equations.size()&&!(equations[pivot].mask&(1u<<col)))++pivot;if(pivot==equations.size())fail("MEDIA_UNRECOVERABLE","Hamming erasures exceed independent equations");std::swap(equations[row],equations[pivot]);for(std::size_t r=0;r<equations.size();++r)if(r!=row&&(equations[r].mask&(1u<<col))){equations[r].mask^=equations[row].mask;for(std::size_t k=0;k<BLOCK;++k)equations[r].rhs[k]^=equations[row].rhs[k];}++row;}
            for(const auto& e:equations)if(!e.mask&&std::any_of(e.rhs.begin(),e.rhs.end(),[](auto b){return b!=0;}))fail("MEDIA_INCONSISTENT","CRC-valid Hamming syndrome is nonzero");
            for(std::size_t c=0;c<missing.size();++c){auto it=std::find_if(equations.begin(),equations.end(),[&](const auto& e){return e.mask==(1u<<c);});if(it==equations.end())fail("MEDIA_INTERNAL","Missing Hamming pivot");lanes[missing[c]]=it->rhs;}
        }else{
            const auto capacity=layout==DualParity?2u:1u;if(missing.size()>capacity)fail("MEDIA_UNRECOVERABLE","Parity erasure capacity exceeded");auto [p,q]=parity_positions(stripe);std::vector<std::size_t> lost_data;for(std::size_t d=0;d<pos.size();++d)if(!available[pos[d]])lost_data.push_back(d);
            if(lost_data.size()==1){auto d=lost_data[0];for(std::size_t k=0;k<BLOCK;++k){unsigned char value=0;if(available[p]){value=lanes[p][k];for(std::size_t j=0;j<pos.size();++j)if(j!=d)value^=lanes[pos[j]][k];}else if(layout==DualParity&&available[q]){value=lanes[q][k];for(std::size_t j=0;j<pos.size();++j)if(j!=d)value^=gf_mul(lanes[pos[j]][k],gf_pow(2,static_cast<unsigned>(j)));value=gf_div(value,gf_pow(2,static_cast<unsigned>(d)));}else fail("MEDIA_UNRECOVERABLE","No independent parity for missing data");lanes[pos[d]][k]=value;}}
            else if(lost_data.size()==2){auto a=lost_data[0],b=lost_data[1];if(layout!=DualParity||!available[p]||!available[q])fail("MEDIA_UNRECOVERABLE","Two data erasures need P and Q");auto ca=gf_pow(2,static_cast<unsigned>(a)),cb=gf_pow(2,static_cast<unsigned>(b));for(std::size_t k=0;k<BLOCK;++k){unsigned char s=lanes[p][k],t=lanes[q][k];for(std::size_t d=0;d<pos.size();++d)if(d!=a&&d!=b){s^=lanes[pos[d]][k];t^=gf_mul(lanes[pos[d]][k],gf_pow(2,static_cast<unsigned>(d)));}auto x=gf_div(static_cast<unsigned char>(t^gf_mul(cb,s)),static_cast<unsigned char>(ca^cb));lanes[pos[a]][k]=x;lanes[pos[b]][k]=s^x;}}
        }
        std::vector<unsigned char> logical(static_cast<std::size_t>(stripe_bytes()));
        if(layout==Mirror)std::copy(lanes[0].begin(),lanes[0].end(),logical.begin());
        else if(layout==Hamming){for(std::size_t k=0;k<logical.size();++k)for(unsigned half=0;half<2;++half){auto bit=k*2+half;for(unsigned d=0;d<4;++d)if(lanes[pos[d]][bit/8]&(1u<<(bit%8)))logical[k]|=static_cast<unsigned char>(1u<<(d+4*half));}}
        else for(std::size_t d=0;d<data_members;++d)for(std::size_t i=0;i<BLOCK;++i)logical[layout==ByteParity?i*data_members+d:d*BLOCK+i]=lanes[pos[d]][i];
        auto expected=encode(logical,stripe);for(std::size_t i=0;i<available.size();++i)if(available[i]&&*available[i]!=expected[i])fail("MEDIA_INCONSISTENT","CRC-valid data/parity disagree");return logical;
    }
    std::vector<unsigned char> pack(const Block& payload,std::uint64_t stripe,std::uint64_t seq)const{std::vector<unsigned char> b(CELL);std::memcpy(b.data(),"EDMC",4);put32(b.data()+4,1);put64(b.data()+8,stripe);put64(b.data()+16,seq);put32(b.data()+24,crc(payload.data(),payload.size()));put32(b.data()+28,crc(b.data(),28));std::copy(payload.begin(),payload.end(),b.begin()+CELL_HEADER);return b;}
    std::optional<std::pair<std::uint64_t,Block>> unpack(const unsigned char* b,std::uint64_t stripe)const{if(std::memcmp(b,"EDMC",4)||get32(b+4)!=1||get64(b+8)!=stripe||get64(b+16)==0||get32(b+28)!=crc(b,28)||get32(b+24)!=crc(b+CELL_HEADER,BLOCK))return std::nullopt;Block payload{};std::copy(b+CELL_HEADER,b+CELL,payload.begin());return std::make_pair(get64(b+16),payload);}
    std::vector<unsigned char> read_stripe(std::uint64_t stripe){++stripe_reads;if(!allocated(stripe))return std::vector<unsigned char>(static_cast<std::size_t>(stripe_bytes()),0);std::vector<std::optional<Block>> available(options.members);std::optional<std::uint64_t> sequence;
        auto offset=HEADER+stripe*CELL;for(std::size_t i=0;i<options.members;++i){if(!members[i]||members[i]->size()<offset+CELL)continue;std::vector<unsigned char> b(CELL);members[i]->read(offset,b.data(),b.size());auto cell=unpack(b.data(),stripe);if(!cell)continue;if(cell->first>generation)fail("MEDIA_INCONSISTENT","Member is newer than durable media control");if(sequence&&*sequence!=cell->first)fail("MEDIA_INCONSISTENT","CRC-valid stripe generations disagree");sequence=cell->first;available[i]=cell->second;}return decode(std::move(available),stripe);
    }
    void check_writable(){if(failed)fail("MEDIA_RECOVERY_REQUIRED","Previous media mutation failed; reopen for recovery");if(std::any_of(members.begin(),members.end(),[](const auto& p){return !p;}))fail("MEDIA_DEGRADED","Degraded device is read-only until rebuild");}
    void apply_journal(const std::vector<unsigned char>& bytes,const json& target,std::uint64_t stripe){
        for(std::size_t i=0;i<options.members;++i)if(members[i]){members[i]->write(HEADER+stripe*CELL,bytes.data()+JHEADER+i*CELL,CELL);if(i==0)fault("media_after_member_0");}
        for(auto& f:members)if(f)f->sync();
        fault("media_after_members_sync");atomic_json(directory,target);load_manifest(target);fault("media_after_control_switch");journal->truncate(0);journal->sync();fault("media_after_journal_clear");
    }
    void recover(){auto n=journal->size();if(n==0)return;if(n<JHEADER){journal->truncate(0);journal->sync();return;}std::array<unsigned char,JHEADER> h{};journal->read(0,h.data(),h.size());
        if(std::memcmp(h.data(),"EDUMRED1",8)||get32(h.data()+8)!=1||get32(h.data()+12)!=layout||get32(h.data()+16)!=options.members||std::memcmp(h.data()+48,identity.data(),32)||get32(h.data()+124)!=crc(h.data(),124))fail("MEDIA_JOURNAL","Invalid stripe journal header");
        auto seq=get64(h.data()+24),stripe=get64(h.data()+32),body=get64(h.data()+80),manifest_bytes=get64(h.data()+88);if(!seq||manifest_bytes<2||manifest_bytes>1024*1024||body!=options.members*CELL+manifest_bytes||stripe>=MAX_BYTES/stripe_bytes()+1)fail("MEDIA_JOURNAL","Invalid journal fixed lengths/fields");auto expected=JHEADER+body+JFOOT;
        if(n<expected){journal->truncate(0);journal->sync();return;}if(n!=expected)fail("MEDIA_JOURNAL","Unexpected trailing bytes in stripe journal");std::vector<unsigned char> bytes(static_cast<std::size_t>(n));journal->read(0,bytes.data(),bytes.size());auto* footer=bytes.data()+n-JFOOT;
        if(std::memcmp(footer,"EDUMCOM1",8)||get64(footer+8)!=seq||get32(footer+16)!=get32(h.data()+124)||get32(footer+20)!=crc(bytes.data()+JHEADER,static_cast<std::size_t>(body))||get32(footer+28)!=crc(footer,28))fail("MEDIA_JOURNAL","Complete stripe journal checksum/commit marker is corrupt");
        if(seq<generation||(generation!=std::numeric_limits<std::uint64_t>::max()&&seq>generation+1))fail("MEDIA_JOURNAL","Journal generation is not adjacent to control");
        json target;try{target=json::parse(bytes.begin()+JHEADER+options.members*CELL,bytes.end()-JFOOT);}catch(const std::exception&){fail("MEDIA_JOURNAL","Malformed journal control image");}
        auto saved=manifest();load_manifest(target);if(generation!=seq||!allocated(stripe))fail("MEDIA_JOURNAL","Journal target control does not include its stripe");load_manifest(saved);
        std::vector<std::optional<Block>> cells(options.members);for(std::size_t i=0;i<options.members;++i){auto p=unpack(bytes.data()+JHEADER+i*CELL,stripe);if(!p||p->first!=seq)fail("MEDIA_JOURNAL","Invalid journal member afterimage");cells[i]=p->second;}decode(cells,stripe,false);apply_journal(bytes,target,stripe);++replay_count;
    }
    void write_stripe(std::uint64_t stripe,const std::vector<unsigned char>& logical,std::uint64_t end){check_writable();auto before=manifest();if(generation==std::numeric_limits<std::uint64_t>::max())fail("MEDIA_LIMIT","Media generation exhausted");++generation;logical_size=std::max(logical_size,end);allocate(stripe);auto target=manifest();auto seq=generation;load_manifest(before);auto text=target.dump();if(text.size()>1024*1024)fail("MEDIA_LIMIT","Allocation map too large");auto lanes=encode(logical,stripe);std::vector<unsigned char> bytes(JHEADER+options.members*CELL+text.size()+JFOOT);auto* h=bytes.data();std::memcpy(h,"EDUMRED1",8);put32(h+8,1);put32(h+12,layout);put32(h+16,static_cast<std::uint32_t>(options.members));put64(h+24,seq);put64(h+32,stripe);std::memcpy(h+48,identity.data(),32);put64(h+80,options.members*CELL+text.size());put64(h+88,text.size());put32(h+124,crc(h,124));for(std::size_t i=0;i<options.members;++i){auto cell=pack(lanes[i],stripe,seq);std::copy(cell.begin(),cell.end(),bytes.begin()+JHEADER+i*CELL);}std::copy(text.begin(),text.end(),bytes.begin()+JHEADER+options.members*CELL);auto* f=bytes.data()+bytes.size()-JFOOT;std::memcpy(f,"EDUMCOM1",8);put64(f+8,seq);put32(f+16,get32(h+124));put32(f+20,crc(bytes.data()+JHEADER,bytes.size()-JHEADER-JFOOT));put32(f+28,crc(f,28));
        try{journal->truncate(0);journal->write(0,bytes.data(),bytes.size());journal->sync();fault("media_after_journal_sync");apply_journal(bytes,target,stripe);++stripe_writes;}catch(...){failed=true;throw;}
    }
};

ManagedBlockDevice::ManagedBlockDevice(const std::string& directory,const std::string& identity,const ManagedMediaOptions& options,bool create):impl_(std::make_unique<Impl>(directory,identity,options,create)){}
ManagedBlockDevice::~ManagedBlockDevice()=default;
std::uint64_t ManagedBlockDevice::size(){std::lock_guard<std::mutex> l(impl_->mutex);return impl_->logical_size;}
void ManagedBlockDevice::read(std::uint64_t offset,unsigned char* dst,std::size_t length){auto& p=*impl_;std::lock_guard<std::mutex> l(p.mutex);if(offset>p.logical_size||length>p.logical_size-offset)media_detail::fail("MEDIA_RANGE","Read exceeds logical file");while(length){auto stripe=offset/p.stripe_bytes(),at=offset%p.stripe_bytes();auto logical=p.read_stripe(stripe);auto count=std::min<std::size_t>(length,logical.size()-static_cast<std::size_t>(at));std::copy_n(logical.data()+at,count,dst);offset+=count;dst+=count;length-=count;}}
void ManagedBlockDevice::write(std::uint64_t offset,const unsigned char* src,std::size_t length){auto& p=*impl_;std::lock_guard<std::mutex> l(p.mutex);if(offset>MAX_BYTES||length>MAX_BYTES-offset)media_detail::fail("MEDIA_LIMIT","Managed file exceeds 1TiB logical limit");if(!length)return;p.check_writable();while(length){auto stripe=offset/p.stripe_bytes(),at=offset%p.stripe_bytes();auto logical=p.read_stripe(stripe);auto count=std::min<std::size_t>(length,logical.size()-static_cast<std::size_t>(at));std::copy_n(src,count,logical.data()+at);p.write_stripe(stripe,logical,offset+count);offset+=count;src+=count;length-=count;}}
void ManagedBlockDevice::sync(){auto& p=*impl_;std::lock_guard<std::mutex> l(p.mutex);if(p.failed)media_detail::fail("MEDIA_RECOVERY_REQUIRED","Device requires reopen");for(auto& f:p.members)if(f)f->sync();p.journal->sync();}
json ManagedBlockDevice::configuration()const{auto& p=*impl_;std::lock_guard<std::mutex> l(p.mutex);return {{"format","edudb-managed-media-v1"},{"layout",p.options.layout},{"members",p.options.members},{"data_lanes",p.data_members},{"block_bytes",BLOCK},{"stripe_bytes",p.stripe_bytes()},{"hardware_scope","same-host member files; no independent-drive durability claim"},{"member_io","synchronous sequential system calls"},{"degraded_policy","read-only until explicit rebuild"}};}
json ManagedBlockDevice::diagnostics()const{auto& p=*impl_;std::lock_guard<std::mutex> l(p.mutex);json missing=json::array();for(std::size_t i=0;i<p.members.size();++i)if(!p.members[i])missing.push_back(i);return {{"logical_bytes",p.logical_size},{"generation",p.generation},{"missing_members",missing},{"stripe_reads",p.stripe_reads},{"stripe_writes",p.stripe_writes},{"reconstructed_reads",p.reconstructed},{"erased_cells_observed",p.member_failures},{"journal_replays",p.replay_count},{"recovery_required",p.failed},{"allocated_intervals",p.ranges.size()}};}
json ManagedBlockDevice::scrub(bool repair){
    if(repair)media_detail::fail("MEDIA_OPTION","Use explicit member rebuild; scrub does not silently repair");
    auto& p=*impl_;std::lock_guard<std::mutex> l(p.mutex);std::uint64_t checked=0;
    const auto before=p.reconstructed,failures=p.member_failures;
    for(auto [b,e]:p.ranges)for(auto s=b;s<e;++s){p.read_stripe(s);++checked;}
    const auto missing=std::count_if(p.members.begin(),p.members.end(),[](const auto& member){return !member;});
    const bool healthy=p.reconstructed==before&&missing==0;
    return {{"checked_stripes",checked},{"status",healthy?"PASS":"DEGRADED"},{"readable",true},{"all_members_healthy",healthy},{"missing_members",missing},{"reconstructed_stripes",p.reconstructed-before},{"erased_cells",p.member_failures-failures},{"repair",false}};
}
json ManagedBlockDevice::rebuild(std::size_t member){auto& p=*impl_;std::lock_guard<std::mutex> l(p.mutex);if(member>=p.members.size())media_detail::fail("MEDIA_OPTION","Invalid member number");if(p.failed)media_detail::fail("MEDIA_RECOVERY_REQUIRED","Reopen before rebuild");auto target=p.member_path(member);if(!p.members[member]&&fs::exists(target))media_detail::fail("MEDIA_IDENTITY","A new file appeared at missing member path; refusing overwrite");auto temporary=p.directory/("data-member-rebuild-"+unique_suffix()+".tmp");std::uint64_t count=0;{
    File file(temporary,true,true);auto h=p.member_header(member);file.write(0,h.data(),h.size());for(auto [b,e]:p.ranges)for(auto stripe=b;stripe<e;++stripe){auto logical=p.read_stripe(stripe);auto lanes=p.encode(logical,stripe);auto cell=p.pack(lanes[member],stripe,0);std::uint64_t seq=0;auto offset=HEADER+stripe*CELL;for(std::size_t i=0;i<p.members.size();++i)if(i!=member&&p.members[i]&&p.members[i]->size()>=offset+CELL){std::vector<unsigned char> old(CELL);p.members[i]->read(offset,old.data(),old.size());auto parsed=p.unpack(old.data(),stripe);if(parsed){seq=parsed->first;break;}}if(!seq)media_detail::fail("MEDIA_UNRECOVERABLE","No surviving generation for rebuild");cell=p.pack(lanes[member],stripe,seq);file.write(offset,cell.data(),cell.size());++count;}file.sync();}
    p.members[member].reset();try{replace(temporary,target);p.members[member]=std::make_unique<File>(target);}catch(...){p.failed=true;throw;}return {{"member",member},{"rebuilt_stripes",count},{"status","PASS"}};
}
} // namespace edudb
