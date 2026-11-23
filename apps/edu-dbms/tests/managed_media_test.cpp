#include "managed_block_device.hpp"
#include <algorithm>
#include <chrono>
#include <array>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#ifdef _WIN32
#include <windows.h>
#include <shellapi.h>
#endif
namespace fs=std::filesystem;
using namespace edudb;
namespace {
std::size_t checks=0;
void check(bool value,const std::string& what){++checks;if(!value)throw std::runtime_error(what);}
const std::string id="0123456789abcdef0123456789abcdef";
fs::path member(const fs::path& p,std::size_t i){std::ostringstream s;s<<"data-member-"<<std::setfill('0')<<std::setw(2)<<i<<".pages";return p/s.str();}
std::vector<unsigned char> bytes(const fs::path& p){std::ifstream f(p,std::ios::binary);return std::vector<unsigned char>(std::istreambuf_iterator<char>(f),{});}
void save(const fs::path& p,const std::vector<unsigned char>& b){std::ofstream f(p,std::ios::binary|std::ios::trunc);f.write(reinterpret_cast<const char*>(b.data()),static_cast<std::streamsize>(b.size()));if(!f)throw std::runtime_error("save failed");}
std::uint32_t crc(const unsigned char* p,std::size_t n){std::uint32_t c=0xffffffffu;for(std::size_t i=0;i<n;++i){c^=p[i];for(unsigned k=0;k<8;++k)c=(c>>1)^((c&1)?0xedb88320u:0);}return ~c;}
void put32(unsigned char* p,std::uint32_t n){for(unsigned i=0;i<4;++i)p[i]=static_cast<unsigned char>(n>>(8*i));}
unsigned char multiply(unsigned char a,unsigned char b){unsigned result=0;for(unsigned bit=0;bit<8;++bit)if((b>>bit)&1)result^=static_cast<unsigned>(a)<<bit;for(int bit=14;bit>=8;--bit)if(result&(1u<<bit))result^=0x11du<<(bit-8);return static_cast<unsigned char>(result);}
std::vector<unsigned char> read_all(ManagedBlockDevice& d){std::vector<unsigned char> b(static_cast<std::size_t>(d.size()));d.read(0,b.data(),b.size());return b;}
template<class F>void rejects(F fn,const std::string& reason){bool caught=false;try{fn();}catch(const DbError&){caught=true;}check(caught,reason);}
void physical(const fs::path& path,const ManagedMediaOptions& cfg,const std::vector<unsigned char>& logical){
    const auto type=static_cast<unsigned>(cfg.layout.back()-'0');auto data=type==1?1:type==2?4:type==6?cfg.members-2:type>=3?cfg.members-1:cfg.members;auto stripe_bytes=data*8192;
    std::vector<std::vector<unsigned char>> files;for(std::size_t m=0;m<cfg.members;++m)files.push_back(bytes(member(path,m)));
    auto stripes=(logical.size()+stripe_bytes-1)/stripe_bytes;
    for(std::size_t s=0;s<stripes;++s){std::vector<std::array<unsigned char,8192>> expected(cfg.members);std::vector<unsigned char> source(stripe_bytes,0);std::copy(logical.begin()+s*stripe_bytes,logical.begin()+std::min(logical.size(),(s+1)*stripe_bytes),source.begin());
        std::vector<std::size_t> positions;auto p=type==3||type==4?cfg.members-1:s%cfg.members;auto q=(s+1)%cfg.members;
        if(type==2)positions={2,4,5,6};else for(std::size_t m=0;m<cfg.members;++m)if(type<3||(m!=p&&(type!=6||m!=q)))positions.push_back(m);
        if(type==1){for(auto& a:expected)std::copy(source.begin(),source.end(),a.begin());}
        else if(type==2){for(std::size_t k=0;k<source.size();++k)for(unsigned half=0;half<2;++half){auto nibble=(source[k]>>(half*4))&15;unsigned code=0;for(unsigned bit=0;bit<4;++bit)if(nibble&(1u<<bit))code|=1u<<positions[bit];for(unsigned parity=0;parity<3;++parity){unsigned n=0;for(unsigned loc=1;loc<=7;++loc)if(loc&(1u<<parity))n^=(code>>(loc-1))&1;if(n)code|=1u<<((1u<<parity)-1);}if(cfg.members==8){unsigned parity=0;for(unsigned bit=0;bit<7;++bit)parity^=(code>>bit)&1;code|=parity<<7;}auto lane_bit=k*2+half;for(std::size_t m=0;m<cfg.members;++m)if((code>>m)&1)expected[m][lane_bit/8]|=static_cast<unsigned char>(1u<<(lane_bit%8));}}
        else {for(std::size_t d=0;d<data;++d)for(std::size_t i=0;i<8192;++i)expected[positions[d]][i]=source[type==3?i*data+d:d*8192+i];if(type>=3){unsigned char coefficient=1;for(auto m:positions){for(std::size_t i=0;i<8192;++i){expected[p][i]^=expected[m][i];if(type==6)expected[q][i]^=multiply(coefficient,expected[m][i]);}coefficient=multiply(coefficient,2);}}}
        for(std::size_t m=0;m<cfg.members;++m){auto offset=96+s*(8192+32)+32;check(files[m].size()>=offset+8192,"physical member length");check(std::equal(expected[m].begin(),expected[m].end(),files[m].begin()+offset),"independent stripe/parity layout differs");}
    }
}
void one(const fs::path& root,ManagedMediaOptions cfg){auto path=root/(cfg.layout+"-"+std::to_string(cfg.members));check(!fs::exists(path),"fresh test directory");std::vector<unsigned char> expected;std::mt19937_64 random(1729);
    {ManagedBlockDevice d(path.u8string(),id,cfg,true);auto stripe=d.configuration().at("stripe_bytes").get<std::size_t>();expected.resize(stripe*3+351,0);std::vector<unsigned char> input(expected.size());for(auto& b:input)b=static_cast<unsigned char>(random());d.write(0,input.data(),input.size());expected=input;
        for(unsigned op=0;op<14;++op){auto offset=random()%(expected.size()-1);auto n=std::min<std::size_t>(1+random()%6000,expected.size()-offset);input.resize(n);for(auto& b:input)b=static_cast<unsigned char>(random());d.write(offset,input.data(),input.size());std::copy(input.begin(),input.end(),expected.begin()+offset);check(read_all(d)==expected,"random overwrite agrees with byte vector");}
        check(d.scrub().at("status")=="PASS","scrub");rejects([&]{ManagedBlockDevice other(path.u8string(),id,cfg,false);},"exclusive managed writer");unsigned char b=0;rejects([&]{d.read(expected.size(),&b,1);},"read bounds");d.sync();}
    physical(path,cfg,expected);
    {ManagedBlockDevice d(path.u8string(),id,cfg,false);check(read_all(d)==expected,"reopen");check(d.diagnostics().at("journal_replays")==0,"clean reopen no replay");}
    for(std::size_t m=0;m<cfg.members;++m){auto old=member(path,m);auto aside=old;aside+=".missing-"+std::to_string(m);fs::rename(old,aside);{ManagedBlockDevice d(path.u8string(),id,cfg,false);if(cfg.layout=="RAID0"){rejects([&]{read_all(d);},"RAID0 must reject loss");}else{check(read_all(d)==expected,"single-member degraded decode");unsigned char x=1;rejects([&]{d.write(0,&x,1);},"degraded is read-only");check(d.rebuild(m).at("status")=="PASS","member rebuild");check(read_all(d)==expected,"rebuild logical equivalence");}}if(cfg.layout=="RAID0")fs::rename(aside,old);}
    physical(path,cfg,expected);
    const unsigned tolerated=cfg.layout=="RAID2"?cfg.members==8?3:2:cfg.layout=="RAID6"?2:cfg.layout=="RAID1"?static_cast<unsigned>(cfg.members-1):cfg.layout=="RAID0"?0:1;
    for(std::size_t a=0;a<cfg.members;++a)for(std::size_t b=a+1;b<cfg.members;++b){auto pa=member(path,a),pb=member(path,b),aa=fs::path(pa.u8string()+".pair"),bb=fs::path(pb.u8string()+".pair");fs::rename(pa,aa);fs::rename(pb,bb);{ManagedBlockDevice d(path.u8string(),id,cfg,false);if(tolerated>=2)check(read_all(d)==expected,"all double-member erasures");else rejects([&]{read_all(d);},"double loss exceeds redundancy");}fs::rename(aa,pa);fs::rename(bb,pb);}
    if(cfg.layout=="RAID2"&&cfg.members==8)for(std::size_t a=0;a<8;++a)for(std::size_t b=a+1;b<8;++b)for(std::size_t c=b+1;c<8;++c){std::array<std::size_t,3> missing{a,b,c};for(auto m:missing)fs::rename(member(path,m),fs::path(member(path,m).u8string()+".triple"));{ManagedBlockDevice d(path.u8string(),id,cfg,false);check(read_all(d)==expected,"all SECDED triples are known erasures");}for(auto m:missing)fs::rename(fs::path(member(path,m).u8string()+".triple"),member(path,m));}
    auto first=member(path,0);auto original=bytes(first),corrupt=original;corrupt[96+32+17]^=32;save(first,corrupt);{ManagedBlockDevice d(path.u8string(),id,cfg,false);if(tolerated){check(read_all(d)==expected,"payload CRC turns corruption into erasure");auto scrub=d.scrub();check(scrub["status"]=="DEGRADED"&&scrub["readable"]==true&&scrub["all_members_healthy"]==false&&scrub["erased_cells"].get<int>()>0,"recoverable corruption never reports all members healthy");}else rejects([&]{read_all(d);},"uncorrectable corruption");}save(first,original);
    if(cfg.layout!="RAID0"){corrupt=original;corrupt[96+32+17]^=32;put32(corrupt.data()+96+24,crc(corrupt.data()+96+32,8192));put32(corrupt.data()+96+28,crc(corrupt.data()+96,28));save(first,corrupt);{ManagedBlockDevice d(path.u8string(),id,cfg,false);rejects([&]{read_all(d);},"CRC-valid parity inconsistency rejected");}save(first,original);}
    corrupt=original;corrupt[32]^=1;save(first,corrupt);rejects([&]{ManagedBlockDevice d(path.u8string(),id,cfg,false);},"foreign/corrupt header rejected");save(first,original);
    std::cout<<cfg.layout<<'/'<<cfg.members<<" PASS\n";
}
}
int main(int argc,char** argv){
#ifdef _WIN32
    int count=0;auto wide=CommandLineToArgvW(GetCommandLineW(),&count);std::vector<std::string> args;std::vector<char*> pointers;
    if(wide){for(int i=0;i<count;++i){int n=WideCharToMultiByte(CP_UTF8,0,wide[i],-1,nullptr,0,nullptr,nullptr);std::string a(n,'\0');WideCharToMultiByte(CP_UTF8,0,wide[i],-1,a.data(),n,nullptr,nullptr);a.pop_back();args.push_back(std::move(a));}LocalFree(wide);for(auto& a:args)pointers.push_back(a.data());argc=count;argv=pointers.data();}
#endif
try{if(argc==1){auto root=fs::absolute(fs::path("managed-media-model-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));if(fs::exists(root))throw std::runtime_error("fresh test path required");fs::create_directory(root);for(auto cfg:std::vector<ManagedMediaOptions>{{"RAID0",4},{"RAID1",3},{"RAID2",7},{"RAID2",8},{"RAID3",4},{"RAID4",4},{"RAID5",5},{"RAID6",6}})one(root,cfg);std::cout<<json{{"status","PASS"},{"checks",checks},{"evidence_directory",root.u8string()}}.dump()<<'\n';return 0;}if(argc<3)throw std::runtime_error("usage: probe SELFTEST|CREATE|WRITE|READ directory [layout members]");std::string mode=argv[1];fs::path root=fs::u8path(argv[2]);if(mode=="SELFTEST"){if(fs::exists(root))throw std::runtime_error("selftest root must be new");fs::create_directories(root);for(auto cfg:std::vector<ManagedMediaOptions>{{"RAID0",4},{"RAID1",3},{"RAID2",7},{"RAID2",8},{"RAID3",4},{"RAID4",4},{"RAID5",5},{"RAID6",6}})one(root,cfg);std::cout<<json{{"status","PASS"},{"checks",checks}}.dump()<<'\n';return 0;}if(argc!=5)throw std::runtime_error("layout/count missing");ManagedMediaOptions cfg{argv[3],static_cast<std::size_t>(std::stoul(argv[4]))};ManagedBlockDevice d(root.u8string(),id,cfg,mode=="CREATE");std::vector<unsigned char> b(8192,(mode=="CREATE"||mode=="READ_OLD")?17:91);if(mode=="CREATE"||mode=="WRITE")d.write(0,b.data(),b.size());else if(mode=="READ"||mode=="READ_OLD"){auto expected_value=mode=="READ"?91:17;d.read(0,b.data(),b.size());check(std::all_of(b.begin(),b.end(),[&](auto v){return v==expected_value;}),"exact recovered byte value");}else throw std::runtime_error("mode");std::cout<<d.diagnostics().dump()<<'\n';return 0;}catch(const DbError& e){std::cerr<<e.code<<": "<<e.what()<<'\n';return 2;}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
