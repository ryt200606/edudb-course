#include "record_codec.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <limits>
#include <random>
using namespace edudb::record;
namespace {
std::size_t checks=0;
void require(bool b,const char* m){++checks;if(!b)throw std::runtime_error(m);}
template<class F>void rejects(F fn,const std::string& code={}){bool caught=false;try{fn();}catch(const Error& e){caught=code.empty()||e.code==code;}require(caught,"Expected codec error and precise category");}
json col(const char* name,const char* type,std::uint64_t length=0){return {{"name",name},{"type",type},{"length",length},{"not_null",false}};}
json meta(const char* format){return {{"record_format",format},{"columns",json::array({col("id","INTEGER"),col("flag","BOOLEAN"),col("姓名","VARCHAR",3)})}};}
void put(std::string& b,std::size_t at,std::uint64_t n,unsigned width){for(unsigned i=0;i<width;++i)b[at+i]=static_cast<char>(n>>(8*i));}
void seal(std::string& b){std::uint32_t c=~0u;for(std::size_t i=0;i<b.size()-4;++i){c^=static_cast<unsigned char>(b[i]);for(int j=0;j<8;++j)c=(c>>1)^((c&1)?0xedb88320u:0);}put(b,b.size()-4,~c,4);}
json reference(std::uint64_t bytes=16384){return {{"$lob",{{"method","BPLUS"},{"root",91},{"height",2},{"count",6},{"options",{{"max_keys",255},{"prefix_compression",false}}},{"stats",{{"splits",1},{"merges",0}}}}},{"owner",7},{"bytes",bytes}};}
void exact_layout(){
    auto fixed=schema(meta("FIXED"));auto row=json::array({std::numeric_limits<std::int64_t>::min(),true,"中🙂"});auto b=encode(fixed,42,row);
    require(b.size()==62&&layout(fixed)["fixed_record_bytes"]==62,"Hand-calculated fixed width:32 header+1 bitmap+8 integer+1 bool+16 string+4 CRC");
    require(b.substr(0,4)=="EDRC"&&b[4]==1&&b[5]==1&&static_cast<unsigned char>(b[8])==42,"Actual versioned binary header");
    require(std::all_of(b.begin()+33,b.begin()+40,[](char c){return c==0;})&&static_cast<unsigned char>(b[40])==128&&b[41]==1,"INTEGER64 little endian and BOOLEAN fixed offsets");
    require(b[42]==7&&b.substr(46,7)=="中🙂"&&std::all_of(b.begin()+53,b.begin()+58,[](char c){return c==0;}),"VARCHAR reserves4*3 bytes while storing7 UTF-8 bytes for2 characters");
    auto decoded=decode(fixed,b);require(decoded.logical_id==42&&decoded.row==row&&decoded.format==Format::Fixed,"Exact fixed field reconstruction");
    auto short_row=json::array({1,false,"a"}),nulls=json::array({nullptr,nullptr,nullptr});require(encode(fixed,43,short_row).size()==62&&encode(fixed,44,nulls).size()==62,"All rows of fixed schema have the same bytes including NULL");
    auto variable=schema(meta("VARIABLE"));auto compact=encode(variable,42,row);require(compact.size()==77,"Hand-calculated variable width:32+1+24 directory+8+1+7+4");
    require(encode(variable,43,short_row).size()==71&&encode(variable,44,nulls).size()==61,"Variable row directory distinguishes NULL, empty and shorter UTF-8");
    require(decode(variable,compact).row==row&&decode(variable,encode(variable,44,nulls)).row==nulls,"Variable round trip");
    auto legacy=schema(meta("JSON"));auto old=encode(legacy,42,row);require(old[0]=='{'&&decode(fixed,old).format==Format::Json&&decode(fixed,old).row==row,"Actual legacy JSON envelope recognized automatically");
    auto old_text=meta("JSON");old_text["columns"][2]={{"name","text"},{"type","TEXT"}};auto unbounded=schema(old_text);auto textrow=json::array({1,true,std::string(7000,'x')});require(decode(unbounded,encode(unbounded,1,textrow)).row==textrow,"Legacy TEXT without length remains readable");
}
void scalar_model(){
    std::mt19937_64 random(101026);const std::string words[]={"","a","中文","🙂","中🙂",std::string("a\0b",3)};
    for(auto f:{"JSON","FIXED","VARIABLE"}){auto metadata=meta(f);metadata["columns"].push_back(col("value","REAL"));auto s=schema(metadata);
        for(std::uint64_t i=1;i<=1600;++i){auto bits=random();std::int64_t integer=0;std::memcpy(&integer,&bits,8);auto row=json::array({integer,bool(i%2),words[i%6],static_cast<double>(static_cast<std::int64_t>(random()%200000)-100000)/7});
            for(std::size_t j=0;j<row.size();++j)if(random()%7==0)row[j]=nullptr;
            auto bytes=encode(s,i,row);auto out=decode(s,bytes);require(out.logical_id==i&&out.row==row&&out.format==s.format,"Seeded independent typed-row model preserves all values/NULL/LID");
        }
    }
    for(auto value:{std::numeric_limits<std::int64_t>::min(),std::int64_t(-1),std::int64_t(0),std::numeric_limits<std::int64_t>::max()}){auto s=schema(meta("VARIABLE"));auto row=json::array({value,nullptr,""});require(decode(s,encode(s,1,row)).row==row,"Full int64 range has no JSON number conversion loss");}
    auto m=meta("FIXED");m["columns"][0]["type"]="REAL";auto s=schema(m);auto bytes=encode(s,1,json::array({-0.0,true,"a"}));auto d=decode(s,bytes).row[0].get<double>();require(std::signbit(d),"REAL preserves IEEE754 signed zero");
}
void blobs(){
    for(auto f:{"FIXED","VARIABLE"}){auto m=meta(f);m["columns"].push_back(col("payload","BLOB",16777216));m["record_options"]={{"blob_ref_bytes",512}};auto s=schema(m);auto value=reference();auto row=json::array({1,true,"中",value});require(decode(s,encode(s,1,row)).row==row,"External BLOB descriptor preserved exactly, including options/stats/owner");
        auto oversized=value;oversized["$lob"]["padding"]=std::string(600,'x');row[3]=oversized;rejects([&]{encode(s,1,row);},"BLOB_REFERENCE_TOO_LARGE");
        row[3]={{"$blob","ABCD"}};rejects([&]{encode(s,1,row);},"TYPE_ERROR");row[3]=reference(16777217);rejects([&]{encode(s,1,row);},"VALUE_TOO_LONG");row[3]=reference();row[3]["$lob"]["root"]=-1;rejects([&]{encode(s,1,row);},"TYPE_ERROR");
        json nested=nullptr;for(int i=0;i<70;++i)nested=json::array({nested});row[3]=reference();row[3]["$lob"]["nested"]=nested;rejects([&]{encode(s,1,row);},"BLOB_REFERENCE_TOO_LARGE");
        row[3]=reference();row[3]["$lob"]["wide"]=json::array();for(int i=0;i<4096;++i)row[3]["$lob"]["wide"].push_back(nullptr);rejects([&]{encode(s,1,row);},"BLOB_REFERENCE_TOO_LARGE");
    }
    auto m=meta("FIXED");m["columns"].push_back(col("b1","BLOB",1));m["columns"].push_back(col("b2","BLOB",1));m["record_options"]={{"blob_ref_bytes",4096}};rejects([&]{schema(m);},"ROW_TOO_LARGE");
}
void boundaries(){
    auto m=meta("FIXED");m["columns"]=json::array({col("s","VARCHAR",2020)});rejects([&]{schema(m);},"ROW_TOO_LARGE");m["columns"][0]["length"]=2019;auto s=schema(m);std::string large;for(int i=0;i<2019;++i)large+="🙂";auto b=encode(s,1,json::array({large}));require(b.size()==8117&&decode(s,b).row[0]==large,"Worst-case4-byte UTF-8 fixed record fits before CREATE");
    m["record_format"]="VARIABLE";m["columns"][0]["length"]=1000000;s=schema(m);rejects([&]{encode(s,1,json::array({std::string(8200,'a')}));},"ROW_TOO_LARGE");
    m=meta("FIXED");m["columns"][2]["length"]=0;rejects([&]{schema(m);},"RECORD_LAYOUT");m=meta("VARIABLE");m["columns"]=json::array();for(int i=0;i<1024;++i)m["columns"].push_back(col(("c"+std::to_string(i)).c_str(),"BOOLEAN"));rejects([&]{schema(m);},"ROW_TOO_LARGE");
    auto normal=schema(meta("FIXED"));rejects([&]{encode(normal,0,json::array({1,true,"a"}));},"INVALID_ROW");rejects([&]{encode(normal,1,json::array({1,true,"中文测试"}));},"VALUE_TOO_LONG");rejects([&]{encode(normal,1,json::array({1,2,"a"}));},"TYPE_ERROR");rejects([&]{encode(normal,1,json::array({std::numeric_limits<std::uint64_t>::max(),true,"a"}));},"TYPE_ERROR");
    auto required=meta("FIXED");required["columns"][0]["not_null"]=true;auto not_null=schema(required);rejects([&]{encode(not_null,1,json::array({nullptr,true,"a"}));},"NOT_NULL_VIOLATION");
    for(const std::string& invalid:{std::string("\xc0\x80",2),std::string("\xed\xa0\x80",3),std::string("\xf4\x90\x80\x80",4),std::string("\xe4\xb8",2),std::string("\x80",1)})rejects([&]{encode(normal,1,json::array({1,true,invalid}));},"INVALID_UTF8");
    require(utf8_characters(std::string("a\0b",3))==3&&utf8_characters("中🙂")==2,"Character count matches SQL semantics including embedded NUL");
}
void damage(){
    for(auto f:{"FIXED","VARIABLE"}){auto s=schema(meta(f));auto bytes=encode(s,43,json::array({9,true,"中文"}));
        for(std::size_t i=0;i<bytes.size();++i){auto broken=bytes;broken[i]^=1;rejects([&]{decode(s,broken);});}
        for(std::size_t n=0;n<bytes.size();++n)rejects([&]{decode(s,bytes.substr(0,n));});
        auto renamed=meta(f);renamed["columns"][0]["name"]="renamed";auto different=schema(renamed);rejects([&]{decode(different,bytes);},"SCHEMA_MISMATCH");
        auto wrong=bytes;wrong[32]=static_cast<char>(128);seal(wrong);rejects([&]{decode(s,wrong);},"CORRUPT_RECORD");
        wrong=bytes;put(wrong,24,bytes.size()-1,4);seal(wrong);rejects([&]{decode(s,wrong);},"CORRUPT_RECORD");
        wrong=bytes;wrong[30]=1;seal(wrong);rejects([&]{decode(s,wrong);},"CORRUPT_RECORD");
        if(std::string(f)=="FIXED"){wrong=bytes;wrong[41]=2;seal(wrong);rejects([&]{decode(s,wrong);},"CORRUPT_RECORD");wrong=encode(s,1,json::array({nullptr,true,""}));wrong[33]=1;seal(wrong);rejects([&]{decode(s,wrong);},"CORRUPT_RECORD");}
        else{wrong=bytes;put(wrong,33,1,4);seal(wrong);rejects([&]{decode(s,wrong);},"CORRUPT_RECORD");wrong=bytes;put(wrong,33+8+4,100000,4);seal(wrong);rejects([&]{decode(s,wrong);},"CORRUPT_RECORD");}
    }
    auto s=schema(meta("JSON"));rejects([&]{decode(s,"{\"id\":-1,\"row\":[1,true,\"a\"]}");},"CORRUPT_RECORD");rejects([&]{decode(s,"{\"id\":1,\"row\":[1,4,\"a\"]}");},"CORRUPT_RECORD");
}
}
int main(){try{exact_layout();scalar_model();blobs();boundaries();damage();std::cout<<"PASS record_codec Stage1 "<<checks<<" checks: actual fixed/null bitmap/variable directory, UTF8 code points, int64, external BLOB refs, legacy and corruption\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL record_codec Stage1: "<<e.what()<<"\n";return 1;}}
