#include "record_codec.hpp"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <set>

namespace edudb::record {
namespace {
constexpr std::size_t header_bytes=32,checksum_bytes=4;
[[noreturn]]void bad(const char* message){throw Error("CORRUPT_RECORD",message);}
void put(std::string& b,std::size_t at,std::uint64_t n,unsigned width){for(unsigned i=0;i<width;++i)b[at+i]=static_cast<char>(n>>(8*i));}
std::uint64_t get(const std::string& b,std::size_t at,unsigned width){if(at>b.size()||width>b.size()-at)bad("Record field exceeds record boundary");std::uint64_t n=0;for(unsigned i=0;i<width;++i)n|=std::uint64_t(static_cast<unsigned char>(b[at+i]))<<(8*i);return n;}
std::uint32_t crc(const std::string& b,std::size_t size){std::uint32_t c=~0u;for(std::size_t i=0;i<size;++i){c^=static_cast<unsigned char>(b[i]);for(unsigned k=0;k<8;++k)c=(c>>1)^((c&1)?0xedb88320u:0);}return ~c;}
bool zeroes(const std::string& b,std::size_t at,std::size_t size){return std::all_of(b.begin()+at,b.begin()+at+size,[](char c){return c==0;});}
bool positive(const json& value){return value.is_number_unsigned()?value.get<std::uint64_t>()!=0:value.is_number_integer()&&value.get<std::int64_t>()>0;}
json bounded_json(const std::string& text){return json::parse(text,[](int depth,json::parse_event_t,json&){if(depth>64)bad("Record JSON nesting exceeds64 levels");return true;});}
void bounded_reference(const json& value,std::size_t maximum){
    std::vector<std::pair<const json*,unsigned>> pending{{&value,0}};std::size_t nodes=0,strings=0;
    auto append=[&](const json& child,unsigned depth){if(depth>64||nodes+pending.size()>=2048)throw Error("BLOB_REFERENCE_TOO_LARGE","External BLOB descriptor depth/node budget exceeded");pending.emplace_back(&child,depth);};
    while(!pending.empty()){auto current=pending.back();pending.pop_back();if(current.second>64||++nodes>2048)throw Error("BLOB_REFERENCE_TOO_LARGE","External BLOB descriptor depth/node budget exceeded");const auto& v=*current.first;
        if(v.is_string()){auto n=v.get_ref<const std::string&>().size();if(n>maximum-strings)throw Error("BLOB_REFERENCE_TOO_LARGE","External BLOB descriptor text exceeds cap");strings+=n;}
        else if(v.is_array())for(const auto& item:v)append(item,current.second+1);
        else if(v.is_object())for(auto it=v.begin();it!=v.end();++it){if(it.key().size()>maximum-strings)throw Error("BLOB_REFERENCE_TOO_LARGE","External BLOB descriptor keys exceed cap");strings+=it.key().size();append(it.value(),current.second+1);}
    }
}
std::size_t width(const Schema& s,const Column& c){
    if(c.type=="INTEGER"||c.type=="REAL")return 8;
    if(c.type=="BOOLEAN")return 1;
    if(c.type=="BLOB")return 4+s.blob_ref_bytes;
    if(!c.length)throw Error("RECORD_LAYOUT","FIXED VARCHAR/TEXT requires an explicit character capacity");
    if(c.length>(maximum_record_bytes-4)/4)throw Error("ROW_TOO_LARGE","FIXED VARCHAR worst-case four-byte UTF-8 capacity cannot fit one heap record");
    return 4+static_cast<std::size_t>(4*c.length);
}
std::size_t null_bytes(const Schema& s){return (s.columns.size()+7)/8;}
bool null_at(const std::string& b,std::size_t column){return (static_cast<unsigned char>(b[header_bytes+column/8])>>(column%8))&1;}
void check_value(const Column& c,const json& value){
    if(value.is_null()){if(c.not_null)throw Error("NOT_NULL_VIOLATION","NULL in non-null column: "+c.name);return;}
    if(c.type=="INTEGER"){
        if(value.is_number_unsigned()){if(value.get<std::uint64_t>()>std::uint64_t(std::numeric_limits<std::int64_t>::max()))throw Error("TYPE_ERROR","INTEGER exceeds int64");}
        else if(!value.is_number_integer())throw Error("TYPE_ERROR","INTEGER required: "+c.name);
    }else if(c.type=="REAL"){
        if(!value.is_number()||!std::isfinite(value.get<double>()))throw Error("TYPE_ERROR","Finite REAL required: "+c.name);
    }else if(c.type=="BOOLEAN"){
        if(!value.is_boolean())throw Error("TYPE_ERROR","BOOLEAN required: "+c.name);
    }else if(c.type=="VARCHAR"){
        if(!value.is_string())throw Error("TYPE_ERROR","VARCHAR required: "+c.name);
        auto count=utf8_characters(value.get_ref<const std::string&>());
        if(c.length&&count>c.length)throw Error("VALUE_TOO_LONG","VARCHAR character capacity exceeded: "+c.name);
    }else if(c.type=="BLOB"){
        if(!value.is_object()||!value.contains("$lob")||!value["$lob"].is_object()||!value.contains("owner")||!value.contains("bytes"))throw Error("TYPE_ERROR","Record codec accepts an external $lob reference, not an inline BLOB");
        for(auto field:{"owner","bytes"})if((!value[field].is_number_integer()&&!value[field].is_number_unsigned())||(value[field].is_number_integer()&&value[field].get<std::int64_t>()<0))throw Error("TYPE_ERROR","Invalid external BLOB identity/length");
        if(!positive(value["owner"])||value["owner"].get<std::uint64_t>()>=(std::uint64_t(1)<<48)||!value["$lob"].contains("root")||!positive(value["$lob"]["root"])||value["$lob"]["root"].get<std::uint64_t>()>=(std::uint64_t(1)<<48))throw Error("TYPE_ERROR","External BLOB needs valid positive owner/root page identities");
        if(c.length&&value["bytes"].get<std::uint64_t>()>c.length)throw Error("VALUE_TOO_LONG","BLOB payload byte limit exceeded");
        bounded_reference(value,maximum_record_bytes);
    }
}
void check_row(const Schema& s,const json& row){if(!row.is_array()||row.size()!=s.columns.size())throw Error("INVALID_ROW","Row width differs from schema");for(std::size_t i=0;i<row.size();++i)check_value(s.columns[i],row[i]);}
std::string scalar(const Schema& s,const Column& c,const json& v){
    if(c.type=="INTEGER"){std::string b(8,0);put(b,0,static_cast<std::uint64_t>(v.get<std::int64_t>()),8);return b;}
    if(c.type=="REAL"){static_assert(sizeof(double)==8&&std::numeric_limits<double>::is_iec559,"IEEE754 binary64 required");double d=v.get<double>();std::uint64_t bits=0;std::memcpy(&bits,&d,8);std::string b(8,0);put(b,0,bits,8);return b;}
    if(c.type=="BOOLEAN")return std::string(1,v.get<bool>()?1:0);
    if(c.type=="VARCHAR")return v.get<std::string>();
    bounded_reference(v,s.blob_ref_bytes);std::string bytes;try{bytes=v.dump();}catch(...){throw Error("TYPE_ERROR","External BLOB descriptor is not valid UTF-8 JSON");}if(bytes.size()>s.blob_ref_bytes)throw Error("BLOB_REFERENCE_TOO_LARGE","External BLOB descriptor exceeds explicit record_options.blob_ref_bytes; never truncated");return bytes;
}
json unscalar(const Schema& s,const Column& c,const std::string& bytes){
    if(c.type=="INTEGER"){if(bytes.size()!=8)bad("Invalid INTEGER width");auto bits=get(bytes,0,8);std::int64_t value=0;std::memcpy(&value,&bits,8);return value;}
    if(c.type=="REAL"){if(bytes.size()!=8)bad("Invalid REAL width");auto bits=get(bytes,0,8);double d=0;std::memcpy(&d,&bits,8);if(!std::isfinite(d))bad("Non-finite REAL bits");return d;}
    if(c.type=="BOOLEAN"){if(bytes.size()!=1||(bytes[0]!=0&&bytes[0]!=1))bad("Invalid BOOLEAN encoding");return bytes[0]==1;}
    if(c.type=="VARCHAR")return bytes;
    if(bytes.size()>s.blob_ref_bytes)bad("External BLOB descriptor exceeds schema cap");
    try{return bounded_json(bytes);}catch(...){bad("Malformed external BLOB descriptor JSON");}
}
void decoded_value(const Schema& s,std::size_t column,const json& value){try{check_value(s.columns[column],value);}catch(const Error&){bad("Record value violates its schema");}}
}
const char* format_name(Format f)noexcept{switch(f){case Format::Json:return "JSON";case Format::Fixed:return "FIXED";case Format::Variable:return "VARIABLE";}return "INVALID";}
Format parse_format(const std::string& name){if(name=="JSON")return Format::Json;if(name=="FIXED")return Format::Fixed;if(name=="VARIABLE")return Format::Variable;throw Error("RECORD_LAYOUT","record_format must be JSON/FIXED/VARIABLE");}
std::size_t utf8_characters(const std::string& text){
    std::size_t count=0;
    for(std::size_t i=0;i<text.size();){auto first=static_cast<unsigned char>(text[i]);unsigned n=first<0x80?1:first>=0xc2&&first<=0xdf?2:first>=0xe0&&first<=0xef?3:first>=0xf0&&first<=0xf4?4:0;
        if(!n||n>text.size()-i)throw Error("INVALID_UTF8","Invalid UTF-8 leading byte or truncated code point");
        for(unsigned j=1;j<n;++j)if((static_cast<unsigned char>(text[i+j])&0xc0)!=0x80)throw Error("INVALID_UTF8","Invalid UTF-8 continuation byte");
        if(n>=3){auto second=static_cast<unsigned char>(text[i+1]);if((first==0xe0&&second<0xa0)||(first==0xed&&second>=0xa0)||(first==0xf0&&second<0x90)||(first==0xf4&&second>=0x90))throw Error("INVALID_UTF8","Overlong, surrogate or out-of-range UTF-8 code point");}
        i+=n;++count;
    }
    return count;
}
Schema schema(const json& metadata){
    Schema s;if(!metadata.is_object()||!metadata.contains("columns")||!metadata["columns"].is_array()||metadata["columns"].empty()||metadata["columns"].size()>1024)throw Error("RECORD_LAYOUT","Schema needs 1..1024 columns");
    if(metadata.contains("record_format")){if(!metadata["record_format"].is_string())throw Error("RECORD_LAYOUT","record_format must be a string");s.format=parse_format(metadata["record_format"].get<std::string>());}
    auto options=metadata.value("record_options",json::object());if(!options.is_object())throw Error("RECORD_LAYOUT","record_options must be an object");
    for(auto it=options.begin();it!=options.end();++it)if(it.key()!="blob_ref_bytes")throw Error("RECORD_LAYOUT","Unknown record_options field");
    if(options.contains("blob_ref_bytes")){const auto& n=options["blob_ref_bytes"];if(!n.is_number_integer()||n.get<std::int64_t>()<16||n.get<std::uint64_t>()>4096)throw Error("RECORD_LAYOUT","blob_ref_bytes must be 16..4096");s.blob_ref_bytes=n.get<std::size_t>();}
    std::set<std::string> names;json signature=json::array();
    for(const auto& column:metadata["columns"]){
        if(!column.is_object()||!column.contains("name")||!column["name"].is_string()||!column.contains("type")||!column["type"].is_string())throw Error("RECORD_LAYOUT","Malformed column definition");
        Column c;c.name=column["name"].get<std::string>();c.type=column["type"].get<std::string>();utf8_characters(c.name);if(c.name.empty()||!names.insert(c.name).second)throw Error("RECORD_LAYOUT","Duplicate or empty column name");if(c.type=="TEXT")c.type="VARCHAR";
        if(c.type!="INTEGER"&&c.type!="REAL"&&c.type!="BOOLEAN"&&c.type!="VARCHAR"&&c.type!="BLOB")throw Error("RECORD_LAYOUT","Unsupported record type");
        if(column.contains("length")){const auto& n=column["length"];if(!n.is_number_integer()||n.get<std::int64_t>()<0||n.get<std::uint64_t>()>std::numeric_limits<std::uint32_t>::max())throw Error("RECORD_LAYOUT","Column length is invalid");c.length=n.get<std::uint64_t>();}
        if(column.contains("not_null")&&!column["not_null"].is_boolean())throw Error("RECORD_LAYOUT","not_null must be BOOLEAN");
        c.not_null=column.value("not_null",false);signature.push_back({{"name",c.name},{"type",c.type},{"length",c.length},{"not_null",c.not_null}});s.columns.push_back(std::move(c));
    }
    auto text=json{{"columns",signature},{"blob_ref_bytes",s.blob_ref_bytes}}.dump();s.fingerprint=14695981039346656037ull;for(unsigned char c:text){s.fingerprint^=c;s.fingerprint*=1099511628211ull;}
    validate_layout(s);return s;
}
void validate_layout(const Schema& s){
    if(s.columns.empty()||s.columns.size()>1024)throw Error("RECORD_LAYOUT","Invalid column count");
    if(s.blob_ref_bytes<16||s.blob_ref_bytes>4096)throw Error("RECORD_LAYOUT","Invalid external BLOB reference capacity");
    if(s.format==Format::Json)return;
    std::size_t bytes=header_bytes+null_bytes(s)+checksum_bytes;
    if(s.format==Format::Fixed){for(const auto& c:s.columns){auto n=width(s,c);if(n>maximum_record_bytes-bytes)throw Error("ROW_TOO_LARGE","FIXED maximum record cannot fit one 8KiB heap slot");bytes+=n;}}
    else if(s.format==Format::Variable){if(s.columns.size()>(maximum_record_bytes-bytes)/8)throw Error("ROW_TOO_LARGE","VARIABLE offset directory cannot fit one heap slot");}
    else throw Error("RECORD_LAYOUT","Invalid record format");
}
json layout(const Schema& s){
    validate_layout(s);json fields=json::array();std::size_t at=header_bytes+null_bytes(s);
    if(s.format==Format::Fixed)for(const auto& c:s.columns){auto n=width(s,c);fields.push_back({{"name",c.name},{"type",c.type},{"offset",at},{"bytes",n},{"varchar_units","Unicode code points; reserve 4 UTF-8 bytes per point"}});at+=n;}
    return {{"format",format_name(s.format)},{"binary_header_bytes",s.format==Format::Json?0:header_bytes},{"null_bitmap_bytes",s.format==Format::Json?0:null_bytes(s)},{"variable_directory_bytes",s.format==Format::Variable?s.columns.size()*8:0},{"fixed_record_bytes",s.format==Format::Fixed?json(at+checksum_bytes):json(nullptr)},{"checksum_bytes",s.format==Format::Json?0:checksum_bytes},{"maximum_slot_record_bytes",maximum_record_bytes},{"blob_ref_bytes",s.blob_ref_bytes},{"schema_fingerprint",s.fingerprint},{"schema_fingerprint_stored",s.format!=Format::Json},{"fields",fields}};
}
std::string encode(const Schema& s,std::uint64_t logical_id,const json& row){
    if(!logical_id)throw Error("INVALID_ROW","Logical row id must be positive");
    validate_layout(s);check_row(s,row);
    if(s.format==Format::Json){auto text=json{{"id",logical_id},{"row",row}}.dump();if(text.size()>maximum_record_bytes)throw Error("ROW_TOO_LARGE","JSON row cannot fit one heap slot");return text;}
    const auto nb=null_bytes(s);std::size_t directory=s.format==Format::Variable?s.columns.size()*8:0;std::string bytes(header_bytes+nb+directory,0);std::memcpy(bytes.data(),"EDRC",4);bytes[4]=1;bytes[5]=static_cast<char>(s.format);put(bytes,6,s.columns.size(),2);put(bytes,8,logical_id,8);put(bytes,16,s.fingerprint,8);put(bytes,28,nb,2);
    for(std::size_t i=0;i<row.size();++i){
        const auto& c=s.columns[i];const auto& value=row[i];if(value.is_null())bytes[header_bytes+i/8]=static_cast<char>(static_cast<unsigned char>(bytes[header_bytes+i/8])|(1u<<(i%8)));
        if(s.format==Format::Fixed){auto n=width(s,c);if(n>maximum_record_bytes-checksum_bytes-bytes.size())throw Error("ROW_TOO_LARGE","FIXED row exceeds slot");auto begin=bytes.size();bytes.resize(begin+n,0);
            if(!value.is_null()){auto data=scalar(s,c,value);if(c.type=="VARCHAR"||c.type=="BLOB"){if(data.size()>n-4)throw Error("ROW_TOO_LARGE","Fixed field capacity exceeded");put(bytes,begin,data.size(),4);std::copy(data.begin(),data.end(),bytes.begin()+begin+4);}else std::copy(data.begin(),data.end(),bytes.begin()+begin);}
        }else if(!value.is_null()){
            auto data=scalar(s,c,value);if(data.size()>maximum_record_bytes-checksum_bytes-bytes.size())throw Error("ROW_TOO_LARGE","VARIABLE row cannot fit one heap slot");put(bytes,header_bytes+nb+i*8,bytes.size(),4);put(bytes,header_bytes+nb+i*8+4,data.size(),4);bytes+=data;
        }
    }
    auto total=bytes.size()+checksum_bytes;put(bytes,24,total,4);bytes.resize(total,0);put(bytes,total-4,crc(bytes,total-4),4);return bytes;
}
Decoded decode(const Schema& s,const std::string& bytes){
    validate_layout(s);
    if(bytes.empty()||bytes.size()>maximum_record_bytes)bad("Record exceeds heap slot bounds");
    if(bytes.size()<4||bytes.compare(0,4,"EDRC")){
        try{auto value=bounded_json(bytes);if(!value.is_object()||!value.contains("id")||!positive(value["id"])||!value.contains("row"))bad("Malformed legacy JSON record");auto row=value["row"];check_row(s,row);return {value["id"].get<std::uint64_t>(),std::move(row),Format::Json};}catch(const Error&){bad("Legacy JSON record violates structure/schema");}catch(...){bad("Invalid legacy JSON record");}
    }
    if(bytes.size()<header_bytes+checksum_bytes||bytes[4]!=1||get(bytes,30,2))bad("Invalid binary record header/version/reserved bits");
    auto format=static_cast<Format>(static_cast<unsigned char>(bytes[5]));if(format!=Format::Fixed&&format!=Format::Variable)bad("Invalid binary record format");
    if(get(bytes,6,2)!=s.columns.size()||get(bytes,16,8)!=s.fingerprint)throw Error("SCHEMA_MISMATCH","Binary record schema fingerprint differs; schema changes require transactionally rewriting records");
    auto logical_id=get(bytes,8,8);if(!logical_id||get(bytes,24,4)!=bytes.size()||get(bytes,28,2)!=null_bytes(s)||get(bytes,bytes.size()-4,4)!=crc(bytes,bytes.size()-4))bad("Invalid binary record length/id/checksum");
    const auto nb=null_bytes(s);if(bytes.size()<header_bytes+nb+checksum_bytes)bad("Truncated NULL bitmap");
    if(s.columns.size()%8&&(static_cast<unsigned char>(bytes[header_bytes+nb-1])>>(s.columns.size()%8)))bad("Nonzero unused NULL bitmap bits");
    const auto end=bytes.size()-checksum_bytes;std::size_t at=header_bytes+nb;
    if(format==Format::Variable){if(s.columns.size()>(end-at)/8)bad("Truncated VARIABLE offset directory");at+=s.columns.size()*8;}
    json row=json::array();
    for(std::size_t i=0;i<s.columns.size();++i){const auto& c=s.columns[i];json value=nullptr;
        if(format==Format::Fixed){std::size_t n=0;try{n=width(s,c);}catch(const Error&){bad("Invalid FIXED schema width");}if(n>end-at)bad("Truncated FIXED field");
            if(null_at(bytes,i)){if(!zeroes(bytes,at,n))bad("Nonzero NULL field padding");}
            else if(c.type=="VARCHAR"||c.type=="BLOB"){auto length=get(bytes,at,4);if(length>n-4||!zeroes(bytes,at+4+length,n-4-length))bad("Invalid fixed variable-field length/padding");value=unscalar(s,c,bytes.substr(at+4,length));}
            else value=unscalar(s,c,bytes.substr(at,n));
            at+=n;
        }else{auto offset=get(bytes,header_bytes+nb+i*8,4),length=get(bytes,header_bytes+nb+i*8+4,4);
            if(null_at(bytes,i)){if(offset||length)bad("Nonzero NULL variable directory entry");}
            else{if(offset!=at||length>end-at)bad("Invalid/noncontiguous variable field offset/length");value=unscalar(s,c,bytes.substr(at,length));at+=length;}
        }
        decoded_value(s,i,value);row.push_back(std::move(value));
    }
    if(at!=end)bad("Unconsumed bytes after last record field");
    return {logical_id,std::move(row),format};
}
} // namespace edudb::record
