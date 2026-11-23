#include "interchange.hpp"
#include "input_limits.hpp"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <sstream>

namespace edudb::interchange {
void validate_utf8(const std::string& s) {
    for(std::size_t i=0;i<s.size();) {
        auto c=static_cast<unsigned char>(s[i]);
        if(c==0)throw DbError("INVALID_ENCODING","NUL bytes are not permitted in text interchange",static_cast<int>(i));
        if(c<0x80){++i;continue;}
        std::size_t n=0;std::uint32_t v=0,minimum=0;
        if(c>=0xc2&&c<=0xdf){n=2;v=c&31u;minimum=0x80;}
        else if(c>=0xe0&&c<=0xef){n=3;v=c&15u;minimum=0x800;}
        else if(c>=0xf0&&c<=0xf4){n=4;v=c&7u;minimum=0x10000;}
        else throw DbError("INVALID_ENCODING","Invalid UTF-8 leading byte",static_cast<int>(i));
        if(i+n>s.size())throw DbError("INVALID_ENCODING","Incomplete UTF-8 sequence",static_cast<int>(i));
        for(std::size_t j=1;j<n;++j){auto b=static_cast<unsigned char>(s[i+j]);if((b&0xc0)!=0x80)throw DbError("INVALID_ENCODING","Invalid UTF-8 continuation byte",static_cast<int>(i+j));v=(v<<6)|(b&63u);}
        if(v<minimum||v>0x10ffff||(v>=0xd800&&v<=0xdfff))throw DbError("INVALID_ENCODING","Invalid UTF-8 code point",static_cast<int>(i));
        i+=n;
    }
}
CsvRows parse_csv(const std::string& content) {
    if(content.size()>64u*1024u*1024u)throw DbError("IMPORT_RESOURCE","CSV input exceeds 64 MiB");
    validate_utf8(content);CsvRows rows;std::vector<Cell> row;std::string field;
    std::size_t i=content.compare(0,3,"\xef\xbb\xbf")==0?3:0;
    bool quoted=false,closed=false,was_quoted=false,started=false;
    std::size_t cells=0,estimated=content.size();
    auto charge=[&](std::size_t bytes){if(bytes>input::csv_parse_estimate_limit-estimated)throw DbError("IMPORT_RESOURCE","CSV parsed allocation estimate exceeds128 MiB;64 MiB content bound is separate (not RSS)",static_cast<int>(i));estimated+=bytes;};
    // Two bytes per decoded character cover geometric string growth; fixed
    // charges include Cell/vector metadata, spare capacity and allocator slack.
    auto append=[&](char c){charge(2);field+=c;};
    auto finish_field=[&](){if(row.size()>=1024||cells>=2000000)throw DbError("IMPORT_RESOURCE","CSV exceeds 1024 fields per row or 2 million cells",static_cast<int>(i));charge(128);++cells;const bool is_null=!was_quoted&&field=="\\N";row.push_back({std::move(field),is_null});field.clear();was_quoted=false;closed=false;started=false;};
    auto finish_row=[&](){if(rows.size()>=1000000)throw DbError("IMPORT_RESOURCE","CSV exceeds one million rows",static_cast<int>(i));charge(64);finish_field();rows.push_back(std::move(row));row.clear();};
    for(;i<content.size();++i){char c=content[i];
        if(quoted){if(c=='"'){if(i+1<content.size()&&content[i+1]=='"'){append('"');++i;}else {quoted=false;closed=true;}}else append(c);continue;}
        if(c==','){finish_field();continue;}
        if(c=='\n'||c=='\r'){finish_row();if(c=='\r'&&i+1<content.size()&&content[i+1]=='\n')++i;continue;}
        if(closed)throw DbError("CSV_PARSE","Unexpected character after closing quote",static_cast<int>(i));
        if(c=='"'){if(started||!field.empty())throw DbError("CSV_PARSE","Quote inside an unquoted field",static_cast<int>(i));quoted=true;was_quoted=true;started=true;}
        else {append(c);started=true;}
    }
    if(quoted)throw DbError("CSV_PARSE","Unterminated quoted CSV field",static_cast<int>(i));
    if(started||closed||!field.empty()||!row.empty())finish_row();
    return rows;
}
static std::string csv_field(const std::string& text) {
    validate_utf8(text);
    if(!text.empty()&&text!="\\N"&&text.find_first_of(",\"\r\n")==std::string::npos)return text;
    std::string out="\"";for(char c:text){out+=c;if(c=='"')out+='"';}return out+'"';
}
static std::string blob_hex(const json& value) {
    if(!value.is_object()||value.size()!=1||!value.contains("$blob")||!value["$blob"].is_string())throw DbError("TYPE_ERROR","BLOB interchange expects {$blob: hexadecimal}");
    auto hex=value["$blob"].get<std::string>();
    if(hex.size()%2||hex.find_first_not_of("0123456789abcdefABCDEF")!=std::string::npos)throw DbError("TYPE_ERROR","Invalid hexadecimal BLOB");
    std::transform(hex.begin(),hex.end(),hex.begin(),[](unsigned char c){return static_cast<char>(std::toupper(c));});return hex;
}
std::string encode_csv(const json& columns,const json& rows,bool header) {
    if(!columns.is_array()||!rows.is_array())throw DbError("INVALID_REQUEST","CSV columns and rows must be arrays");
    std::string out;
    if(header){for(std::size_t i=0;i<columns.size();++i){if(i)out+=',';auto name=columns[i].is_string()?columns[i].get<std::string>():columns[i].at("name").get<std::string>();out+=csv_field(name);}out+="\r\n";}
    for(const auto& row:rows){if(!row.is_array()||row.size()!=columns.size())throw DbError("INVALID_REQUEST","CSV row width does not match columns");
        for(std::size_t i=0;i<row.size();++i){if(i)out+=',';const auto& v=row[i];if(v.is_null())out+="\\N";else if(v.is_string())out+=csv_field(v.get<std::string>());else if(v.is_boolean())out+=v.get<bool>()?"true":"false";else if(v.is_number()){if(v.is_number_float()&&!std::isfinite(v.get<double>()))throw DbError("TYPE_ERROR","Non-finite REAL cannot be exported");out+=v.dump();}else out+=csv_field(blob_hex(v));}out+="\r\n";
    }return out;
}
std::string quote_identifier(const std::string& name) {
    validate_utf8(name);if(name.empty())throw DbError("INVALID_REQUEST","Identifier cannot be empty");
    std::string out="\"";for(char c:name){out+=c;if(c=='"')out+='"';}return out+'"';
}
std::string sql_literal(const json& value) {
    if(value.is_null())return "NULL";
    if(value.is_boolean())return value.get<bool>()?"TRUE":"FALSE";
    if(value.is_number()){if(value.is_number_float()&&!std::isfinite(value.get<double>()))throw DbError("TYPE_ERROR","Non-finite REAL cannot be exported");return value.dump();}
    if(value.is_string()){auto text=value.get<std::string>();validate_utf8(text);std::string out="'";for(char c:text){out+=c;if(c=='\'')out+='\'';}return out+'\'';}
    return "X'"+blob_hex(value)+"'";
}
static std::string identifier_list(const json& names) {
    std::string out;for(const auto& name:names){if(!out.empty())out+=", ";out+=quote_identifier(name.get<std::string>());}return out;
}
static std::string options_clause(const json& options) {
    if(!options.is_object()||options.empty())return "";
    std::string out=" WITH (";bool first=true;
    for(auto it=options.begin();it!=options.end();++it){if(!first)out+=", ";first=false;out+=quote_identifier(it.key())+"="+sql_literal(it.value());}return out+")";
}
std::string dump_table(const std::string& name,const json& table) {
    if(table.contains("cluster"))throw DbError("EXPORT_DEPENDENCY","Use Engine SQL export to include cluster definitions and ordered attach operations");
    const auto& columns=table.at("columns");std::string out="CREATE TABLE "+quote_identifier(name)+" (";
    for(std::size_t i=0;i<columns.size();++i){if(i)out+=", ";const auto& c=columns[i];auto type=c.at("type").get<std::string>();
        if(type!="INTEGER"&&type!="REAL"&&type!="BOOLEAN"&&type!="VARCHAR"&&type!="BLOB")throw DbError("TYPE_ERROR","Unsupported type in SQL export: "+type);
        out+=quote_identifier(c.at("name").get<std::string>())+" "+type;
        if(type=="VARCHAR"||type=="BLOB")out+='('+std::to_string(c.value("length",type=="BLOB"?16777216:255))+')';
        if(c.value("primary",false))out+=" PRIMARY KEY";else {if(c.value("not_null",false))out+=" NOT NULL";if(c.value("unique",false))out+=" UNIQUE";}
        if(c.contains("default")&&!c["default"].is_null())out+=" DEFAULT "+sql_literal(c["default"]);
    }
    for(const auto& idx:table.value("indexes",json::array())) {
        const auto index_name=idx.value("name",std::string());
        if(index_name.rfind("__",0)!=0)continue;
        const bool primary=idx.value("primary",false)||index_name.rfind("__pk_",0)==0;
        const bool unique=idx.value("unique",false);
        if(!primary&&!unique)continue;
        json names=idx.contains("columns")?idx["columns"]:json::array({idx.at("column")});
        bool inline_constraint=false;
        if(names.size()==1)for(const auto& c:columns)if(c["name"]==names[0])inline_constraint=primary?c.value("primary",false):c.value("primary",false)||c.value("unique",false);
        if(!inline_constraint)out+=std::string(", ")+(primary?"PRIMARY KEY":"UNIQUE")+" ("+identifier_list(names)+")";
    }
    out+=")";
    if(table.contains("record_format")){const auto name=table.at("record_format").get<std::string>();if(name!="JSON"&&name!="FIXED"&&name!="VARIABLE")throw DbError("RECORD_LAYOUT","Unknown record format in export");out+=" RECORD FORMAT "+name;if(table.contains("record_options"))out+=options_clause(table["record_options"]);}
    if(table.contains("organization")&&table["organization"].is_object()){const auto& org=table["organization"];auto method=org.value("method",std::string("HEAP"));if(method!="HEAP"){out+=" ORGANIZATION "+method+" ("+identifier_list(org.at("columns"))+")";if(org.contains("options"))out+=options_clause(org["options"]);}}
    out+=";\n";
    // One row per statement keeps exports readable and bounds parser statement size.
    for(const auto& row:table.value("rows",json::array())){out+="INSERT INTO "+quote_identifier(name)+" VALUES (";for(std::size_t i=0;i<row.size();++i){if(i)out+=", ";out+=sql_literal(row[i]);}out+=");\n";}
    for(const auto& idx:table.value("indexes",json::array())){
        // Primary indexes are recreated by the PRIMARY KEY column clause.
        if(idx.value("primary",false))continue;
        auto idx_name=idx.at("name").get<std::string>();
        if(idx_name.rfind("__",0)==0)continue;
        json names=idx.contains("columns")?idx["columns"]:json::array({idx.at("column")});
        out+="CREATE ";if(idx.value("unique",false))out+="UNIQUE ";out+="INDEX "+quote_identifier(idx_name)+" ON "+quote_identifier(name)+" USING "+idx.value("method",std::string("BPLUS"))+" ("+identifier_list(names)+")";
        if(idx.contains("include")&&!idx["include"].empty())out+=" INCLUDE ("+identifier_list(idx["include"])+")";
        if(idx.contains("options"))out+=options_clause(idx["options"]);
        out+=";\n";
    }return out;
}
std::vector<std::string> split_sql_script(const std::string& content) {
    if(content.size()>64u*1024u*1024u)throw DbError("IMPORT_RESOURCE","SQL import exceeds 64 MiB");
    validate_utf8(content);std::vector<std::string> out;std::string part;char quote=0;bool line=false,block=false,meaningful=false;
    auto finish=[&](){if(!meaningful)return;if(out.size()>=100000)throw DbError("IMPORT_RESOURCE","SQL import exceeds 100000 statements");out.push_back(std::move(part));part.clear();meaningful=false;};
    for(std::size_t i=content.compare(0,3,"\xef\xbb\xbf")==0?3:0;i<content.size();++i){char c=content[i],n=i+1<content.size()?content[i+1]:0;
        if(line){part+=c;if(c=='\n')line=false;continue;}
        if(block){part+=c;if(c=='*'&&n=='/'){part+=n;++i;block=false;}continue;}
        if(!quote&&c=='-'&&n=='-'){part+=c;part+=n;++i;line=true;continue;}
        if(!quote&&c=='/'&&n=='*'){part+=c;part+=n;++i;block=true;continue;}
        if((c=='\''||c=='"')&&(!quote||quote==c)){meaningful=true;part+=c;if(quote&&n==c){part+=n;++i;continue;}quote=quote?0:c;continue;}
        if(!quote&&c==';'){finish();part.clear();meaningful=false;}
        else {part+=c;if(!std::isspace(static_cast<unsigned char>(c)))meaningful=true;}
    }
    if(quote||block)throw DbError("SQL_PARSE","Unterminated quote or comment in SQL import");
    finish();
    return out;
}
}
