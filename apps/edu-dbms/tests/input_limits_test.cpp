#include "input_limits.hpp"
#include "interchange.hpp"
#include <iostream>
#include <stdexcept>
#ifdef _WIN32
#include <windows.h>
#endif
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& message){++checks;if(!value)throw std::runtime_error(message);}
template<class F>static void rejected(F action,const std::string& code,bool position=false){bool caught=false;try{action();}catch(const edudb::DbError& e){caught=e.code==code&&(!position||e.position>=0);}check(caught,"Expected bounded rejection "+code);}
int main(){
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
#endif
    try{
    const std::string allowed=std::string(64,'[')+"0"+std::string(64,']');check(edudb::input::parse_json(allowed).is_array(),"64 container levels remain valid");rejected([](){edudb::input::parse_json(std::string(65,'[')+"0"+std::string(65,']'));},"REQUEST_TOO_COMPLEX",true);
    const auto escaped=json{{"text",std::string(5000,'[')+"\\\"}中文\n"}};check(edudb::input::parse_json(escaped.dump())==escaped,"Delimiters and escaped quotes inside JSON strings do not count as structure");check(edudb::input::parse_json("\xef\xbb\xbf{\"text\":\"中文\"}")["text"]=="中文","JSON UTF8 BOM remains accepted");
    for(const auto& invalid:{"{\"a\":[1}","{\"a\":\"unterminated","{\"a\":1,}","{\"a\":1e9999}",""})rejected([&](){edudb::input::parse_json(invalid);},"INVALID_JSON");
    std::string nodes="[";for(int n=0;n<65535;++n){if(n)nodes+=',';nodes+='0';}nodes+=']';check(edudb::input::parse_json(nodes).size()==65535,"Exactly65536 JSON value nodes are accepted");nodes.insert(nodes.size()-1,",0");rejected([&](){edudb::input::parse_json(nodes);},"REQUEST_TOO_COMPLEX",true);
    json shape=0;for(int n=0;n<64;++n)shape=json::array({std::move(shape)});edudb::input::check_json_value(shape);check(true,"Native JSON depth64 uses iterative validation");shape=json::array({std::move(shape)});rejected([&](){edudb::input::check_json_value(shape);},"REQUEST_TOO_COMPLEX");
    json wide=json::array();for(int n=0;n<65536;++n)wide.push_back(0);rejected([&](){edudb::input::check_json_value(wide);},"REQUEST_TOO_COMPLEX");
    {json big=std::string(edudb::input::json_estimate_limit,'x');rejected([&](){edudb::input::check_json_value(big);},"REQUEST_TOO_COMPLEX");}
    auto small=edudb::interchange::parse_csv("\xef\xbb\xbfid,text\r\n1,\"中文\r\n引号\"\"内部\"\r\n2,\\N\r\n3,\"\\N\"\r\n4,\"\"\r\n");check(small.size()==5&&small[1][1].text=="中文\r\n引号\"内部"&&small[2][1].is_null&&!small[3][1].is_null&&small[3][1].text=="\\N"&&small[4][1].text.empty(),"CSV BOM/CRLF/quoted newlines/quoted versus unquoted NULL and empty string are preserved");
    check(edudb::interchange::parse_csv(std::string(1023,',')+'\n')[0].size()==1024,"1024 empty fields per row remain supported");rejected([](){edudb::interchange::parse_csv(std::string(1024,',')+'\n');},"IMPORT_RESOURCE",true);
    rejected([](){edudb::interchange::parse_csv(std::string(900000,'\n'));},"IMPORT_RESOURCE",true);
    {std::string empty;for(int n=0;n<1500;++n)empty+=std::string(999,',')+'\n';rejected([&](){edudb::interchange::parse_csv(empty);},"IMPORT_RESOURCE",true);}
    {std::string batch;for(int n=0;n<50000;++n)batch+="1,中文,\\N\n";auto rows=edudb::interchange::parse_csv(batch);check(rows.size()==50000&&rows.back().size()==3&&rows.back()[2].is_null,"Normal50K three-column CSV is inside the allocation estimate");}
    {std::string maximum_blob_hex(32u*1024u*1024u,'A');maximum_blob_hex+='\n';auto rows=edudb::interchange::parse_csv(maximum_blob_hex);check(rows.size()==1&&rows[0][0].text.size()==32u*1024u*1024u,"Full16MiB BLOB hex field is not silently reduced by the CSV estimate");}
    for(const auto& sql:{"SELECT '(((((((((((((((中文'", "SELECT \"((((identifier\"", "/* ((((((( */ SELECT 1 -- (((((\n"}){edudb::input::check_sql_delimiters(sql);check(true,"SQL quoted/comment parentheses ignored");}
    rejected([](){edudb::input::check_sql_delimiters("--中文\nSELECT "+std::string(128,'(')+"1");},"QUERY_TOO_COMPLEX",true);edudb::input::check_sql_delimiters("SELECT "+std::string(127,'(')+"1"+std::string(127,')'));check(true,"Existing127 parenthesis contract preserved");
    std::cout<<"PASS "<<checks<<" bounded JSON and CSV allocation/encoding/boundary checks\n";
    }catch(const std::exception&e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<"\n";return 1;}
}
