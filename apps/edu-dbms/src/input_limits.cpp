#include "input_limits.hpp"
#include <vector>

namespace edudb::input {
namespace {
struct JsonBudget {
    std::size_t nodes=0,estimated=0;
    void charge(std::size_t n,int position){
        if(n>json_estimate_limit-estimated)throw DbError("REQUEST_TOO_COMPLEX","JSON allocation estimate exceeds96 MiB (not an RSS limit)",position);
        estimated+=n;
    }
    void node(int position){
        if(nodes>=json_node_limit)throw DbError("REQUEST_TOO_COMPLEX","JSON exceeds65536 value/key nodes",position);
        ++nodes;charge(128,position);
    }
};
bool space(char c){return c==' '||c=='\t'||c=='\r'||c=='\n';}
}
void check_json_text(const std::string& text){
    if(text.size()>64u*1024u*1024u)throw DbError("REQUEST_TOO_LARGE","Encoded JSON exceeds64 MiB");
    JsonBudget budget;budget.charge(text.size(),0);std::vector<char> containers;
    containers.reserve(json_depth_limit);
    for(std::size_t i=text.compare(0,3,"\xef\xbb\xbf")==0?3:0;i<text.size();){
        const char c=text[i];const int position=static_cast<int>(i);
        if(space(c)||c==','||c==':'){++i;continue;}
        if(c=='"'){
            budget.node(position);++i;bool closed=false;
            while(i<text.size()){
                if(text[i]=='\\'){if(i+1==text.size())break;i+=2;}
                else if(text[i++]=='"'){closed=true;break;}
            }
            if(!closed)throw DbError("INVALID_JSON","Unterminated JSON string",position);
            continue;
        }
        if(c=='{'||c=='['){
            budget.node(position);
            if(containers.size()>=json_depth_limit)throw DbError("REQUEST_TOO_COMPLEX","JSON container depth exceeds64",position);
            containers.push_back(c);++i;continue;
        }
        if(c=='}'||c==']'){
            if(containers.empty()||containers.back()!=(c=='}'?'{':'['))throw DbError("INVALID_JSON","Unmatched JSON container",position);
            containers.pop_back();++i;continue;
        }
        budget.node(position);
        while(i<text.size()&&!space(text[i])&&text[i]!=','&&text[i]!=':'&&text[i]!='['&&text[i]!=']'&&text[i]!='{'&&text[i]!='}'&&text[i]!='"')++i;
    }
    if(!containers.empty())throw DbError("INVALID_JSON","Unterminated JSON container",static_cast<int>(text.size()));
}
json parse_json(const std::string& text){
    check_json_text(text);
    try{return json::parse(text);}
    catch(const json::parse_error& error){throw DbError("INVALID_JSON","Request body must be valid JSON",error.byte?static_cast<int>(error.byte-1):-1);}
    catch(const json::exception&){throw DbError("INVALID_JSON","Request body must contain representable JSON values");}
}
void check_json_value(const json& value){
    JsonBudget budget;
    struct Frame {const json* value;std::size_t depth;json::const_iterator next,end;};
    std::vector<Frame> stack;stack.reserve(json_depth_limit+1);
    auto enter=[&](const json& node,std::size_t depth){
        budget.node(-1);
        if(node.is_string())budget.charge(node.get_ref<const std::string&>().size(),-1);
        if(node.is_binary())budget.charge(node.get_binary().size(),-1);
        if(node.is_structured()){
            if(depth>json_depth_limit)throw DbError("REQUEST_TOO_COMPLEX","JSON container depth exceeds64");
            stack.push_back({&node,depth,node.cbegin(),node.cend()});
        }
    };
    enter(value,value.is_structured()?1:0);
    while(!stack.empty()){
        auto& frame=stack.back();
        if(frame.next==frame.end){stack.pop_back();continue;}
        const json& child=*frame.next;
        if(frame.value->is_object()){budget.node(-1);budget.charge(frame.next.key().size(),-1);}
        ++frame.next;const auto depth=frame.depth+(child.is_structured()?1:0);enter(child,depth);
    }
}
void check_sql_delimiters(const std::string& text){
    if(text.size()>sql_byte_limit)throw DbError("QUERY_TOO_COMPLEX","Native SQL input exceeds64 MiB",static_cast<int>(sql_byte_limit));
    std::size_t depth=0;char quote=0;bool line=false,block=false;
    for(std::size_t i=0;i<text.size();++i){const char c=text[i],next=i+1<text.size()?text[i+1]:0;
        if(line){if(c=='\n')line=false;continue;}
        if(block){if(c=='*'&&next=='/'){block=false;++i;}continue;}
        if(quote){if(c==quote){if(next==quote)++i;else quote=0;}continue;}
        if(c=='-'&&next=='-'){line=true;++i;continue;}
        if(c=='/'&&next=='*'){block=true;++i;continue;}
        if(c=='\''||c=='"'){quote=c;continue;}
        if(c=='('){if(depth>=sql_delimiter_limit)throw DbError("QUERY_TOO_COMPLEX","SQL parenthesis depth exceeds127",static_cast<int>(i));++depth;}
        else if(c==')'&&depth)--depth;
    }
}
json describe(){return {{"json_container_depth",json_depth_limit},{"json_value_and_key_nodes",json_node_limit},{"json_allocation_estimate_bytes",json_estimate_limit},{"native_sql_bytes",sql_byte_limit},{"sql_delimiter_depth",sql_delimiter_limit},{"sql_query_nesting",sql_query_nesting_limit},{"sql_ast_nodes",sql_ast_node_limit},{"sql_ast_walk_depth",sql_ast_walk_depth_limit},{"sql_import_ast_nodes",sql_ast_node_limit},{"sql_import_parse_estimate_bytes",sql_import_ast_estimate_limit},{"sql_lexer_tokens",500000},{"sql_expression_depth",128},{"csv_input_bytes",64u*1024u*1024u},{"csv_parse_estimate_bytes",csv_parse_estimate_limit},{"allocation_scope","conservative allocation estimates, not process RSS or physical memory measurements"}};}
}
