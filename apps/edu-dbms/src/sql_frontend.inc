std::string upper(std::string s) { for (char& c:s) if (c>='a'&&c<='z') c=char(c-'a'+'A'); return s; }
std::string lower(std::string s) { for (char& c:s) if (c>='A'&&c<='Z') c=char(c-'A'+'a'); return s; }
[[noreturn]] void fail(const std::string& code,const std::string& message,int p=-1) { throw DbError(code,message,p); }
struct QueryLimits {
    std::uint64_t work_units=20000000,materialized_rows=1000000,materialized_bytes=64u*1024u*1024u;
    std::uint64_t result_rows=100000,result_bytes=16u*1024u*1024u,timeout_ms=10000;
    json describe() const {return {{"work_units",work_units},{"materialized_rows",materialized_rows},
        {"lexer_tokens",500000},{"expression_depth",128},{"sql_input",input::describe()},
        {"materialized_bytes",materialized_bytes},{"result_rows",result_rows},{"result_bytes",result_bytes},
        {"timeout_ms",timeout_ms},{"memory_scope","cumulative JSON allocation estimate, not resident process memory"}};}
};
std::uint64_t json_allocation_estimate(const json& value) {
    std::uint64_t bytes=sizeof(json);
    if(value.is_array()) {
        bytes+=sizeof(std::vector<json>);
        for(const auto& element:value)bytes+=json_allocation_estimate(element);
    } else if(value.is_object()) {
        bytes+=sizeof(json::object_t);
        for(auto it=value.begin();it!=value.end();++it)
            bytes+=sizeof(std::string)+it.key().size()+1+4*sizeof(void*)+json_allocation_estimate(it.value());
    } else if(value.is_string())bytes+=sizeof(std::string)+value.get_ref<const std::string&>().size()+1;
    return bytes;
}
struct QueryBudget {
    const QueryLimits& limits;
    const Clock::time_point started=Clock::now();
    std::uint64_t work_units=0,materialized_rows=0,materialized_bytes=0,result_rows=0,result_bytes=0;
    std::shared_ptr<const std::atomic<bool>> cancellation;
    explicit QueryBudget(const QueryLimits& configured):limits(configured){}
    void check_time() const {
        if(cancellation&&cancellation->load())fail("DEADLOCK","Transaction cancelled by lock policy; rollback required");
        if(Clock::now()-started>std::chrono::milliseconds(limits.timeout_ms))
            fail("QUERY_LIMIT_EXCEEDED","SELECT execution time exceeds "+std::to_string(limits.timeout_ms)+" ms");
    }
    void work() {
        if(work_units>=limits.work_units)fail("QUERY_LIMIT_EXCEEDED","SELECT work budget exceeds "+std::to_string(limits.work_units)+" units");
        ++work_units;
        if((work_units&1023u)==0)check_time();
    }
    void materialize(const json& row) {
        work();
        // Include spare vector capacity/bookkeeping rather than claiming this
        // counter is an exact allocator or RSS measurement.
        auto bytes=json_allocation_estimate(row)+32;
        if(materialized_rows>=limits.materialized_rows||bytes>limits.materialized_bytes-materialized_bytes)
            fail("QUERY_LIMIT_EXCEEDED","SELECT materialization exceeds "+std::to_string(limits.materialized_rows)+
                 " cumulative rows or "+std::to_string(limits.materialized_bytes)+" estimated bytes");
        ++materialized_rows;materialized_bytes+=bytes;
    }
    void output(const json& row) {
        // HTTP transports INTEGER cells as quoted decimal strings; allow two
        // extra bytes per cell as well as the row separator.
        auto bytes=std::uint64_t(row.dump().size())+2*row.size()+1;
        if(result_rows>=limits.result_rows||bytes>limits.result_bytes-result_bytes)
            fail("QUERY_LIMIT_EXCEEDED","SELECT output exceeds "+std::to_string(limits.result_rows)+
                 " rows or "+std::to_string(limits.result_bytes)+" estimated wire bytes; use LIMIT");
        ++result_rows;result_bytes+=bytes;materialize(row);
    }
    json snapshot() const {return {{"work_units",work_units},{"materialized_rows",materialized_rows},
        {"materialized_bytes_estimate",materialized_bytes},{"result_rows",result_rows},
        {"result_bytes_estimate",result_bytes},{"limits",limits.describe()}};}
};
thread_local QueryBudget* active_query_budget=nullptr;
struct QueryBudgetScope {
    QueryBudget* previous;
    explicit QueryBudgetScope(QueryBudget& current):previous(active_query_budget){active_query_budget=&current;}
    ~QueryBudgetScope(){active_query_budget=previous;}
};
std::function<void()> query_page_progress(QueryBudget& budget){return [&budget]{budget.work();budget.check_time();};}
void query_work(){if(active_query_budget)active_query_budget->work();}
void query_materialize(const json& row){if(active_query_budget)active_query_budget->materialize(row);}
thread_local bool internal_query_output=false;
struct InternalQueryOutputScope{bool previous;explicit InternalQueryOutputScope(bool internal):previous(internal_query_output){internal_query_output=internal;}~InternalQueryOutputScope(){internal_query_output=previous;}};
void query_output(const json& row){if(active_query_budget){if(internal_query_output)active_query_budget->materialize(row);else active_query_budget->output(row);}}
struct Token { enum Kind {Ident,Number,String,Symbol,End} kind; std::string text; int pos; bool quoted=false; };

std::vector<Token> lex(const std::string& s) {
    input::check_sql_delimiters(s);
    std::vector<Token> out;
    auto emit=[&](Token token){if(out.size()>=500000)fail("QUERY_TOO_COMPLEX","SQL token count exceeds 500000",token.pos);out.push_back(std::move(token));};
    for (std::size_t i=s.compare(0,3,"\xef\xbb\xbf")==0?3:0;i<s.size();) {
        unsigned char c=static_cast<unsigned char>(s[i]);
        if (c==' '||c=='\t'||c=='\r'||c=='\n') { ++i; continue; }
        if (s.compare(i,2,"--")==0) { while(i<s.size()&&s[i]!='\n') ++i; continue; }
        if (s.compare(i,2,"/*")==0) { auto e=s.find("*/",i+2); if(e==std::string::npos) fail("SYNTAX_ERROR","Unterminated comment",int(i)); i=e+2; continue; }
        int start=int(i);
        if (c=='\''||c=='\"') {
            char q=s[i++]; std::string value; bool done=false;
            while(i<s.size()) {
                if(s[i]==q) { ++i; if(i<s.size()&&s[i]==q) { value+=q; ++i; } else {done=true;break;} }
                else value+=s[i++];
            }
            if(!done) fail("SYNTAX_ERROR","Unterminated quoted token",start);
            emit({q=='\''?Token::String:Token::Ident,value,start,q=='\"'}); continue;
        }
        if ((c>='0'&&c<='9')||(c=='.'&&i+1<s.size()&&s[i+1]>='0'&&s[i+1]<='9')) {
            bool dot=false;
            while(i<s.size()&&((s[i]>='0'&&s[i]<='9')||(!dot&&s[i]=='.'))) { if(s[i]=='.') dot=true; ++i; }
            if(i<s.size()&&(s[i]=='e'||s[i]=='E')) { ++i; if(i<s.size()&&(s[i]=='+'||s[i]=='-')) ++i; auto b=i; while(i<s.size()&&s[i]>='0'&&s[i]<='9') ++i; if(b==i) fail("SYNTAX_ERROR","Invalid numeric exponent",start); }
            emit({Token::Number,s.substr(start,i-start),start}); continue;
        }
        if ((c>='A'&&c<='Z')||(c>='a'&&c<='z')||c=='_'||c>=128) {
            ++i; while(i<s.size()) { unsigned char d=static_cast<unsigned char>(s[i]); if(!((d>='A'&&d<='Z')||(d>='a'&&d<='z')||(d>='0'&&d<='9')||d=='_'||d>=128)) break; ++i; }
            emit({Token::Ident,lower(s.substr(start,i-start)),start}); continue;
        }
        std::string op(1,s[i++]);
        if(i<s.size()&&((op=="<"&&(s[i]=='='||s[i]=='>'))||(op==">"&&s[i]=='=')||(op=="!"&&s[i]=='='))) op+=s[i++];
        if(std::string("(),.;+-*/=<>!").find(char(c))==std::string::npos||op=="!") fail("SYNTAX_ERROR","Unexpected character",start);
        emit({Token::Symbol,op,start});
    }
    out.push_back({Token::End,"",int(s.size())}); return out;
}

struct Statement;
thread_local std::size_t* active_parser_nodes=nullptr;
struct ParserNodeScope {
    std::size_t nodes=0;std::size_t* previous;
    ParserNodeScope():previous(active_parser_nodes){active_parser_nodes=&nodes;}
    ~ParserNodeScope(){active_parser_nodes=previous;}
};
struct Expr {
    enum Kind {Literal,Column,Star,Binary,Unary,IsNull,Function,Parameter,Subquery} kind=Literal;
    std::string text, qualifier, type="NULL";
    json value=nullptr;
    std::vector<std::shared_ptr<Expr>> args;
    std::size_t depth=1,graph_depth=1;
    int bound=-1, pos=-1,outer_depth=0;
    std::shared_ptr<Statement> query;json subquery_plan=nullptr;
};
using E = std::shared_ptr<Expr>;
E expr(Expr::Kind k,const std::string& text="",int pos=-1) {
    if(active_parser_nodes){if(*active_parser_nodes>=input::sql_ast_node_limit)fail("QUERY_TOO_COMPLEX","SQL AST exceeds250000 expression nodes",pos);++*active_parser_nodes;}
    auto e=std::make_shared<Expr>();e->kind=k;e->text=text;e->pos=pos;return e;
}
constexpr std::size_t max_expression_depth=128;
void set_arguments(const E& e,std::vector<E> arguments) {
    std::size_t depth=1,graph_depth=1;
    for(const auto& argument:arguments){depth=std::max(depth,argument->depth+1);graph_depth=std::max(graph_depth,argument->graph_depth+1);}
    if(e->pos<0)for(auto argument=arguments.rbegin();argument!=arguments.rend();++argument)if((*argument)->pos>=0){e->pos=(*argument)->pos;break;}
    if(depth>max_expression_depth)fail("QUERY_TOO_COMPLEX","Expression depth exceeds 128",e->pos);
    const auto graph_limit=input::sql_ast_walk_depth_limit-(active_parser_nodes?1:0);
    if(graph_depth>graph_limit)fail("QUERY_TOO_COMPLEX","Constructed AST height including query containers exceeds160",e->pos);
    e->depth=depth;e->graph_depth=graph_depth;e->args=std::move(arguments);
}
struct SelectItem { E value; std::string alias; };
struct Source { std::string table,alias,index_hint; bool force_scan=false; };
struct SortItem { E value; bool desc=false; };
struct Statement {
    std::string kind,table,newname,index,column,action,cluster_name;
    json cluster_columns=json::array();
    bool if_exists=false,if_not_exists=false,unique=false,explain=false,analyze=false,distinct=false;
    json columns=json::array(),table_constraints=json::array();
    json index_columns=json::array(),include_columns=json::array(),options=json::object();
    json organization={{"method","HEAP"},{"columns",json::array()},{"options",json::object()}};
    std::string method="BPLUS",join_hint;
    std::vector<std::string> names;
    std::vector<std::vector<E>> values;
    std::vector<std::pair<std::string,E>> assignments;
    std::vector<SelectItem> items;
    std::vector<Source> sources;
    std::vector<std::string> read_tables;
    E where,on,having;
    std::vector<std::pair<E,std::size_t>> join_conditions;
    std::string record_format;
    json record_options=json::object();
    std::vector<std::string> join_kinds;
    std::string set_operation;bool set_all=false;
    std::shared_ptr<Statement> set_left,set_right,query_definition;
    std::string definition_sql;
    std::vector<E> group;
    std::vector<SortItem> order;
    std::size_t limit=std::numeric_limits<std::size_t>::max(),offset=0;
    // Input-only accounting, never serialized as table/view schema metadata.
    std::size_t input_ast_nodes=0,input_ast_estimated_bytes=0,input_graph_depth=1;
};

// These cached heights are computed at each parser construction edge. This
// prevents a rejected cross-query tree from already owning a long recursive
// shared_ptr destruction chain. The independent iterative check below remains
// an additional check, rather than the first line of defense.
void finish_statement_graph(Statement& s,int position){
    std::size_t height=1;
    auto expression=[&](const E& value){if(value)height=std::max(height,value->graph_depth+1);};
    auto query=[&](const std::shared_ptr<Statement>& value){if(value)height=std::max(height,value->input_graph_depth+1);};
    query(s.query_definition);query(s.set_left);query(s.set_right);
    for(const auto& row:s.values)for(const auto& value:row)expression(value);
    for(const auto& value:s.assignments)expression(value.second);
    for(const auto& item:s.items)expression(item.value);
    expression(s.where);expression(s.on);expression(s.having);
    for(const auto& condition:s.join_conditions)expression(condition.first);
    for(const auto& value:s.group)expression(value);
    for(const auto& order:s.order)expression(order.value);
    if(height>input::sql_ast_walk_depth_limit)fail("QUERY_TOO_COMPLEX","Constructed query dependency depth exceeds160",position);
    s.input_graph_depth=height;
}
void attach_subquery(const E& expression,Statement query,int position){
    finish_statement_graph(query,position);
    const auto height=std::max(expression->graph_depth,query.input_graph_depth+1);
    const auto limit=input::sql_ast_walk_depth_limit-(active_parser_nodes?1:0);
    if(height>limit)fail("QUERY_TOO_COMPLEX","Constructed subquery height including query containers exceeds160",position);
    expression->graph_depth=height;expression->query=std::make_shared<Statement>(std::move(query));
}

// Validate the completed graph and collect dependencies without recursion.
// This must run before binder/optimizer/dependency visitors touch the graph.
std::vector<std::string> bounded_read_tables(const Statement& root,std::size_t* node_count=nullptr,std::size_t* byte_estimate=nullptr){
    struct Node {const Statement* query;const Expr* expression;std::size_t depth;};
    std::vector<Node> pending={{&root,nullptr,0}};std::unordered_map<const void*,std::size_t> deepest;std::set<std::string> tables;std::size_t estimated=0;
    auto charge=[&](std::uint64_t bytes,int position){if(bytes>input::sql_import_ast_estimate_limit-estimated)fail("QUERY_TOO_COMPLEX","SQL AST allocation estimate exceeds384 MiB (not RSS)",position);estimated+=static_cast<std::size_t>(bytes);};
    auto expression=[&](const E& e,std::size_t depth){if(e)pending.push_back({nullptr,e.get(),depth});};
    auto query=[&](const std::shared_ptr<Statement>& s,std::size_t depth){if(s)pending.push_back({s.get(),nullptr,depth});};
    while(!pending.empty()){
        const auto node=pending.back();pending.pop_back();const int position=node.expression?node.expression->pos:-1;
        if(node.depth>=input::sql_ast_walk_depth_limit)fail("QUERY_TOO_COMPLEX","Combined query/expression AST height exceeds160",position);
        const void* identity=node.query?static_cast<const void*>(node.query):static_cast<const void*>(node.expression);
        auto old=deepest.find(identity);if(old!=deepest.end()&&old->second>=node.depth)continue;
        const bool first=old==deepest.end();
        if(first&&deepest.size()>=input::sql_ast_node_limit)fail("QUERY_TOO_COMPLEX","SQL AST exceeds250000 total nodes",position);
        deepest[identity]=node.depth;
        if(node.expression){
            if(first){const auto& e=*node.expression;charge(sizeof(Expr)+64+2*(e.text.size()+e.qualifier.size()+e.type.size())+2*e.args.size()*sizeof(E)+json_allocation_estimate(e.value),position);}
            for(const auto& child:node.expression->args){expression(child,node.depth+1);}
            query(node.expression->query,node.depth+1);continue;
        }
        const auto& s=*node.query;for(const auto& source:s.sources)tables.insert(source.table);
        if(first){
            charge(sizeof(Statement)+256+2*(s.sources.size()*sizeof(Source)+s.items.size()*sizeof(SelectItem)+s.values.size()*sizeof(std::vector<E>)+s.assignments.size()*sizeof(std::pair<std::string,E>)+s.order.size()*sizeof(SortItem)+s.group.size()*sizeof(E)+s.join_conditions.size()*sizeof(std::pair<E,std::size_t>)+s.names.size()*sizeof(std::string)+s.join_kinds.size()*sizeof(std::string)),position);
            for(const std::string* text:{&s.kind,&s.table,&s.newname,&s.index,&s.column,&s.action,&s.cluster_name,&s.record_format,&s.method,&s.join_hint,&s.set_operation,&s.definition_sql})charge(2*text->size(),position);
            for(const auto& source:s.sources)charge(2*(source.table.size()+source.alias.size()+source.index_hint.size()),position);
            for(const auto& item:s.items)charge(2*item.alias.size(),position);
            for(const auto& row:s.values)charge(2*row.size()*sizeof(E),position);
            for(const auto& assignment:s.assignments)charge(2*assignment.first.size(),position);
            for(const auto& name:s.names)charge(2*name.size(),position);
            for(const auto& kind:s.join_kinds)charge(2*kind.size(),position);
            for(const json* value:{&s.columns,&s.table_constraints,&s.index_columns,&s.include_columns,&s.options,&s.organization,&s.cluster_columns,&s.record_options})charge(json_allocation_estimate(*value),position);
        }
        query(s.query_definition,node.depth+1);query(s.set_left,node.depth+1);query(s.set_right,node.depth+1);
        for(const auto& row:s.values)for(const auto& value:row)expression(value,node.depth+1);
        for(const auto& assignment:s.assignments)expression(assignment.second,node.depth+1);
        for(const auto& item:s.items)expression(item.value,node.depth+1);
        expression(s.where,node.depth+1);expression(s.on,node.depth+1);expression(s.having,node.depth+1);
        for(const auto& condition:s.join_conditions)expression(condition.first,node.depth+1);
        for(const auto& key:s.group)expression(key,node.depth+1);
        for(const auto& order:s.order)expression(order.value,node.depth+1);
    }
    if(node_count)*node_count=deepest.size();
    if(byte_estimate)*byte_estimate=estimated;
    return {tables.begin(),tables.end()};
}

class Parser {
    std::vector<Token> ts; std::size_t at=0;std::string original_sql;
    std::size_t expression_depth=0,unary_depth=0,negation_depth=0,select_depth=0,set_nodes=0;
    struct DepthGuard {
        std::size_t& depth;
        explicit DepthGuard(std::size_t& value,std::size_t maximum=max_expression_depth,int position=-1):depth(value) {
            if(++depth>maximum)fail("QUERY_TOO_COMPLEX","SQL nesting exceeds "+std::to_string(maximum),position);
        }
        ~DepthGuard(){--depth;}
    };
    const Token& t() const {return ts[at];}
    bool is(const std::string& s) const {return t().kind!=Token::String&&t().kind!=Token::End&&!t().quoted&&upper(t().text)==s;}
    bool take(const std::string& s) {if(is(s)){++at;return true;}return false;}
    void need(const std::string& s) {if(!take(s)) fail("SYNTAX_ERROR","Expected "+s+", got '"+t().text+"'",t().pos);}
    std::string identifier() {if(t().kind!=Token::Ident) fail("SYNTAX_ERROR","Expected identifier",t().pos);auto s=t().text;++at;if(s.empty()||s.size()>128)fail("INVALID_IDENTIFIER","Identifier must contain 1..128 UTF-8 bytes");return s;}
    std::size_t count() {if(t().kind!=Token::Number||t().text.find_first_of(".eE")!=std::string::npos) fail("SYNTAX_ERROR","Expected nonnegative integer",t().pos);std::size_t n=0;try {n=std::stoull(t().text);}catch(...){fail("NUMERIC_OVERFLOW","Integer limit overflow",t().pos);}++at;return n;}
    E primary() {
        int p=t().pos;
        if(take("EXISTS")){need("(");auto e=expr(Expr::Subquery,"EXISTS",p);auto query=select_expression();select_tail(query);need(")");attach_subquery(e,std::move(query),p);return e;}
        if(take("(")){if(is("SELECT")){auto e=expr(Expr::Subquery,"SCALAR",p);auto query=select_expression();select_tail(query);need(")");attach_subquery(e,std::move(query),p);return e;}auto e=expression();need(")");return e;}
        if(take("NULL")){return expr(Expr::Literal,"",p);}
        if(take("TRUE")){auto e=expr(Expr::Literal,"",p);e->value=true;return e;}
        if(take("FALSE")){auto e=expr(Expr::Literal,"",p);e->value=false;return e;}
        if(is("X")&&at+1<ts.size()&&ts[at+1].kind==Token::String){++at;auto h=upper(t().text);++at;
            if(h.size()%2||h.find_first_not_of("0123456789ABCDEF")!=std::string::npos)fail("INVALID_BLOB","Hex BLOB literal requires an even number of hex digits",p);
            if(h.size()>32u*1024u*1024u)fail("VALUE_TOO_LONG","BLOB maximum is 16 MiB",p);
            auto e=expr(Expr::Literal,"",p);e->value={{"$blob",h}};return e;}
        if(t().kind==Token::String){auto e=expr(Expr::Literal,"",p);e->value=t().text;++at;return e;}
        if(t().kind==Token::Number){auto e=expr(Expr::Literal,"",p);auto n=t().text;++at;try{if(n.find_first_of(".eE")!=std::string::npos){
            // strtod may set ERANGE for a representable subnormal. Only a
            // non-finite result or actual underflow to zero is out of range.
            errno=0;char* end=nullptr;double v=std::strtod(n.c_str(),&end);
            if(end!=n.c_str()+n.size()||!std::isfinite(v)||(errno==ERANGE&&v==0))fail("NUMERIC_OVERFLOW","REAL literal outside finite representable range",p);
            e->value=v;
        }else e->value=std::stoll(n);}catch(const DbError&){throw;}catch(...){fail("NUMERIC_OVERFLOW","Numeric literal outside supported range",p);}return e;}
        if(take("*"))return expr(Expr::Star,"*",p);
        if(t().kind==Token::Ident){static const std::set<std::string> reserved={"FROM","WHERE","ORDER","GROUP","HAVING","JOIN","INNER","ON","LIMIT","OFFSET","VALUES","SET","AS","BY","AND","OR","IS","SELECT","CREATE","INSERT","UPDATE","DELETE"};if(!t().quoted&&reserved.count(upper(t().text)))fail("SYNTAX_ERROR","Expected expression before "+upper(t().text),p);auto name=identifier();if(take("(")){auto e=expr(Expr::Function,upper(name),p);if(!take(")")){do{e->args.push_back(expression());}while(take(","));need(")");}set_arguments(e,std::move(e->args));return e;}auto e=expr(Expr::Column,name,p);if(take(".")){e->qualifier=name;if(take("*")){e->kind=Expr::Star;e->text="*";}else e->text=identifier();}return e;}
        fail("SYNTAX_ERROR","Expected expression",p);
    }
    E unary(){DepthGuard guard(unary_depth,max_expression_depth,t().pos);if(is("+")||is("-")){auto op=t().text;int p=t().pos;++at;
        // Permit INT64_MIN without first overflowing its positive magnitude.
        if(op=="-"&&t().kind==Token::Number&&t().text=="9223372036854775808"){++at;auto e=expr(Expr::Literal,"",p);e->value=std::numeric_limits<std::int64_t>::min();return e;}
        auto e=expr(Expr::Unary,op,p);set_arguments(e,{unary()});return e;}return primary();}
    E product(){auto a=unary();while(is("*")||is("/")){auto e=expr(Expr::Binary,t().text,t().pos);++at;set_arguments(e,{a,unary()});a=e;}return a;}
    E sum(){auto a=product();while(is("+")||is("-")){auto e=expr(Expr::Binary,t().text,t().pos);++at;set_arguments(e,{a,product()});a=e;}return a;}
    E comparison(){auto a=sum();if(is("IN")||(is("NOT")&&at+1<ts.size()&&upper(ts[at+1].text)=="IN")){bool n=take("NOT");need("IN");need("(");if(!is("SELECT")&&!is("("))fail("UNSUPPORTED_SQL","IN currently requires a query operand",t().pos);auto e=expr(Expr::Subquery,n?"NOT_IN":"IN",a->pos);set_arguments(e,{a});auto query=select_expression();select_tail(query);need(")");attach_subquery(e,std::move(query),a->pos);return e;}if(take("IS")){bool n=take("NOT");need("NULL");auto e=expr(Expr::IsNull,n?"IS NOT NULL":"IS NULL",a->pos);set_arguments(e,{a});return e;}if(is("=")||is("<>")||is("!=")||is("<")||is(">")||is("<=")||is(">=")){auto e=expr(Expr::Binary,t().text,t().pos);++at;set_arguments(e,{a,sum()});return e;}return a;}
    E negation(){DepthGuard guard(negation_depth,max_expression_depth,t().pos);if(is("NOT")){const auto position=t().pos;++at;auto e=expr(Expr::Unary,"NOT",position);set_arguments(e,{negation()});return e;}return comparison();}
    E conjunction(){auto a=negation();while(is("AND")){auto e=expr(Expr::Binary,"AND",t().pos);++at;set_arguments(e,{a,negation()});a=e;}return a;}
    E expression(){DepthGuard guard(expression_depth,max_expression_depth,t().pos);auto a=conjunction();while(is("OR")){auto e=expr(Expr::Binary,"OR",t().pos);++at;set_arguments(e,{a,conjunction()});a=e;}return a;}
    json column_def(){
        json c={{"name",identifier()},{"length",0},{"not_null",false},{"primary",false},{"unique",false},{"default",nullptr}};
        auto ty=upper(identifier());
        if(ty=="INT"||ty=="BIGINT")ty="INTEGER";
        if(ty=="DOUBLE"){take("PRECISION");ty="REAL";}
        if(ty=="BOOL")ty="BOOLEAN";
        if(ty=="VARCHAR"){need("(");auto len=count();need(")");if(!len||len>4096)fail("INVALID_SCHEMA","VARCHAR length must be 1..4096 characters");c["length"]=len;}
        else if(ty=="BLOB"){std::size_t len=16u*1024u*1024u;if(take("(")){len=count();need(")");}if(!len||len>16u*1024u*1024u)fail("INVALID_SCHEMA","BLOB length must be 1..16777216 bytes");c["length"]=len;}
        else if(ty!="INTEGER"&&ty!="REAL"&&ty!="BOOLEAN")fail("UNSUPPORTED_TYPE","Supported types: INTEGER, REAL, BOOLEAN, VARCHAR(n), BLOB(n)");
        c["type"]=ty;
        for(;;){if(take("NOT")){need("NULL");c["not_null"]=true;}else if(take("NULL")){}else if(take("PRIMARY")){need("KEY");c["primary"]=true;c["not_null"]=true;}else if(take("UNIQUE"))c["unique"]=true;else if(take("DEFAULT")){auto e=unary();if(e->kind==Expr::Unary&&(e->text=="-"||e->text=="+")&&e->args[0]->kind==Expr::Literal){auto v=e->args[0]->value;if(v.is_null())c["default"]=nullptr;else if(!v.is_number())fail("TYPE_ERROR","Invalid numeric DEFAULT");else if(e->text=="+")c["default"]=v;else if(v.is_number_integer()){auto n=v.get<std::int64_t>();if(n==std::numeric_limits<std::int64_t>::min())fail("NUMERIC_OVERFLOW","Default overflow");c["default"]=-n;}else c["default"]=-v.get<double>();}else if(e->kind==Expr::Literal)c["default"]=e->value;else fail("UNSUPPORTED_SQL","DEFAULT requires a constant");}else break;}
        return c;
    }
    bool alias_candidate() const {static const std::set<std::string> stop={"FROM","WHERE","INNER","LEFT","RIGHT","FULL","CROSS","OUTER","JOIN","ON","ORDER","BY","ASC","DESC","LIMIT","OFFSET","GROUP","HAVING","UNION","INTERSECT","EXCEPT","SET","VALUES","AND","OR","IS","USE"};return t().kind==Token::Ident&&(t().quoted||!stop.count(upper(t().text)));}
    Source source(){Source s;s.table=identifier();s.alias=s.table;if(take("AS"))s.alias=identifier();else if(alias_candidate())s.alias=identifier();if(take("USE")){if(take("SCAN"))s.force_scan=true;else{need("INDEX");need("(");s.index_hint=identifier();need(")");}}return s;}
    json name_list(){json a=json::array();need("(");do{a.push_back(identifier());}while(take(","));need(")");return a;}
    std::string method_name(){auto m=upper(identifier());if(m=="BPLUSTREE")m="BPLUS";if(m=="B_TREE")m="BTREE";if(m=="EXTENDIBLE_HASH")m="HASH_EXTENDIBLE";if(m=="STATIC_HASH")m="HASH_STATIC";if(m=="LINEAR_HASH")m="HASH_LINEAR";
        static const std::set<std::string> valid={"BPLUS","BTREE","DENSE","SPARSE","MULTILEVEL","ORDERED_DENSE","ORDERED_SPARSE","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR","BITMAP","BITMAP_HYBRID"};if(!valid.count(m))fail("UNSUPPORTED_ACCESS_METHOD","Unknown index method: "+m);return m;}
    json method_options(){json o=json::object();if(!take("WITH"))return o;need("(");do{auto n=identifier();if(o.contains(n))fail("DUPLICATE_OPTION","Option specified twice: "+n);need("=");auto v=unary();if(v->kind!=Expr::Literal)fail("INVALID_OPTION","Index options require literal values");o[n]=v->value;}while(take(","));need(")");return o;}
    json transaction_options(){
        json o=json::object();for(;;){
            if(take("PROTOCOL")){if(o.contains("protocol"))fail("DUPLICATE_OPTION","Duplicate transaction protocol");o["protocol"]=upper(identifier());}
            else if(take("READ")){if(o.contains("read_only"))fail("DUPLICATE_OPTION","Duplicate read mode");if(take("ONLY"))o["read_only"]=true;else{need("WRITE");o["read_only"]=false;}}
            else if(take("ISOLATION")){need("LEVEL");if(o.contains("isolation"))fail("DUPLICATE_OPTION","Duplicate isolation level");if(take("SERIALIZABLE"))o["isolation"]="SERIALIZABLE";else if(take("REPEATABLE")){need("READ");o["isolation"]="REPEATABLE_READ";}else if(take("READ")){if(take("COMMITTED"))o["isolation"]="READ_COMMITTED";else{need("UNCOMMITTED");o["isolation"]="READ_UNCOMMITTED";}}else fail("SYNTAX_ERROR","Unknown isolation level");}
            else break;
        }return o;
    }
    json organization_spec(){auto m=upper(identifier());if(m=="CLUSTERED")m="BPLUS";if(m!="HEAP"&&m!="ORDERED"&&m!="BPLUS"&&m!="HASH_STATIC"&&m!="HASH_EXTENDIBLE"&&m!="HASH_LINEAR")fail("UNSUPPORTED_ACCESS_METHOD","Unsupported table organization: "+m);json keys=json::array();if(is("("))keys=name_list();if(m!="HEAP"&&keys.empty())fail("INVALID_SCHEMA","Organization requires key columns");if(m=="HEAP"&&!keys.empty())fail("INVALID_SCHEMA","HEAP has no organization key");return {{"method",m},{"columns",keys},{"options",method_options()}};}
    void record_spec(Statement& s){need("FORMAT");s.record_format=upper(identifier());if(s.record_format!="JSON"&&s.record_format!="FIXED"&&s.record_format!="VARIABLE")fail("RECORD_LAYOUT","RECORD FORMAT requires JSON, FIXED or VARIABLE");s.record_options=method_options();}
    Statement select_operand(){
        DepthGuard guard(select_depth);
        if(take("(")){auto s=select_expression();select_tail(s);need(")");finish_statement_graph(s,t().pos);return s;}
        need("SELECT");Statement s;

            s.kind="SELECT";s.distinct=take("DISTINCT");do{SelectItem i;i.value=expression();if(take("AS"))i.alias=identifier();else if(alias_candidate())i.alias=identifier();s.items.push_back(i);}while(take(","));
            if(take("FROM")){
                s.sources.push_back(source());
                while(is("INNER")||is("LEFT")||is("RIGHT")||is("FULL")||is("CROSS")||is("JOIN")){
                    std::string kind="INNER";
                    if(take("LEFT"))kind="LEFT";else if(take("RIGHT"))kind="RIGHT";else if(take("FULL"))kind="FULL";else if(take("CROSS"))kind="CROSS";else take("INNER");
                    if(kind!="INNER"&&kind!="CROSS")take("OUTER");
                    need("JOIN");
                    if(s.sources.size()>=32)fail("QUERY_TOO_COMPLEX","At most32 joined relations are supported",t().pos);
                    s.sources.push_back(source());E next;
                    if(kind=="CROSS"){next=expr(Expr::Literal);next->value=true;}else{need("ON");next=expression();}
                    s.join_kinds.push_back(kind=="CROSS"?"INNER":kind);s.join_conditions.push_back({next,s.sources.size()});
                    if(s.on){auto both=expr(Expr::Binary,"AND");set_arguments(both,{s.on,next});s.on=both;}else s.on=next;
                }
            }
            if(take("USE")){need("JOIN");need("(");s.join_hint=upper(identifier());need(")");static const std::set<std::string> methods={"NESTED_LOOP","BLOCK_NESTED_LOOP","INDEX_NESTED_LOOP","MERGE","HASH"};if(!methods.count(s.join_hint))fail("INVALID_JOIN_METHOD","Unknown join algorithm: "+s.join_hint);if(s.sources.size()<2)fail("INVALID_JOIN_METHOD","USE JOIN requires at least two sources");}
            if(take("WHERE"))s.where=expression();
            if(take("GROUP")){need("BY");do{s.group.push_back(expression());}while(take(","));}
            if(take("HAVING"))s.having=expression();
        finish_statement_graph(s,t().pos);return s;
    }
    Statement select_expression(int minimum_precedence=1){
        DepthGuard guard(select_depth,input::sql_query_nesting_limit,t().pos);auto left=select_operand();
        for(;;){
            int precedence=is("INTERSECT")?2:(is("UNION")||is("EXCEPT"))?1:0;
            if(precedence<minimum_precedence)break;
            if(++set_nodes>64)fail("QUERY_TOO_COMPLEX","At most64 set-operation nodes are supported",t().pos);
            auto operation=upper(t().text);++at;bool all=take("ALL");if(!all)take("DISTINCT");
            auto right=select_expression(precedence+1);Statement parent;parent.kind="SELECT";
            if(std::max(left.input_graph_depth,right.input_graph_depth)>=input::sql_ast_walk_depth_limit)fail("QUERY_TOO_COMPLEX","Constructed set-query dependency depth exceeds160",t().pos);
            parent.set_operation=operation;parent.set_all=all;parent.set_left=std::make_shared<Statement>(std::move(left));parent.set_right=std::make_shared<Statement>(std::move(right));
            parent.sources=parent.set_left->sources;parent.sources.insert(parent.sources.end(),parent.set_right->sources.begin(),parent.set_right->sources.end());finish_statement_graph(parent,t().pos);left=std::move(parent);
        }
        return left;
    }
    void select_tail(Statement& s){
        if(take("ORDER")){need("BY");do{SortItem i;i.value=expression();i.desc=take("DESC");if(!i.desc)take("ASC");s.order.push_back(i);}while(take(","));}
        if(take("LIMIT"))s.limit=count();
        if(take("OFFSET"))s.offset=count();
    }
public:
    explicit Parser(const std::string& sql):ts(lex(sql)),original_sql(sql){}
    Statement parse(){
        ParserNodeScope node_scope;
        Statement s;
        if(take("EXPLAIN")){s.explain=true;s.analyze=take("ANALYZE");if(!is("SELECT")&&!is("("))fail("UNSUPPORTED_SQL","EXPLAIN currently supports SELECT",t().pos);}
        if(is("SELECT")||is("(")){
            bool explain=s.explain,analyze=s.analyze;s=select_expression();select_tail(s);s.explain=explain;s.analyze=analyze;
        } else if(take("CREATE")){
            s.unique=take("UNIQUE");
            if(take("MATERIALIZED")){if(s.unique)fail("SYNTAX_ERROR","UNIQUE cannot precede MATERIALIZED VIEW");need("VIEW");s.kind="CREATE_MATERIALIZED_VIEW";s.table=identifier();s.options=method_options();need("AS");const auto start=t().pos;auto definition=select_expression();select_tail(definition);finish_statement_graph(definition,t().pos);if(definition.input_graph_depth>=input::sql_ast_walk_depth_limit)fail("QUERY_TOO_COMPLEX","Materialized definition height including query containers exceeds160",t().pos);s.query_definition=std::make_shared<Statement>(std::move(definition));s.definition_sql=original_sql.substr(std::size_t(start),std::size_t(t().pos-start));}
            else if(take("CLUSTER")){if(s.unique)fail("SYNTAX_ERROR","UNIQUE cannot precede CLUSTER");s.kind="CREATE_CLUSTER";if(take("IF")){need("NOT");need("EXISTS");s.if_not_exists=true;}s.cluster_name=identifier();}
            else if(take("TABLE")){if(s.unique)fail("SYNTAX_ERROR","UNIQUE must precede INDEX");s.kind="CREATE_TABLE";if(take("IF")){need("NOT");need("EXISTS");s.if_not_exists=true;}s.table=identifier();need("(");do{if(take("PRIMARY")){need("KEY");auto ns=name_list();s.table_constraints.push_back({{"columns",ns},{"kind","primary"}});}else if(take("UNIQUE")){auto ns=name_list();s.table_constraints.push_back({{"columns",ns},{"kind","unique"}});}else s.columns.push_back(column_def());}while(take(","));need(")");bool org_seen=false,record_seen=false,cluster_seen=false;while(is("ORGANIZATION")||is("RECORD")||is("CLUSTER")){if(take("CLUSTER")){if(cluster_seen)fail("DUPLICATE_OPTION","Duplicate cluster clause");cluster_seen=true;s.cluster_name=identifier();s.cluster_columns=name_list();}else if(take("ORGANIZATION")){if(org_seen)fail("DUPLICATE_OPTION","Duplicate organization");org_seen=true;s.organization=organization_spec();}else{need("RECORD");if(record_seen)fail("DUPLICATE_OPTION","Duplicate record format");record_seen=true;record_spec(s);}}}
            else{need("INDEX");s.kind="CREATE_INDEX";if(take("IF")){need("NOT");need("EXISTS");s.if_not_exists=true;}s.index=identifier();need("ON");s.table=identifier();if(take("USING"))s.method=method_name();s.index_columns=name_list();s.column=s.index_columns[0];if(take("USING"))s.method=method_name();if(take("INCLUDE"))s.include_columns=name_list();s.options=method_options();}
        } else if(take("DROP")){
            if(take("MATERIALIZED")){need("VIEW");s.kind="DROP_MATERIALIZED_VIEW";if(take("IF")){need("EXISTS");s.if_exists=true;}s.table=identifier();}
            else if(take("CLUSTER")){s.kind="DROP_CLUSTER";if(take("IF")){need("EXISTS");s.if_exists=true;}s.cluster_name=identifier();}
            else if(take("TABLE")){s.kind="DROP_TABLE";if(take("IF")){need("EXISTS");s.if_exists=true;}s.table=identifier();}else{need("INDEX");s.kind="DROP_INDEX";if(take("IF")){need("EXISTS");s.if_exists=true;}s.index=identifier();}
        } else if(take("REFRESH")){need("MATERIALIZED");need("VIEW");s.kind="REFRESH_MATERIALIZED_VIEW";s.table=identifier();
        } else if(take("ALTER")){
            need("TABLE");s.kind="ALTER_TABLE";s.table=identifier();
            if(take("CLUSTER")){s.action="CLUSTER";s.cluster_name=identifier();s.cluster_columns=name_list();}
            else if(take("NO")){need("CLUSTER");s.action="NO_CLUSTER";}
            else if(take("RECORD")){s.action="RECORD_FORMAT";record_spec(s);}
            else if(take("ORGANIZATION")){s.action="ORGANIZATION";s.organization=organization_spec();}
            else if(take("ADD")){take("COLUMN");s.action="ADD";s.columns.push_back(column_def());}
            else if(take("DROP")){take("COLUMN");s.action="DROP";s.column=identifier();}
            else if(take("RENAME")){if(take("TO")){s.action="RENAME_TABLE";s.newname=identifier();}else{take("COLUMN");s.action="RENAME_COLUMN";s.column=identifier();need("TO");s.newname=identifier();}}
            else fail("UNSUPPORTED_SQL","ALTER TABLE supports ADD/DROP/RENAME COLUMN and RENAME TO",t().pos);
        } else if(take("REORGANIZE")){take("TABLE");s.kind="REORGANIZE";s.table=identifier();if(take("ORGANIZATION"))s.organization=organization_spec();else s.organization=nullptr;
        } else if(take("REINDEX")){s.kind="REINDEX";if(take("TABLE")){s.table=identifier();}else{take("INDEX");s.index=identifier();}
        } else if(take("INSPECT")){s.kind="INSPECT";if(take("CLUSTER")){s.kind="INSPECT_CLUSTER";s.cluster_name=identifier();}else if(take("INDEX"))s.index=identifier();else{take("TABLE");s.table=identifier();if(take("INDEX"))s.index=identifier();}if(take("LIMIT")){s.limit=count();if(!s.limit||s.limit>2048)fail("ARGUMENT","INSPECT LIMIT must be 1..2048");}}
        else if(take("ANALYZE")){take("TABLE");s.kind="ANALYZE";s.table=identifier();s.options=method_options();}
        else if(take("INSERT")){
            need("INTO");s.kind="INSERT";s.table=identifier();if(take("(")){do{s.names.push_back(identifier());}while(take(","));need(")");}
            if(take("DEFAULT")){need("VALUES");s.values.emplace_back();}else{need("VALUES");do{need("(");std::vector<E> row;do{row.push_back(expression());}while(take(","));need(")");s.values.push_back(row);}while(take(","));}
        } else if(take("UPDATE")){
            s.kind="UPDATE";s.table=identifier();need("SET");do{auto n=identifier();need("=");s.assignments.push_back({n,expression()});}while(take(","));if(take("WHERE"))s.where=expression();
        } else if(take("DELETE")){
            need("FROM");s.kind="DELETE";s.table=identifier();if(take("WHERE"))s.where=expression();
        } else if(take("BEGIN")){take("TRANSACTION");s.kind="BEGIN";s.options=transaction_options();}
        else if(take("SET")){
            if(take("ARCHIVING")){s.kind="SET_ARCHIVING";if(take("ON"))s.options["enabled"]=true;else{need("OFF");s.options["enabled"]=false;}}
            else if(take("BUFFER")){s.kind="SET_BUFFER";if(take("POLICY"))s.options["policy"]=upper(identifier());else{auto option=upper(identifier());if(option!="READ_AHEAD"&&option!="WAIT_MS")fail("SYNTAX_ERROR","SET BUFFER requires POLICY, READ_AHEAD or WAIT_MS");if(t().kind!=Token::Number||t().text.find_first_not_of("0123456789")!=std::string::npos)fail("SYNTAX_ERROR","Unsigned buffer option required");auto value=count();s.options[option=="READ_AHEAD"?"read_ahead_pages":"wait_ms"]=value;}}
            else if(take("LOCK")){if(take("GRANULARITY")){s.kind="SET_LOCK_GRANULARITY";s.options["granularity"]=upper(identifier());}else{need("POLICY");s.kind="SET_LOCK_POLICY";s.options["policy"]=upper(identifier());}}
            else{need("TRANSACTION");s.kind="SET_TRANSACTION";s.options=transaction_options();if(s.options.empty())fail("SYNTAX_ERROR","SET TRANSACTION requires an option");}
        }
        else if(take("TREE")){if(take("OPEN")){s.kind="TREE_OPEN";s.table=identifier();need("PARENT");s.column=identifier();}else if(take("LOCK")){s.kind="TREE_LOCK";s.where=unary();}else if(take("UNLOCK")){s.kind="TREE_UNLOCK";s.where=unary();}else{need("STATUS");s.kind="TREE_STATUS";}}
        else if(take("SAVEPOINT")){s.kind="SAVEPOINT";s.table=identifier();}
        else if(take("RELEASE")){take("SAVEPOINT");s.kind="RELEASE";s.table=identifier();}
        else if(take("COMMIT")){take("TRANSACTION");s.kind="COMMIT";}
        else if(take("ROLLBACK")){take("TRANSACTION");s.kind="ROLLBACK";if(take("TO")){take("SAVEPOINT");s.kind="ROLLBACK_TO";s.table=identifier();}}
        else if(take("BACKUP")){need("DATABASE");need("AS");s.kind="BACKUP_DATABASE";if(t().kind==Token::String){s.table=t().text;++at;}else s.table=identifier();}
        else if(take("ARCHIVE")){need("WAL");s.kind="ARCHIVE_WAL";}
        else if(take("PROMOTE")){need("STANDBY");s.kind="PROMOTE_STANDBY";}
        else if(take("BECOME")){need("STANDBY");s.kind="BECOME_STANDBY";}
        else if(take("CHECKPOINT"))s.kind="CHECKPOINT";
        else if(take("VERIFY"))s.kind="VERIFY";
        else if(take("TRUNCATE")){take("TABLE");s.kind="TRUNCATE";s.table=identifier();}
        else if(take("SHOW")){if(take("RECOVERY"))s.kind="SHOW_RECOVERY";else if(take("MATERIALIZED")){need("VIEWS");s.kind="SHOW_MATERIALIZED_VIEWS";}else if(take("CLUSTERS"))s.kind="SHOW_CLUSTERS";else if(take("BUFFER"))s.kind="SHOW_BUFFER";else if(take("MEDIA"))s.kind="SHOW_MEDIA";else if(take("TRANSACTIONS"))s.kind="SHOW_TRANSACTIONS";else if(take("LOCKS"))s.kind="SHOW_LOCKS";else if(take("WAL")){s.kind=take("RECORDS")?"SHOW_WAL_RECORDS":"SHOW_WAL";if(s.kind=="SHOW_WAL_RECORDS"&&take("LIMIT"))s.limit=count();}else if(take("TABLES"))s.kind="SHOW_TABLES";else{need("INDEXES");need("FROM");s.kind="SHOW_INDEXES";s.table=identifier();}}
        else if(take("SCRUB")){need("MEDIA");s.kind="SCRUB_MEDIA";}
        else if(take("DESCRIBE")||take("DESC")){s.kind="DESCRIBE";s.table=identifier();}
        else fail("UNSUPPORTED_SQL","Unsupported or empty SQL statement",t().pos);
        take(";");if(t().kind!=Token::End)fail("SYNTAX_ERROR","Unexpected token '"+t().text+"'. Execute one statement per request.",t().pos);
        finish_statement_graph(s,t().pos);s.read_tables=bounded_read_tables(s,&s.input_ast_nodes,&s.input_ast_estimated_bytes);s.input_ast_nodes=std::max(s.input_ast_nodes,node_scope.nodes);return s;
    }
};

bool blob_value(const json& v){return v.is_object()&&v.size()==1&&v.contains("$blob")&&v["$blob"].is_string();}
bool lazy_blob(const json& v){return v.is_object()&&v.contains("$lob")&&v.contains("bytes");}
thread_local std::function<json(const json&,std::uint64_t,std::uint64_t)> active_blob_reader;
struct BlobReaderScope {
    std::function<json(const json&,std::uint64_t,std::uint64_t)> previous;
    explicit BlobReaderScope(std::function<json(const json&,std::uint64_t,std::uint64_t)> reader):previous(std::move(active_blob_reader)){active_blob_reader=std::move(reader);}
    ~BlobReaderScope(){active_blob_reader=std::move(previous);}
};
json resolve_blob(const json& v){if(!lazy_blob(v))return v;if(!active_blob_reader)fail("INTERNAL_ERROR","Lazy BLOB has no transaction reader");return active_blob_reader(v,1,std::numeric_limits<std::uint64_t>::max());}
bool aggregate_function(const std::string& n){return n=="COUNT"||n=="SUM"||n=="AVG"||n=="MIN"||n=="MAX";}
std::string value_type(const json& v){if(v.is_null())return "NULL";if(v.is_boolean())return "BOOLEAN";if(v.is_number_integer())return "INTEGER";if(v.is_number_float())return "REAL";if(v.is_string())return "VARCHAR";if(blob_value(v))return "BLOB";fail("TYPE_ERROR","Unsupported value");}
bool numeric(const std::string& t){return t=="INTEGER"||t=="REAL";}
bool aggregate(const E& e){if(!e)return false;if(e->kind==Expr::Function&&aggregate_function(e->text))return true;for(auto& a:e->args)if(aggregate(a))return true;return false;}
struct BoundCol {std::string name,type,table; int relation_id=-1,column_id=-1,slot_id=-1;};
using Schema=std::vector<BoundCol>;
thread_local std::vector<Schema> outer_binding_scopes;
thread_local std::vector<json> outer_parameter_rows;
thread_local std::function<void(const E&,const Schema&)> active_subquery_binder;
thread_local std::function<json(const E&,const json&)> active_subquery_evaluator;
struct OuterBindingScope{explicit OuterBindingScope(const Schema& schema){outer_binding_scopes.push_back(schema);}~OuterBindingScope(){outer_binding_scopes.pop_back();}};
struct OuterParameterScope{explicit OuterParameterScope(const json& row){query_materialize(row);outer_parameter_rows.push_back(row);}~OuterParameterScope(){outer_parameter_rows.pop_back();}};
struct SubqueryCallbacksScope{
    decltype(active_subquery_binder) binder;decltype(active_subquery_evaluator) evaluator;
    SubqueryCallbacksScope(decltype(binder) b,decltype(evaluator) e):binder(std::move(active_subquery_binder)),evaluator(std::move(active_subquery_evaluator)){active_subquery_binder=std::move(b);active_subquery_evaluator=std::move(e);}
    ~SubqueryCallbacksScope(){active_subquery_binder=std::move(binder);active_subquery_evaluator=std::move(evaluator);}
};
bool runtime_expression(const E& e){if(!e)return false;if(e->kind==Expr::Parameter||e->kind==Expr::Subquery)return true;for(const auto& a:e->args)if(runtime_expression(a))return true;return false;}
bool subquery_expression(const E& e){if(!e)return false;if(e->kind==Expr::Subquery)return true;for(const auto& a:e->args)if(subquery_expression(a))return true;return false;}
void bind_expr(const E& e,const Schema& cols,bool allow_aggregate=false,bool inside_aggregate=false){
    if(!e)return;
    if(e->kind==Expr::Literal){e->type=value_type(e->value);return;}
    if(e->kind==Expr::Column){int found=-1;for(std::size_t i=0;i<cols.size();++i)if(cols[i].name==e->text&&(e->qualifier.empty()||cols[i].table==e->qualifier)){if(found!=-1)fail("AMBIGUOUS_COLUMN","Ambiguous column: "+e->text,e->pos);found=int(i);}if(found<0){
            if(!e->qualifier.empty())for(const auto& c:cols)if(c.table==e->qualifier)fail("COLUMN_NOT_FOUND","Column not found in the nearest alias scope: "+e->qualifier+"."+e->text,e->pos);
            for(std::size_t level=0;level<outer_binding_scopes.size();++level){const auto& outer=outer_binding_scopes[outer_binding_scopes.size()-1-level];int outer_found=-1;for(std::size_t i=0;i<outer.size();++i)if(outer[i].name==e->text&&(e->qualifier.empty()||outer[i].table==e->qualifier)){if(outer_found>=0)fail("AMBIGUOUS_COLUMN","Ambiguous outer column: "+e->text,e->pos);outer_found=int(i);}if(outer_found>=0){e->kind=Expr::Parameter;e->bound=outer_found;e->outer_depth=int(level+1);e->type=outer[outer_found].type;return;}if(!e->qualifier.empty())for(const auto& c:outer)if(c.table==e->qualifier)fail("COLUMN_NOT_FOUND","Column not found in the nearest outer alias scope: "+e->qualifier+"."+e->text,e->pos);}
            fail("COLUMN_NOT_FOUND","Column not found: "+(e->qualifier.empty()?"":e->qualifier+".")+e->text,e->pos);
        }e->bound=found;e->type=cols[found].type;return;}
    if(e->kind==Expr::Parameter)return;
    if(e->kind==Expr::Subquery){for(const auto& a:e->args)bind_expr(a,cols,allow_aggregate,inside_aggregate);if(!active_subquery_binder)fail("UNSUPPORTED_SQL","Subqueries are currently supported in SELECT only",e->pos);active_subquery_binder(e,cols);return;}
    if(e->kind==Expr::Star)fail("SYNTAX_ERROR","Wildcard is only valid as SELECT item or COUNT(*)",e->pos);
    if(e->kind==Expr::Function){
        if(!aggregate_function(e->text)){
            if(e->text!="OCTET_LENGTH"&&e->text!="BLOB_SLICE")fail("UNSUPPORTED_FUNCTION","Supported functions: COUNT, SUM, AVG, MIN, MAX, OCTET_LENGTH, BLOB_SLICE",e->pos);
            if(e->args.size()!=(e->text=="OCTET_LENGTH"?1u:3u))fail("FUNCTION_ARGUMENT","Incorrect scalar function argument count",e->pos);
            for(auto& a:e->args)bind_expr(a,cols,allow_aggregate,inside_aggregate);
            auto ty=e->args[0]->type;
            if(ty!="NULL"&&ty!="BLOB"&&!(e->text=="OCTET_LENGTH"&&ty=="VARCHAR"))fail("TYPE_ERROR","OCTET_LENGTH requires BLOB/VARCHAR; BLOB_SLICE requires BLOB",e->pos);
            for(std::size_t i=1;i<e->args.size();++i)if(e->args[i]->type!="INTEGER"&&e->args[i]->type!="NULL")fail("TYPE_ERROR","BLOB_SLICE position and length must be INTEGER",e->pos);
            e->type=e->text=="OCTET_LENGTH"?"INTEGER":"BLOB";return;
        }
        if(!allow_aggregate||inside_aggregate)fail("INVALID_AGGREGATE","Aggregate is not valid in this expression",e->pos);
        if(e->text!="COUNT"&&e->text!="SUM"&&e->text!="AVG"&&e->text!="MIN"&&e->text!="MAX")fail("UNSUPPORTED_FUNCTION","Supported aggregate functions: COUNT, SUM, AVG, MIN, MAX",e->pos);
        if(e->args.size()!=1)fail("INVALID_AGGREGATE","Aggregate requires exactly one argument",e->pos);
        if(e->args[0]->kind==Expr::Star){if(e->text!="COUNT"||!e->args[0]->qualifier.empty())fail("INVALID_AGGREGATE","Only COUNT(*) accepts wildcard",e->pos);}else bind_expr(e->args[0],cols,false,true);
        if((e->text=="SUM"||e->text=="AVG")&&!numeric(e->args[0]->type)&&e->args[0]->type!="NULL")fail("TYPE_ERROR","SUM/AVG require numeric input",e->pos);
        if((e->text=="MIN"||e->text=="MAX")&&e->args[0]->type=="BLOB")fail("UNSUPPORTED_SQL","MIN/MAX do not order BLOB values",e->pos);
        e->type=e->text=="COUNT"?"INTEGER":e->text=="AVG"?"REAL":e->args[0]->type;return;
    }
    for(auto& a:e->args)bind_expr(a,cols,allow_aggregate,inside_aggregate);
    if(e->kind==Expr::IsNull){e->type="BOOLEAN";return;}
    auto a=e->args[0]->type;
    if(e->kind==Expr::Unary){if(e->text=="NOT"){if(a!="BOOLEAN"&&a!="NULL")fail("TYPE_ERROR","NOT requires BOOLEAN",e->pos);e->type="BOOLEAN";}else{if(!numeric(a)&&a!="NULL")fail("TYPE_ERROR","Unary arithmetic requires numeric input",e->pos);e->type=a;}return;}
    auto b=e->args[1]->type;
    if(e->text=="AND"||e->text=="OR"){if((a!="BOOLEAN"&&a!="NULL")||(b!="BOOLEAN"&&b!="NULL"))fail("TYPE_ERROR","AND/OR require BOOLEAN",e->pos);e->type="BOOLEAN";}
    else if(e->text=="+"||e->text=="-"||e->text=="*"||e->text=="/"){if((!numeric(a)&&a!="NULL")||(!numeric(b)&&b!="NULL"))fail("TYPE_ERROR","Arithmetic requires numeric input",e->pos);e->type=e->text=="/"||a=="REAL"||b=="REAL"?"REAL":"INTEGER";}
    else{if(a!="NULL"&&b!="NULL"&&(a=="BLOB"||b=="BLOB"))fail("UNSUPPORTED_SQL","BLOB comparisons are not supported; use explicit byte slices in the application",e->pos);if(a!="NULL"&&b!="NULL"&&a!=b&&!(numeric(a)&&numeric(b)))fail("TYPE_ERROR","Cannot compare "+a+" with "+b,e->pos);e->type="BOOLEAN";}
}
void require_boolean(const E& e){if(e&&e->type!="BOOLEAN"&&e->type!="NULL")fail("TYPE_ERROR","WHERE/ON requires BOOLEAN",e->pos);}
int compare_values(const json& a,const json& b){if(a.is_number()&&b.is_number())return access::compare_keys(json::array({a}),json::array({b}));if(blob_value(a)&&blob_value(b)){auto& x=a["$blob"].get_ref<const std::string&>();auto& y=b["$blob"].get_ref<const std::string&>();return x<y?-1:x>y?1:0;}if(a.is_string()&&b.is_string()){auto& x=a.get_ref<const std::string&>();auto& y=b.get_ref<const std::string&>();return x<y?-1:x>y?1:0;}if(a.is_boolean()&&b.is_boolean()){bool x=a.get<bool>(),y=b.get<bool>();return x==y?0:x?1:-1;}fail("TYPE_ERROR","Incompatible comparison values");}
std::int64_t add_int(std::int64_t a,std::int64_t b){if((b>0&&a>std::numeric_limits<std::int64_t>::max()-b)||(b<0&&a<std::numeric_limits<std::int64_t>::min()-b))fail("NUMERIC_OVERFLOW","INTEGER addition overflow");return a+b;}
std::int64_t sub_int(std::int64_t a,std::int64_t b){if((b<0&&a>std::numeric_limits<std::int64_t>::max()+b)||(b>0&&a<std::numeric_limits<std::int64_t>::min()+b))fail("NUMERIC_OVERFLOW","INTEGER subtraction overflow");return a-b;}
std::int64_t mul_int(std::int64_t a,std::int64_t b){if(!a||!b)return 0;auto lo=std::numeric_limits<std::int64_t>::min(),hi=std::numeric_limits<std::int64_t>::max();if((a==-1&&b==lo)||(b==-1&&a==lo)||(a>0?(b>0?a>hi/b:b<lo/a):(b>0?a<lo/b:a<hi/b)))fail("NUMERIC_OVERFLOW","INTEGER multiplication overflow");return a*b;}
json eval(const E& e,const json& row,const std::vector<json>* group=nullptr){
    query_work();
    if(e->kind==Expr::Literal)return e->value;
    if(e->kind==Expr::Column)return row.at(e->bound);
    if(e->kind==Expr::Parameter){if(e->outer_depth<1||std::size_t(e->outer_depth)>outer_parameter_rows.size())fail("INTERNAL_ERROR","Correlated parameter evaluated outside its outer row scope",e->pos);return outer_parameter_rows[outer_parameter_rows.size()-std::size_t(e->outer_depth)].at(e->bound);}
    if(e->kind==Expr::Subquery){if(!active_subquery_evaluator)fail("INTERNAL_ERROR","Subquery evaluated without its transaction executor",e->pos);return active_subquery_evaluator(e,row);}
    if(e->kind==Expr::Function){
        if(!aggregate_function(e->text)){
            auto v=eval(e->args[0],row,group);if(v.is_null())return nullptr;
            if(e->text=="OCTET_LENGTH")return static_cast<std::int64_t>(lazy_blob(v)?v["bytes"].get<std::uint64_t>():blob_value(v)?v["$blob"].get_ref<const std::string&>().size()/2:v.get_ref<const std::string&>().size());
            auto a=eval(e->args[1],row,group),b=eval(e->args[2],row,group);if(a.is_null()||b.is_null())return nullptr;
            auto start=a.get<std::int64_t>(),length=b.get<std::int64_t>();if(start<1||length<0)fail("FUNCTION_ARGUMENT","BLOB_SLICE is 1-based and requires nonnegative length",e->pos);
            if(lazy_blob(v)){if(!active_blob_reader)fail("INTERNAL_ERROR","Lazy BLOB has no reader");return active_blob_reader(v,static_cast<std::uint64_t>(start),static_cast<std::uint64_t>(length));}
            const auto& h=v["$blob"].get_ref<const std::string&>();auto n=h.size()/2;auto begin=std::min<std::uint64_t>(static_cast<std::uint64_t>(start-1),n);auto count=std::min<std::uint64_t>(static_cast<std::uint64_t>(length),n-begin);return json{{"$blob",h.substr(begin*2,count*2)}};
        }
        if(!group)fail("INVALID_AGGREGATE","Missing aggregation context");
        std::uint64_t count=0;json best=nullptr;mv::ExactNumeric sum;
        for(const auto& input:*group){
            query_work();const auto value=e->args[0]->kind==Expr::Star?json(1):eval(e->args[0],input);
            if(value.is_null())continue;
            if(count==std::uint64_t(std::numeric_limits<std::int64_t>::max()))fail("NUMERIC_OVERFLOW","Aggregate count exceeds SQL INTEGER");
            ++count;
            if(e->text=="COUNT")continue;
            if(e->text=="MIN"||e->text=="MAX"){
                if(best.is_null()||(e->text=="MIN"?compare_values(value,best)<0:compare_values(value,best)>0))best=value;
            }else sum.add(value);
        }
        if(e->text=="COUNT")return std::int64_t(count);
        if(e->text=="MIN"||e->text=="MAX")return best;
        if(!count)return nullptr;
        if(e->text=="AVG")return sum.real(count);
        // INTEGER SUM now checks its exact final result. A transient partial
        // sum outside int64 no longer rejects a finally representable result.
        return e->args[0]->type=="INTEGER"?json(sum.integer()):json(sum.real());
    }
    auto a=eval(e->args[0],row,group);
    if(e->kind==Expr::IsNull)return e->text=="IS NULL"?a.is_null():!a.is_null();
    if(e->kind==Expr::Unary){if(a.is_null())return nullptr;if(e->text=="NOT")return !a.get<bool>();if(e->text=="+")return a;if(a.is_number_integer()){auto n=a.get<std::int64_t>();if(n==std::numeric_limits<std::int64_t>::min())fail("NUMERIC_OVERFLOW","INTEGER unary minus overflow");return -n;}return -a.get<double>();}
    // SQL three-valued logic is preserved, including short circuiting.
    if(e->text=="AND"&&!a.is_null()&&!a.get<bool>())return false;
    if(e->text=="OR"&&!a.is_null()&&a.get<bool>())return true;
    auto b=eval(e->args[1],row,group);
    if(e->text=="AND"){if(!b.is_null()&&!b.get<bool>())return false;if(a.is_null()||b.is_null())return nullptr;return true;}
    if(e->text=="OR"){if(!b.is_null()&&b.get<bool>())return true;if(a.is_null()||b.is_null())return nullptr;return false;}
    if(a.is_null()||b.is_null())return nullptr;
    if(e->text=="+"||e->text=="-"||e->text=="*"||e->text=="/"){if(e->text!="/"&&a.is_number_integer()&&b.is_number_integer()){auto x=a.get<std::int64_t>(),y=b.get<std::int64_t>();return e->text=="+"?add_int(x,y):e->text=="-"?sub_int(x,y):mul_int(x,y);}double x=a.get<double>(),y=b.get<double>();if(e->text=="/"&&y==0)fail("DIVISION_BY_ZERO","Division by zero");double z=e->text=="+"?x+y:e->text=="-"?x-y:e->text=="*"?x*y:x/y;if(!std::isfinite(z))fail("NUMERIC_OVERFLOW","REAL arithmetic overflow");return z;}
    int c=compare_values(a,b);if(e->text=="=")return c==0;if(e->text=="!="||e->text=="<>")return c!=0;if(e->text=="<")return c<0;if(e->text==">")return c>0;if(e->text=="<=")return c<=0;if(e->text==">=")return c>=0;fail("INTERNAL_ERROR","Unknown expression operator");
}
bool matches(const E& e,const json& row){if(!e)return true;auto v=eval(e,row);return !v.is_null()&&v.get<bool>();}
std::size_t utf8_length(const std::string& s){std::size_t n=0;for(std::size_t i=0;i<s.size();++n){unsigned char c=s[i];std::size_t len=c<0x80?1:(c>=0xc2&&c<=0xdf)?2:(c>=0xe0&&c<=0xef)?3:(c>=0xf0&&c<=0xf4)?4:0;if(!len||i+len>s.size())fail("INVALID_UTF8","Invalid UTF-8 string");for(std::size_t j=1;j<len;++j)if((static_cast<unsigned char>(s[i+j])&0xc0)!=0x80)fail("INVALID_UTF8","Invalid UTF-8 continuation");if((len==3&&((c==0xe0&&static_cast<unsigned char>(s[i+1])<0xa0)||(c==0xed&&static_cast<unsigned char>(s[i+1])>=0xa0)))||(len==4&&((c==0xf0&&static_cast<unsigned char>(s[i+1])<0x90)||(c==0xf4&&static_cast<unsigned char>(s[i+1])>=0x90))))fail("INVALID_UTF8","Invalid UTF-8 code point");i+=len;}return n;}
json convert(const json& v,const json& c){
    if(v.is_null()){if(c.value("not_null",false)||c.value("primary",false))fail("NOT_NULL_VIOLATION","Column "+c["name"].get<std::string>()+" may not be NULL");return nullptr;}
    auto type=c["type"].get<std::string>();if(type=="INTEGER"){if(!v.is_number_integer()||v.is_boolean())fail("TYPE_ERROR","INTEGER value required for "+c["name"].get<std::string>());if(v.is_number_unsigned()&&v.get<std::uint64_t>()>static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))fail("NUMERIC_OVERFLOW","INTEGER outside int64 range");return v.get<std::int64_t>();}
    if(type=="REAL"){if(!v.is_number())fail("TYPE_ERROR","Numeric value required for "+c["name"].get<std::string>());double x=v.get<double>();if(!std::isfinite(x))fail("NUMERIC_OVERFLOW","Non-finite REAL");return x;}
    if(type=="BOOLEAN"){if(!v.is_boolean())fail("TYPE_ERROR","BOOLEAN value required for "+c["name"].get<std::string>());return v;}
    if(type=="BLOB"){if(!blob_value(v))fail("TYPE_ERROR","BLOB literal X'hex' required for "+c["name"].get<std::string>());auto h=upper(v["$blob"].get<std::string>());if(h.size()%2||h.find_first_not_of("0123456789ABCDEF")!=std::string::npos)fail("INVALID_BLOB","BLOB must contain even hex digits");if(h.size()/2>c.value("length",std::size_t(16777216)))fail("VALUE_TOO_LONG","BLOB byte limit exceeded");return json{{"$blob",h}};}
    if(!v.is_string())fail("TYPE_ERROR","VARCHAR value required for "+c["name"].get<std::string>());
    if(utf8_length(v.get_ref<const std::string&>())>c["length"].get<std::size_t>())fail("VALUE_TOO_LONG","VARCHAR character limit exceeded for "+c["name"].get<std::string>());
    return v;
}
int column_id(const json& table,const std::string& name){for(std::size_t i=0;i<table["columns"].size();++i)if(table["columns"][i]["name"]==name)return int(i);fail("COLUMN_NOT_FOUND","Column not found: "+name);}
Schema schema_for(const json& table,const std::string& alias){Schema cols;for(auto& c:table["columns"])cols.push_back({c["name"].get<std::string>(),c["type"].get<std::string>(),alias});return cols;}
json index_columns(const json& index){return index.contains("columns")?index["columns"]:json::array({index.at("column")});}
void normalize_index(json& ix){
    if(!ix.contains("columns"))ix["columns"]=json::array({ix.at("column")});
    if(ix["columns"].empty())fail("INVALID_SCHEMA","Index requires at least one key column");
    ix["column"]=ix["columns"][0];if(!ix.contains("include"))ix["include"]=json::array();if(!ix.contains("options"))ix["options"]=json::object();
    auto& o=ix["options"];if(!o.is_object())fail("INVALID_OPTION","Index options must be an object");
    auto m=upper(ix.value("method",std::string("BPLUS")));if(m=="DENSE")m="ORDERED_DENSE";
    if(m=="SPARSE"||m=="MULTILEVEL"){bool expected=m=="MULTILEVEL";if(o.contains("multilevel")&&o["multilevel"]!=json(expected))fail("INVALID_OPTION","Conflicting SPARSE/MULTILEVEL option");o["multilevel"]=expected;m="ORDERED_SPARSE";}
    if(m=="BITMAP_HYBRID"){if(o.contains("hybrid")&&o["hybrid"]!=json(true))fail("INVALID_OPTION","BITMAP_HYBRID requires hybrid=TRUE");o["hybrid"]=true;m="BITMAP";}ix["method"]=m;
    static const std::set<std::string> methods={"BPLUS","BTREE","ORDERED_DENSE","ORDERED_SPARSE","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR","BITMAP"};
    if(!methods.count(m))fail("UNSUPPORTED_ACCESS_METHOD","Unsupported method: "+m);
    if(o.contains("duplicates")){if(o.contains("dup_layout"))fail("DUPLICATE_OPTION","Use dup_layout or duplicates, not both");o["dup_layout"]=o["duplicates"];o.erase("duplicates");}
    const std::map<std::string,std::pair<std::uint64_t,std::uint64_t>> ints={{"max_keys",{3,255}},{"buckets",{1,65536}},{"max_depth",{1,20}},{"bitmap_threshold",{1,256}}};
    const std::set<std::string> bools={"prefix_compression","bstar","probing","hybrid","multilevel"};
    const std::map<std::string,std::set<std::string>> applicable={
        {"prefix_compression",{"BPLUS","BTREE","ORDERED_DENSE","ORDERED_SPARSE","BITMAP"}},
        {"bstar",{"BPLUS"}},{"dup_layout",{"BPLUS"}},
        {"buckets",{"HASH_STATIC","HASH_LINEAR"}},{"max_depth",{"HASH_EXTENDIBLE"}},
        {"hash",{"HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR"}},{"probing",{"HASH_STATIC"}},
        {"hybrid",{"BITMAP"}},{"bitmap_threshold",{"BITMAP"}},
        {"multilevel",{"ORDERED_DENSE","ORDERED_SPARSE"}},
        {"locator",{"BPLUS","BTREE","ORDERED_DENSE","HASH_STATIC","HASH_EXTENDIBLE","HASH_LINEAR","BITMAP"}}
    };
    for(auto it=o.begin();it!=o.end();++it){
        auto scope=applicable.find(it.key());if(scope!=applicable.end()&&!scope->second.count(m))fail("INVALID_OPTION","Option "+it.key()+" does not apply to "+m);
        auto range=ints.find(it.key());
        if(range!=ints.end()){
            if(!it.value().is_number_integer()||it.value().is_boolean()||it.value().get<std::int64_t>()<0||it.value().get<std::uint64_t>()<range->second.first||it.value().get<std::uint64_t>()>range->second.second)fail("INVALID_OPTION","Out-of-range integer option: "+it.key());
        }else if(bools.count(it.key())){
            if(!it.value().is_boolean())fail("INVALID_OPTION","Boolean option required: "+it.key());
        }else if(it.key()=="dup_layout"){
            if(!it.value().is_string())fail("INVALID_OPTION","dup_layout requires 'posting' or 'rid'");
            auto value=lower(it.value().get<std::string>());if(value=="composite")value="rid";
            if(value!="posting"&&value!="rid")fail("INVALID_OPTION","dup_layout requires 'posting' or 'rid'");
            it.value()=value;
        }else if(it.key()=="locator"){
            if(!it.value().is_string())fail("INVALID_OPTION","locator requires 'direct' or 'primary'");
            auto value=lower(it.value().get<std::string>());
            if(value!="direct"&&value!="primary")fail("INVALID_OPTION","locator requires 'direct' or 'primary'");
            it.value()=value;
        }else if(it.key()=="hash"){
            if(!it.value().is_string())fail("INVALID_OPTION","hash requires a named algorithm");
            auto value=lower(it.value().get<std::string>());
            if(value!="fnv1a64"&&value!="poly31"&&value!="first_byte"&&value!="constant")fail("INVALID_OPTION","hash requires fnv1a64, poly31, first_byte or constant");
            it.value()=value;
        }else fail("INVALID_OPTION","Unknown or unimplemented index option: "+it.key());
    }
    if(m=="BITMAP"&&o.contains("bitmap_threshold")&&o.contains("hybrid")&&!o["hybrid"].get<bool>())fail("INVALID_OPTION","bitmap_threshold requires hybrid=TRUE");
    if(m=="BPLUS"&&o.value("bstar",false)&&!o.contains("max_keys"))o["max_keys"]=31;
}

void validate_table(json& table){
    std::set<std::string> names;int primary=0;if(table["columns"].empty())fail("INVALID_SCHEMA","A table must have at least one column");
    for(auto& c:table["columns"]){auto n=c["name"].get<std::string>();if(!names.insert(n).second)fail("DUPLICATE_COLUMN","Duplicate column: "+n);if(c.value("primary",false)){++primary;c["not_null"]=true;}if((c.value("primary",false)||c.value("unique",false))&&c["type"]=="BLOB")fail("UNSUPPORTED_SQL","BLOB cannot be a key");if(!c["default"].is_null())c["default"]=convert(c["default"],c);}
    if(primary>1)fail("INVALID_SCHEMA","Declare composite PRIMARY KEY as one table constraint");
    for(auto& r:table["rows"]){if(r.size()!=table["columns"].size())fail("INTERNAL_ERROR","Stored row width does not match schema");for(std::size_t i=0;i<r.size();++i)r[i]=convert(r[i],table["columns"][i]);}
    if(!table.contains("organization"))table["organization"]={{"method","HEAP"},{"columns",json::array()},{"options",json::object()}};
    auto& org=table["organization"];std::set<std::string> orgkeys;for(auto& n:org["columns"]){auto name=n.get<std::string>();int c=column_id(table,name);if(!orgkeys.insert(name).second)fail("DUPLICATE_COLUMN","Duplicate organization key");if(table["columns"][c]["type"]=="BLOB")fail("UNSUPPORTED_SQL","BLOB cannot be organization key");}
    if(org.value("method",std::string("HEAP"))!="HEAP"){json pseudo={{"column",org["columns"][0]},{"columns",org["columns"]},{"method",org["method"]=="ORDERED"?json("ORDERED_SPARSE"):org["method"]},{"options",org.value("options",json::object())}};normalize_index(pseudo);org["options"]=pseudo["options"];if(org["options"].contains("locator")||org["options"].value("bstar",false)||org["options"].value("dup_layout",std::string("rid"))!="rid")fail("INVALID_OPTION","Organization does not accept secondary locator, B* or posting-index options");}else if(!org["options"].empty())fail("INVALID_OPTION","HEAP organization has no access options");
    std::set<std::string> indexes;for(auto& ix:table["indexes"]){normalize_index(ix);auto name=ix["name"].get<std::string>();if(!indexes.insert(name).second)fail("DUPLICATE_INDEX","Duplicate index: "+name);std::set<std::string> keys;std::vector<int> ids;
        for(auto& n:ix["columns"]){auto key=n.get<std::string>();if(!keys.insert(key).second)fail("DUPLICATE_COLUMN","Duplicate index key: "+key);int c=column_id(table,key);if(table["columns"][c]["type"]=="BLOB")fail("UNSUPPORTED_SQL","BLOB cannot be index key");ids.push_back(c);}
        for(auto& n:ix["include"]){auto key=n.get<std::string>();if(!keys.insert(key).second)fail("DUPLICATE_COLUMN","Duplicate INCLUDE/key column: "+key);if(table["columns"][column_id(table,key)]["type"]=="BLOB")fail("UNSUPPORTED_SQL","BLOB cannot be INCLUDE payload");}
        if(!ix["include"].empty()&&ix["method"]!="BPLUS"&&ix["method"]!="BTREE")fail("INVALID_INDEX","INCLUDE requires BPLUS or BTREE");
        if(ix["method"]=="BITMAP"&&(ix["columns"].size()!=1||ix.value("unique",false)||!ix["include"].empty()))fail("INVALID_INDEX","BITMAP requires one non-unique column without INCLUDE");
        if(ix["options"].contains("locator")&&org["method"]=="HEAP"&&ix["options"]["locator"]=="primary")fail("INVALID_OPTION","Primary-key locator requires an organized record file");
        if(ix["options"].value("bstar",false)){
            if(ix["method"]!="BPLUS"||!ix["include"].empty()||ix["options"].value("dup_layout",std::string("rid"))!="rid")fail("INDEX_CONFIG","B* teaching mode requires BPLUS with fixed INTEGER keys, no INCLUDE and RID entries");
            for(auto c:ids)if(table["columns"][c]["type"]!="INTEGER")fail("INDEX_CONFIG","B* teaching mode requires INTEGER keys");
        }
        if(ix["method"]=="ORDERED_SPARSE"){bool prefix=org["method"]=="ORDERED"&&org["columns"].size()>=ix["columns"].size();for(std::size_t i=0;prefix&&i<ix["columns"].size();++i)prefix=org["columns"][i]==ix["columns"][i];if(!prefix)fail("INVALID_INDEX_ORGANIZATION","Sparse index key must prefix the ORDERED organization key");if(ix["options"].value("multilevel",true)!=org["options"].value("multilevel",true))fail("INVALID_INDEX_ORGANIZATION","SPARSE/MULTILEVEL must match ORDERED organization WITH(multilevel=FALSE/TRUE)");if(ix["options"].contains("max_keys")&&ix["options"]["max_keys"]!=org["options"].value("max_keys",json(255)))fail("INVALID_INDEX_ORGANIZATION","Sparse max_keys must match its physical ORDERED file");if(ix["options"].contains("prefix_compression")&&ix["options"]["prefix_compression"]!=org["options"].value("prefix_compression",json(true)))fail("INVALID_INDEX_ORGANIZATION","Sparse prefix compression must match its physical ORDERED file");}
        if(ix.value("unique",false)){std::set<std::string> seen;for(auto& r:table["rows"]){json key=json::array();bool null=false;for(auto c:ids){auto v=r[c];null=null||v.is_null();if(v.is_number()&&v.get<double>()==0.0)v=0;key.push_back(v);}if(!null&&!seen.insert(key.dump()).second)fail("UNIQUE_VIOLATION","Duplicate key for index "+name);}}
    }
}
void add_constraint_indexes(json& table,const std::string& name){for(auto& c:table["columns"]){auto n=c["name"].get<std::string>();if(c.value("primary",false))table["indexes"].push_back({{"name","__pk_"+name},{"column",n},{"columns",json::array({n})},{"unique",true},{"primary",true}});else if(c.value("unique",false))table["indexes"].push_back({{"name","__uq_"+name+"_"+n},{"column",n},{"columns",json::array({n})},{"unique",true}});}}
std::string expression_label(const E& e){if(e->kind==Expr::Column)return e->text;if(e->kind==Expr::Function)return lower(e->text);if(e->kind==Expr::Literal)return "?column?";return "expression";}
void conjuncts(const E& e,std::vector<E>& out){if(!e)return;if(e->kind==Expr::Binary&&e->text=="AND"){conjuncts(e->args[0],out);conjuncts(e->args[1],out);}else out.push_back(e);}
void references(const E& e,std::set<int>& cols){if(e->kind==Expr::Column)cols.insert(e->bound);for(auto& a:e->args)references(a,cols);}
bool contains_plain_column(const E& e,bool in_aggregate=false){if(e->kind==Expr::Column)return !in_aggregate;for(auto& a:e->args)if(contains_plain_column(a,in_aggregate||(e->kind==Expr::Function&&aggregate_function(e->text))))return true;return false;}
json blank_result(const std::string& state="idle"){return {{"ok",true},{"columns",json::array()},{"rows",json::array()},{"affected_rows",0},{"transaction_state",state},{"plan",nullptr},{"metrics",json::object()},{"error",nullptr}};}


// Bound internal MV maintenance plan shares the parser's internal linkage.
struct MaterializedDeltaPlan {
    bool eligible=false,grouped=false;std::string reason,source,aux;
    Statement query;Schema schema;std::vector<E> aggregates;
    std::vector<mv::AggregateSpec> specs;
};
