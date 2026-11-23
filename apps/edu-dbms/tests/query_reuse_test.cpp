#include "query_reuse.hpp"
#include <iostream>
#include <random>
#include <stdexcept>
#ifdef _WIN32
#include <windows.h>
#endif
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& text){++checks;if(!value)throw std::runtime_error(text);}
static auto input(const std::vector<json>& data){return edudb::query::scan([&data](){auto at=std::make_shared<std::size_t>(0);return edudb::query::Pull([&data,at](json& row){if(*at>=data.size())return false;row=data[(*at)++];return true;});},"IndependentSingleSource");}
int main(){
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
#endif
    try{std::mt19937_64 random(20261102);const std::vector<json> values={nullptr,-4,0,1,9007199254740993ll,1.0,true,false,"中文",json{{"$blob","00FF"}}};
    for(int trial=0;trial<250;++trial){const std::size_t width=1+random()%20,from=random()%2?width:0,to=width-from;edudb::query::AliasCopySpec spec;spec.from=from;spec.to=to;spec.width=width;spec.total_width=width*2;for(std::size_t slot=0;slot<width*2;++slot)if(random()%2)spec.required_slots.push_back(static_cast<int>(slot));std::vector<json> rows;json expected=json::array();
        for(int n=0;n<15;++n){json row=json::array();for(std::size_t slot=0;slot<width*2;++slot)row.push_back(nullptr);std::vector<json> physical;for(std::size_t col=0;col<width;++col){auto value=values[random()%values.size()];physical.push_back(value);row[from+col]=value;}json model=json::array();for(std::size_t slot=0;slot<width*2;++slot)model.push_back(nullptr);for(int slot:spec.required_slots)model[slot]=physical[static_cast<std::size_t>(slot)%width];rows.push_back(std::move(row));expected.push_back(std::move(model));}
        edudb::query::ExecContext context;context.memory_limit=65536;auto node=edudb::query::alias_copy(input(rows),spec);node->open(context);json result=json::array(),row;while(node->next(row))result.push_back(row);node->close();check(result==expected,"Required fields reconstruct both aliases without copying unused values");auto actual=node->diagnostics();check(actual["actual_rows"]==15&&actual["pairs_examined"]==0&&actual["source_operators"]==1,"Real alias operator emits one row per input without join pairs");node->open(context);result=json::array();while(node->next(row))result.push_back(row);node->close();check(result==expected,"Repeated open/close preserves order and duplicate multiplicity");
    }
    {edudb::query::AliasCopySpec spec;spec.from=0;spec.to=1;spec.width=2;spec.total_width=4;bool caught=false;try{edudb::query::alias_copy(input({}),spec);}catch(const edudb::DbError&e){caught=e.code=="INVALID_OPERATOR";}check(caught,"Overlapping ranges reject before any copy");}
    {edudb::query::AliasCopySpec spec;spec.total_width=std::numeric_limits<std::size_t>::max();bool caught=false;try{edudb::query::alias_copy(nullptr,spec);}catch(const edudb::DbError&e){caught=e.code=="INVALID_OPERATOR";}check(caught,"Invalid huge schema/null source rejects before mask allocation");}
    {std::vector<json> rows={json::array({std::string(5000,'x'),nullptr})};edudb::query::AliasCopySpec spec;spec.from=0;spec.to=1;spec.width=1;spec.total_width=2;spec.required_slots={1};auto node=edudb::query::alias_copy(input(rows),spec);edudb::query::ExecContext context;context.memory_limit=4096;bool caught=false;try{node->open(context);json row;node->next(row);}catch(const edudb::DbError&e){caught=e.code=="QUERY_OPERATOR_MEMORY";}node->close();check(caught,"Scratch estimate fails safely");context.memory_limit=65536;node->open(context);json row;check(node->next(row)&&row[0].is_null()&&row[1].get<std::string>().size()==5000,"Resource error can close and reopen the same operator");node->close();}
    std::cout<<"PASS "<<checks<<" actual alias reconstruction/order/budget/configuration checks\n";
    }catch(const std::exception&e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<"\n";return 1;}
}
