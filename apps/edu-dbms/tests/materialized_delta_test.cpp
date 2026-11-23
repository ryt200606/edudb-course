#include "materialized_delta.hpp"
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
using edudb::json;
static int checks=0;
static void check(bool value,const std::string& text){++checks;if(!value)throw std::runtime_error(text);}
template<class F>static void error(F call,const std::string& code){++checks;try{call();}catch(const edudb::DbError& e){if(e.code==code)return;throw;}throw std::runtime_error("Expected "+code);}
int main(){try{
    const std::vector<edudb::mv::AggregateSpec> specs={{"COUNT","INTEGER"},{"COUNT","INTEGER"},{"SUM","INTEGER"},{"AVG","INTEGER"},{"SUM","REAL"},{"AVG","REAL"}};
    edudb::mv::AggregateGroup group(specs);
    check(group.values()==json::array({0,0,nullptr,nullptr,nullptr,nullptr}),"Empty aggregate outputs");
    group.change(json::array({1,nullptr,nullptr,nullptr,nullptr,nullptr}),1);
    group.change(json::array({1,2,2,2,2.0,2.0}),1);group.change(json::array({1,4,4,4,4.0,4.0}),1);
    check(group.values()==json::array({3,2,6,3.0,6.0,3.0}),"COUNT nulls and numeric aggregates");
    auto restored=edudb::mv::AggregateGroup::restore(json::parse(group.state().dump()),specs);restored.change(json::array({1,2,2,2,2.0,2.0}),-1);
    check(restored.values()==json::array({2,1,4,4.0,4.0,4.0}),"Persistent state removal");
    restored.change(json::array({1,4,4,4,4.0,4.0}),-1);restored.change(json::array({1,nullptr,nullptr,nullptr,nullptr,nullptr}),-1);
    check(restored.rows()==0&&restored.values()==json::array({0,0,nullptr,nullptr,nullptr,nullptr}),"Last nonnull and last group row removal");
    auto before=restored.state();error([&]{restored.change(json::array({1,1,1,1,1,1}),-1);},"MATERIALIZED_STATE");check(restored.state()==before,"Rejected delta keeps previous group state");
    const auto maximum=std::numeric_limits<double>::max();edudb::mv::ExactNumeric sum;sum.add(maximum);sum.add(maximum);
    check(sum.real(2)==maximum,"Finite AVG does not inherit double SUM overflow");error([&]{sum.real();},"NUMERIC_OVERFLOW");sum.add(maximum,-1);check(sum.real()==maximum,"Exact removal after overflowing SUM magnitude");
    edudb::mv::ExactNumeric cancellation;cancellation.add(1e300);cancellation.add(1);cancellation.add(-1e300);check(cancellation.real()==1,"Exact catastrophic-cancellation protection");
    edudb::mv::ExactNumeric edge;edge.add(std::numeric_limits<std::int64_t>::min());check(edge.integer()==std::numeric_limits<std::int64_t>::min(),"INT64_MIN exact");edge.add(-1);error([&]{edge.integer();},"NUMERIC_OVERFLOW");edge.add(-1,-1);edge.add(std::numeric_limits<std::int64_t>::min(),-1);check(edge.integer()==0,"Integer cancellation and inverse changes");
    edudb::mv::ExactNumeric tiny;tiny.add(-std::numeric_limits<double>::denorm_min());check(tiny.real(3)==0.0&&std::signbit(tiny.real(3)),"Negative subnormal AVG rounds to negative zero");
    error([&]{edudb::mv::ExactNumeric::restore(nullptr);},"MATERIALIZED_STATE");auto bad=sum.state();bad["magnitude_hex"]="bad";error([&]{edudb::mv::ExactNumeric::restore(bad);},"MATERIALIZED_STATE");
    std::cout<<"PASS "<<checks<<" exact incremental aggregate unit checks; SQL/feed integration not claimed\n";
}catch(const std::exception& e){std::cerr<<"FAIL "<<checks<<" "<<e.what()<<"\n";return 1;}}
