#include "materialized_delta.hpp"
#include <iostream>
using edudb::json;
int main(){
    std::string line;
    while(std::getline(std::cin,line))try{
        const auto request=json::parse(line);edudb::mv::ExactNumeric sum;
        for(const auto& item:request["values"])sum.add(item);
        auto restored=edudb::mv::ExactNumeric::restore(json::parse(sum.state().dump()));
        if(request.value("integer",false))std::cout<<json{{"ok",true},{"value",restored.integer()}}.dump()<<"\n";
        else std::cout<<json{{"ok",true},{"value",restored.real(request.value("denominator",std::uint64_t(1)))}}.dump()<<"\n";
    }catch(const edudb::DbError& e){std::cout<<json{{"ok",false},{"error",e.code}}.dump()<<"\n";}
     catch(const std::exception& e){std::cout<<json{{"ok",false},{"error","UNEXPECTED_EXCEPTION"},{"message",e.what()}}.dump()<<"\n";}
}
