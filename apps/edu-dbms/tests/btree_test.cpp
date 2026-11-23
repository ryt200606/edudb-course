#include "btree.hpp"
#include <iostream>
#include <map>
#include <random>
#include <set>
using namespace edudb;
static void check(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}
int main(){try{
    BPlusTree tree(7);std::set<std::pair<std::int64_t,std::uint64_t>> model;std::mt19937 rng(20261028);
    for(int i=0;i<6000;++i){std::int64_t k=static_cast<int>(rng()%101)-50;std::uint64_t rid=rng()%1700;
        if(rng()%3){tree.insert(k,rid);model.insert({k,rid});}else check(tree.erase(k,rid)==(model.erase({k,rid})!=0),"erase mismatch");
        check(tree.validate(),"B+ tree invariant failure");
        if(i%50==0)for(auto op:{"=","<","<=",">",">=","<>"}){std::vector<std::uint64_t> expected;for(auto e:model){bool m=std::string(op)=="="?e.first==k:std::string(op)=="<"?e.first<k:std::string(op)=="<="?e.first<=k:std::string(op)==">"?e.first>k:std::string(op)==">="?e.first>=k:e.first!=k;if(m)expected.push_back(e.second);}check(tree.search(op,k)==expected,"range mismatch");}
    }
    auto copied=BPlusTree::deserialize(tree.serialize());check(copied.entries()==tree.entries(),"serialization mismatch");
    for(auto e:model){check(tree.erase(e.first,e.second),"existing entry missing");check(tree.validate(),"merge/root shrink invariant");}
    check(tree.entries().empty(),"tree not empty");check(tree.stats()["height"]==1,"root did not shrink");
    BPlusTree duplicates(5);for(int i=0;i<1500;++i)duplicates.insert(7,i);check(duplicates.search("=",7).size()==1500,"duplicate key leaf chain");check(duplicates.validate(),"duplicate tree invalid");
    std::cout<<json{{"status","PASS"},{"random_operations",6000},{"duplicate_entries",1500},{"empty_tree",tree.stats()}}.dump()<<"\n";return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}}
