#include "btree.hpp"
#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <set>
namespace edudb {
BPlusTree::BPlusTree(std::size_t maximum):maximum_(std::max<std::size_t>(3,maximum)){add(true);}
int BPlusTree::add(bool leaf){Node n;n.leaf=leaf;nodes_.push_back(std::move(n));return static_cast<int>(nodes_.size()-1);}
BPlusTree::Key BPlusTree::first(int id)const {
    while(!nodes_[id].leaf)id=nodes_[id].children.front();
    return nodes_[id].keys.empty()?Key{0,0}:nodes_[id].keys.front();
}
void BPlusTree::refresh(int id){auto& n=nodes_[id];if(n.leaf)return;n.keys.clear();for(std::size_t i=1;i<n.children.size();++i)n.keys.push_back(first(n.children[i]));}
int BPlusTree::insert_node(int id,Key k){
    if(nodes_[id].leaf){
        auto& ks=nodes_[id].keys;auto it=std::lower_bound(ks.begin(),ks.end(),k);if(it!=ks.end()&&*it==k)return -1;ks.insert(it,k);
        if(ks.size()<=maximum_)return -1;
        int r=add(true);auto mid=nodes_[id].keys.size()/2;
        nodes_[r].keys.assign(nodes_[id].keys.begin()+mid,nodes_[id].keys.end());nodes_[id].keys.resize(mid);
        nodes_[r].next=nodes_[id].next;nodes_[id].next=r;return r;
    }
    auto idx=static_cast<std::size_t>(std::upper_bound(nodes_[id].keys.begin(),nodes_[id].keys.end(),k)-nodes_[id].keys.begin());
    int r=insert_node(nodes_[id].children[idx],k);
    if(r>=0)nodes_[id].children.insert(nodes_[id].children.begin()+idx+1,r);
    refresh(id);if(nodes_[id].keys.size()<=maximum_)return -1;
    r=add(false);auto mid=nodes_[id].children.size()/2;
    nodes_[r].children.assign(nodes_[id].children.begin()+mid,nodes_[id].children.end());nodes_[id].children.resize(mid);
    refresh(id);refresh(r);return r;
}
void BPlusTree::insert(std::int64_t key,std::uint64_t rid){int r=insert_node(root_,{key,rid});if(r>=0){int old=root_;root_=add(false);nodes_[root_].children={old,r};refresh(root_);}}
void BPlusTree::repair(int p,std::size_t idx){
    int c=nodes_[p].children[idx];bool leaf=nodes_[c].leaf;
    auto size=[&](int id){return leaf?nodes_[id].keys.size():nodes_[id].children.size();};
    std::size_t minimum=leaf?(maximum_+1)/2:(maximum_+2)/2;
    if(size(c)>=minimum)return;
    if(idx>0){int left=nodes_[p].children[idx-1];if(size(left)>minimum){
        if(leaf){nodes_[c].keys.insert(nodes_[c].keys.begin(),nodes_[left].keys.back());nodes_[left].keys.pop_back();}
        else{nodes_[c].children.insert(nodes_[c].children.begin(),nodes_[left].children.back());nodes_[left].children.pop_back();refresh(left);refresh(c);}
        refresh(p);return;
    }}
    if(idx+1<nodes_[p].children.size()){int right=nodes_[p].children[idx+1];if(size(right)>minimum){
        if(leaf){nodes_[c].keys.push_back(nodes_[right].keys.front());nodes_[right].keys.erase(nodes_[right].keys.begin());}
        else{nodes_[c].children.push_back(nodes_[right].children.front());nodes_[right].children.erase(nodes_[right].children.begin());refresh(right);refresh(c);}
        refresh(p);return;
    }}
    if(nodes_[p].children.size()<2)return;
    std::size_t bi=idx?idx:idx+1;int a=nodes_[p].children[bi-1],b=nodes_[p].children[bi];
    if(leaf){nodes_[a].keys.insert(nodes_[a].keys.end(),nodes_[b].keys.begin(),nodes_[b].keys.end());nodes_[a].next=nodes_[b].next;}
    else{nodes_[a].children.insert(nodes_[a].children.end(),nodes_[b].children.begin(),nodes_[b].children.end());refresh(a);}
    nodes_[b].alive=false;nodes_[p].children.erase(nodes_[p].children.begin()+bi);refresh(p);
}
bool BPlusTree::erase_node(int id,Key k){
    if(nodes_[id].leaf){auto& ks=nodes_[id].keys;auto it=std::lower_bound(ks.begin(),ks.end(),k);if(it==ks.end()||*it!=k)return false;ks.erase(it);return true;}
    auto idx=static_cast<std::size_t>(std::upper_bound(nodes_[id].keys.begin(),nodes_[id].keys.end(),k)-nodes_[id].keys.begin());
    bool ok=erase_node(nodes_[id].children[idx],k);if(ok){repair(id,idx);refresh(id);}return ok;
}
bool BPlusTree::erase(std::int64_t k,std::uint64_t rid){bool ok=erase_node(root_,{k,rid});if(!nodes_[root_].leaf&&nodes_[root_].children.size()==1){int old=root_;root_=nodes_[old].children[0];nodes_[old].alive=false;}return ok;}
std::vector<std::uint64_t> BPlusTree::search(const std::string& op,std::int64_t key)const{
    std::vector<std::uint64_t> out;int id=root_;
    if(op=="="||op==">"||op==">="){
        Key lo{key,0};while(!nodes_[id].leaf){auto i=std::lower_bound(nodes_[id].keys.begin(),nodes_[id].keys.end(),lo)-nodes_[id].keys.begin();id=nodes_[id].children[static_cast<std::size_t>(i)];}
    }else while(!nodes_[id].leaf)id=nodes_[id].children[0];
    for(;id>=0;id=nodes_[id].next)for(auto e:nodes_[id].keys){
        bool match=op=="="?e.first==key:op=="<"?e.first<key:op=="<="?e.first<=key:op==">"?e.first>key:op==">="?e.first>=key:op=="!="||op=="<>"?e.first!=key:false;
        if(match)out.push_back(e.second);
        if((op=="="||op=="<"||op=="<=")&&e.first>key)return out;
    }return out;
}
std::vector<std::pair<std::int64_t,std::vector<std::uint64_t>>> BPlusTree::entries()const{
    std::vector<std::pair<std::int64_t,std::vector<std::uint64_t>>> out;int id=root_;while(!nodes_[id].leaf)id=nodes_[id].children[0];
    for(;id>=0;id=nodes_[id].next) {
        for(auto e:nodes_[id].keys) {
            if(out.empty()||out.back().first!=e.first)out.push_back({e.first,{}});
            out.back().second.push_back(e.second);
        }
    }
    return out;
}
bool BPlusTree::validate()const{
    try{
        std::set<int> seen;std::vector<int> leaves;int depth=-1;
        std::function<bool(int,int,bool)> visit=[&](int id,int d,bool root){
            if(id<0||static_cast<std::size_t>(id)>=nodes_.size()||!seen.insert(id).second||!nodes_[id].alive)return false;
            auto& n=nodes_[id];if(n.keys.size()>maximum_||!std::is_sorted(n.keys.begin(),n.keys.end())||std::adjacent_find(n.keys.begin(),n.keys.end())!=n.keys.end())return false;
            if(n.leaf){if(!root&&n.keys.size()<(maximum_+1)/2)return false;if(depth<0)depth=d;if(depth!=d)return false;leaves.push_back(id);return true;}
            if(n.children.size()!=n.keys.size()+1||n.children.size()<(root?2:(maximum_+2)/2))return false;
            for(std::size_t i=0;i<n.children.size();++i){if(!visit(n.children[i],d+1,false))return false;if(i&&n.keys[i-1]!=first(n.children[i]))return false;}
            return true;
        };
        if(!visit(root_,0,true))return false;
        Key last{};bool have=false;
        for(std::size_t i=0;i<leaves.size();++i){auto& n=nodes_[leaves[i]];if(n.next!=(i+1<leaves.size()?leaves[i+1]:-1))return false;for(auto k:n.keys){if(have&&!(last<k))return false;last=k;have=true;}}
        return true;
    }catch(...){return false;}
}
json BPlusTree::stats()const{std::size_t count=0,leaf=0,entries_count=0;for(auto& n:nodes_)if(n.alive){++count;if(n.leaf){++leaf;entries_count+=n.keys.size();}}int height=1,id=root_;while(!nodes_[id].leaf){++height;id=nodes_[id].children[0];}return json{{"nodes",count},{"leaf_nodes",leaf},{"entries",entries_count},{"height",height},{"valid",validate()},{"max_keys",maximum_}};}
json BPlusTree::serialize()const{
    std::vector<int> order;std::map<int,int> map;
    std::function<void(int)> visit=[&](int id){map[id]=static_cast<int>(order.size());order.push_back(id);for(int c:nodes_[id].children)visit(c);};visit(root_);
    json result={{"root",0},{"max_keys",maximum_},{"nodes",json::array()}};
    for(int id:order){auto& n=nodes_[id];json j={{"leaf",n.leaf},{"keys",json::array()}};for(auto k:n.keys)j["keys"].push_back(k.first);
        if(n.leaf){j["values"]=json::array();for(auto k:n.keys)j["values"].push_back(json::array({k.second}));j["next"]=n.next<0?-1:map.at(n.next);}
        else{j["children"]=json::array();for(int c:n.children)j["children"].push_back(map.at(c));j["pivots"]=json::array();for(auto k:n.keys)j["pivots"].push_back(k.second);}
        result["nodes"].push_back(j);
    }return result;
}
BPlusTree BPlusTree::deserialize(const json& j){BPlusTree t(j.at("max_keys").get<std::size_t>());for(auto& n:j.at("nodes"))if(n.at("leaf").get<bool>())for(std::size_t i=0;i<n.at("keys").size();++i)for(auto& r:n.at("values").at(i))t.insert(n.at("keys").at(i).get<std::int64_t>(),r.get<std::uint64_t>());if(!t.validate())throw DbError("INDEX_CORRUPT","Invalid B+ tree");return t;}
}
