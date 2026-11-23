#include "tree_control.hpp"
#include "edudb.hpp"
namespace edudb {
LockTreeTopology::LockTreeTopology(std::string name,std::string pk,std::string parent,std::map<std::int64_t,std::optional<std::int64_t>> edges):table(std::move(name)),key(std::move(pk)),parent_column(std::move(parent)),parents(std::move(edges)){
    if(parents.empty()||parents.size()>100000)throw DbError("TREE_TOPOLOGY","Topology needs1..100000 records");
    unsigned roots=0;for(const auto& e:parents){if(!e.second)++roots;else if(!parents.count(*e.second))throw DbError("TREE_TOPOLOGY","Parent record does not exist");}
    if(roots!=1)throw DbError("TREE_TOPOLOGY","Exactly one NULL-parent root is required");
    std::set<std::int64_t> done;
    for(const auto& e:parents){std::set<std::int64_t> path;auto node=std::optional<std::int64_t>(e.first);while(node&&!done.count(*node)){if(!path.insert(*node).second)throw DbError("TREE_TOPOLOGY","Cycle in static logical tree");node=parents.at(*node);}done.insert(path.begin(),path.end());}
}
std::string LockTreeTopology::resource(std::int64_t node)const{return "tree:"+table+":"+std::to_string(node);}
void LockTreeState::before_lock(std::int64_t node)const{
    if(!topology)throw DbError("TREE_NOT_OPEN","Use TREE OPEN first");
    auto p=topology->parents.find(node);if(p==topology->parents.end())throw DbError("TREE_NODE_NOT_FOUND","Node is not in the frozen topology");
    if(released.count(node))throw DbError("TREE_REACQUIRE","Released node cannot be acquired again in this transaction");
    if(held.count(node))throw DbError("TREE_ALREADY_LOCKED","Each node can be acquired at most once");
    if((!held.empty()||!released.empty())&&(!p->second||!held.count(*p->second)))throw DbError("TREE_PARENT_NOT_HELD","After the first lock the parent must currently be X-locked");
}
void LockTreeState::granted(std::int64_t node){before_lock(node);held.insert(node);}
void LockTreeState::before_unlock(std::int64_t node)const{
    if(!held.count(node))throw DbError("TREE_NOT_LOCKED","Node is not currently X-locked");
    if(written.count(node))throw DbError("TREE_WRITTEN_HELD","Written nodes must remain locked until commit or rollback");
}
void LockTreeState::unlocked(std::int64_t node){released.insert(node);held.erase(node);}
void LockTreeState::access(std::int64_t node,bool write){if(!topology||!held.count(node))throw DbError("TREE_NOT_LOCKED","SQL access requires the matching node X lock");if(write)written.insert(node);}
void LockTreeState::clear(){topology.reset();held.clear();released.clear();written.clear();}
nlohmann::json LockTreeState::diagnostics()const{return {{"table",topology?nlohmann::json(topology->table):nlohmann::json(nullptr)},{"held_x_nodes",held},{"released_never_reacquire",released},{"written_held_to_end",written},{"scope","static logical row tree; first lock free, later parent held, early unlock only unwritten nodes"}};}
}
