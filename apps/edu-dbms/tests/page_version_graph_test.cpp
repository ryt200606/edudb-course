#include "page_version_graph.hpp"
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <tuple>
using G=edudb::PageVersionGraph;
using Edge=std::tuple<std::uint64_t,std::uint64_t,std::string,std::uint64_t,std::uint64_t,std::uint64_t>;
int checks=0;
void check(bool yes,const char* why){++checks;if(!yes)throw std::runtime_error(why);}
std::set<Edge> edges(const G& graph){std::set<Edge> out;auto snapshot=graph.diagnostics();for(const auto& e:snapshot["edges"])out.emplace(e["from"],e["to"],e["kind"],e["page"],e["source_page_lsn"],e["target_page_lsn"]);return out;}
void known(){
 G g;g.begin(1,0);g.begin(2,0);g.read(1,10,0,4);g.read(2,20,0,4);
 g.publish(2,1,{{10,0,2,4}});g.finish(2,"COMMITTED");
 g.publish(1,2,{{20,0,4,4}});g.finish(1,"COMMITTED");
 auto e=edges(g);check(e.count({1,2,"RW",10,0,2}),"missing first actual write-skew anti-edge");check(e.count({2,1,"RW",20,0,4}),"missing second actual write-skew anti-edge");check(g.diagnostics()["committed_cycle"].size()==3,"write skew cycle absent");
 g.begin(3,0);g.read(3,10,0,4);check(edges(g).count({3,2,"RW",10,0,2}),"late old-snapshot reader must point to committed successor");
 g.finish(3,"ABORTED");for(const auto& edge:edges(g))check(std::get<0>(edge)!=3&&std::get<1>(edge)!=3,"aborted reader retained");
 g.begin(4,2);g.read(4,10,2,4);g.publish(4,3,{{10,2,6,4}});g.finish(4,"COMMITTED");
 check(edges(g).count({2,4,"WR",10,2,2}),"read-from WR absent");check(edges(g).count({2,4,"WW",10,2,6}),"adjacent-version WW absent");
 g.begin(5,3);g.finish(5,"UNKNOWN");auto d=g.diagnostics();check(d["truncated"],"UNKNOWN reported as complete definite graph");check(d["unknown_initial_versions"].get<std::size_t>()==2,"pre-start creators must remain unknown");
 G small({2,3,3,3});small.begin(1,0);small.finish(1,"COMMITTED");small.begin(2,0);small.finish(2,"COMMITTED");small.begin(3,0);check(small.diagnostics()["truncated"]&&small.diagnostics()["pruned_transactions"]==1,"bounded pruning hidden");
 G bounded({2,2,2,2});bounded.begin(1,0);bounded.read(1,1,0,4);bounded.read(1,2,0,4);bounded.read(1,3,0,4);check(bounded.diagnostics()["disabled"]&&bounded.diagnostics()["truncated"],"budget overflow must disable observation without throwing");
}
struct Version{std::uint64_t page,lsn,csn,creator;};
struct Read{std::uint64_t tx,page,lsn;};
void random_oracle(){
 std::mt19937 rng(710016);
 for(int repetition=0;repetition<250;++repetition){
  G g;std::vector<Version> versions;for(int page=1;page<=3;++page)versions.push_back({static_cast<unsigned>(page),0,0,0});
  std::vector<Read> reads;std::set<std::uint64_t> active,committed;std::uint64_t csn=0,lsn=0;
  for(std::uint64_t tx=1;tx<=8;++tx){active.insert(tx);g.begin(tx,0);}
  for(int step=0;step<80&&!active.empty();++step){
   auto selected=active.begin();std::advance(selected,rng()%active.size());auto tx=*selected;
   if(rng()%4){auto v=versions[rng()%versions.size()];g.read(tx,v.page,v.lsn,4);reads.push_back({tx,v.page,v.lsn});}
   else {if(rng()%5){auto page=1+rng()%3;Version previous{};for(const auto& v:versions)if(v.page==page&&v.csn>=previous.csn)previous=v;
      g.publish(tx,++csn,{{page,previous.lsn,++lsn,4}});versions.push_back({page,lsn,csn,tx});g.finish(tx,"COMMITTED");committed.insert(tx);
     }else g.finish(tx,"ABORTED");active.erase(selected);}
  }
  for(auto tx:active){g.finish(tx,"COMMITTED");committed.insert(tx);}
  std::set<Edge> expected;
  // Offline oracle reconstructs the entire finite schedule from observations,
  // without maintaining online reader lists or mutating the implementation.
  for(const auto& r:reads)if(committed.count(r.tx)){
   const Version* chosen=nullptr;for(const auto& v:versions)if(v.page==r.page&&v.lsn==r.lsn)chosen=&v;
   check(chosen!=nullptr,"oracle missing selected version");
   if(chosen->creator&&chosen->creator!=r.tx)expected.emplace(chosen->creator,r.tx,"WR",r.page,r.lsn,r.lsn);
   for(const auto& later:versions)if(later.page==r.page&&later.csn>chosen->csn&&later.creator!=r.tx)expected.emplace(r.tx,later.creator,"RW",r.page,r.lsn,later.lsn);
  }
  for(const auto& newer:versions)if(newer.creator){const Version* prior=nullptr;for(const auto& old:versions)if(old.page==newer.page&&old.csn<newer.csn&&(!prior||old.csn>prior->csn))prior=&old;if(prior&&prior->creator&&prior->creator!=newer.creator)expected.emplace(prior->creator,newer.creator,"WW",newer.page,prior->lsn,newer.lsn);}
  check(edges(g)==expected,"online page version graph differs from independent offline schedule oracle");check(!g.diagnostics()["truncated"]&&!g.diagnostics()["disabled"],"small valid schedule truncated");
 }
}
void rollback_alias(){
 G g;g.begin(10,0);g.publish(10,1,{{1,0,2,4}});g.finish(10,"COMMITTED");
 g.begin(20,1);g.read(20,1,2,4);g.restore_alias(1,2,8,4);g.finish(20,"ABORTED");
 g.begin(30,1);g.read(30,1,8,4);g.finish(30,"COMMITTED");auto e=edges(g);
 check(e.count({10,30,"WR",1,8,8}),"CLR alias lost original committed creator");check(!e.count({30,10,"RW",1,8,2}),"CLR alias created a false anti-edge to already visible writer");
 auto d=g.diagnostics();check(!d["disabled"]&&d["invalid_events"]==0,"valid rollback alias rejected");
 g.begin(40,1);g.publish(40,2,{{1,8,9,4}});g.finish(40,"COMMITTED");check(edges(g).count({10,40,"WW",1,8,9}),"post-undo writer lost original committed predecessor");
}
void external_epoch(){edudb::PageVersionGraph g;g.begin(1,0);g.publish(1,1,{{1,0,2,4}});g.finish(1,"COMMITTED");g.reset_epoch("physical standby replay");auto cleared=g.diagnostics();check(cleared["epoch_resets"]==1&&cleared["nodes"].empty()&&cleared["versions"].empty(),"external replay retained old graph evidence");g.begin(2,1);g.read(2,1,9,4);g.finish(2,"COMMITTED");auto observed=g.diagnostics();check(observed["edges"].empty()&&!observed["disabled"]&&observed["unknown_creator_versions"]==1,"external version invented creator or dependency");}
int main(){try{known();random_oracle();rollback_alias();external_epoch();std::cout<<"PASS actual page version dependency graph "<<checks<<" checks\n";return 0;}catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}}
