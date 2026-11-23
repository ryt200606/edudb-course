#pragma once
#include "edudb.hpp"
#include <set>
#include <limits>
namespace edudb {
// Transaction-private teaching change stream. Encoded BLOB references are
// retained, never copied as multi-megabyte object bodies. It is not CDC across
// commits or an audit/security log. The transaction's mutex protects access.
struct ChangeFeedSnapshot {
    std::set<std::string> tables;
    json events=json::array();
    std::size_t bytes=0;
};
class TransactionChangeFeed {
    ChangeFeedSnapshot state_;
    std::uint64_t sequence_=0;
    std::size_t limit_;
public:
    explicit TransactionChangeFeed(std::size_t limit=16u*1024u*1024u):limit_(limit){}
    bool enabled(const std::string& table)const{return state_.tables.count(table)!=0;}
    void enable(const std::vector<std::string>& tables){for(const auto& table:tables){if(table.empty()||table.size()>128)throw DbError("INVALID_IDENTIFIER","Changefeed table name is invalid");if(!state_.tables.count(table)&&state_.tables.size()>=4096)throw DbError("CHANGEFEED_LIMIT","At most4096 changefeed subscriptions per transaction");state_.tables.insert(table);}}
    void emit(const std::string& table,const std::string& kind,std::uint64_t id,const json& old_row,const json& new_row){
        if(!enabled(table))return;
        if(kind!="RESET"&&kind!="INSERT"&&kind!="UPDATE"&&kind!="DELETE")throw DbError("INVALID_CHANGEFEED","Unknown native mutation kind");
        if(kind!="RESET"&&old_row==new_row)return;
        if(sequence_==std::numeric_limits<std::uint64_t>::max())throw DbError("CHANGEFEED_LIMIT","Changefeed sequence exhausted");
        json event={{"seq",sequence_+1},{"table",table},{"kind",kind},{"logical_id",id},{"old_row",old_row},{"new_row",new_row}};
        const auto bytes=event.dump().size();if(state_.bytes>limit_||bytes>limit_-state_.bytes)throw DbError("CHANGEFEED_LIMIT","Transaction changefeed exceeds its encoded-event budget");
        state_.events.push_back(std::move(event));++sequence_;state_.bytes+=bytes;
    }
    json read(const std::string& table,std::uint64_t after)const{json events=json::array();for(const auto& event:state_.events)if(event.at("seq").get<std::uint64_t>()>after&&(table.empty()||event.at("table")==table))events.push_back(event);return {{"events",events},{"last_seq",sequence_},{"encoded_bytes",state_.bytes},{"limit_bytes",limit_},{"row_scope","typed encoded rows; BLOB values are lazy external references"}};}
    ChangeFeedSnapshot snapshot()const{return state_;}
    void restore(const ChangeFeedSnapshot& saved){if(saved.bytes>limit_||!saved.events.is_array())throw DbError("INVALID_CHANGEFEED","Invalid saved change stream");state_=saved;/* Never rewind sequence_, including rollback-to. */}
    std::size_t snapshot_bytes()const{std::size_t n=state_.events.dump().size();for(const auto& t:state_.tables)n+=t.size();return n;}
    void clear(){state_=ChangeFeedSnapshot{};}
};
}
