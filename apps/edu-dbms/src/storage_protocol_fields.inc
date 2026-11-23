// Included inside Storage::Impl, after Frame is declared and before methods.
TransactionControl protocols;
PageVersionGraph version_graph;
PageMvto mvto;
struct ProtocolPageVersion {std::uint64_t csn=0;Page image;};
std::map<PageId,std::deque<ProtocolPageVersion>> protocol_history;
std::map<PageId,std::uint64_t> protocol_page_csn,protocol_page_birth;
std::size_t protocol_history_bytes=0;
std::size_t protocol_history_limit=64u*1024u*1024u;
std::size_t protocol_savepoint_limit=32u*1024u*1024u;
bool protocol_configured=false;
struct ProtocolCommit {
    TransactionControl::CommitPermit permit;
    std::vector<std::pair<PageId,Page>> previous_pages;
    std::vector<PageVersionGraph::Write> graph_writes;
};
