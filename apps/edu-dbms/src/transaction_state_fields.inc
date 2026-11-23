// Included inside Transaction::State, after its existing page/descriptor fields.
TransactionControl::Handle protocol;
std::map<PageId,std::uint64_t> protocol_write_base_lsn;
std::set<PageId> protocol_allocated_pages;
struct ProtocolSavepoint {
    std::string name;
    std::map<PageId,std::uint16_t> aries_pages;
    std::unordered_map<PageId,Page> pages;
    std::unordered_set<PageId> new_pages;
    std::unordered_map<PageId,json> descriptors;
    std::unordered_map<std::string,PageId> descriptor_names;
    std::set<PageId> dirty_descriptors;
    std::unordered_map<PageId,std::map<std::uint64_t,PageId>> moved_rows;
    std::map<PageId,std::uint64_t> write_base_lsn;
    std::set<PageId> allocated_pages,layout_touched;
    std::set<std::string> write_tables,ignored_write_tables;
    std::uint64_t read_csn=0;
    std::size_t accounted_bytes=0;
    ChangeFeedSnapshot changefeed;
};
std::vector<ProtocolSavepoint> protocol_savepoints;
std::size_t protocol_savepoint_bytes=0;
