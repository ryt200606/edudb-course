#pragma once
#include "edudb.hpp"
#include <string>
#include <vector>

namespace edudb::interchange {
struct Cell { std::string text; bool is_null = false; };
using CsvRows = std::vector<std::vector<Cell>>;
// RFC 4180 quoting; unquoted \N is NULL, while quoted "\N" is text.
// Input is UTF-8 (an initial BOM is accepted); invalid UTF-8/NUL is rejected.
// Resource bounds: 64 MiB input, 1024 fields/row, 1M rows, 2M cells;
// additionally 128 MiB conservative cumulative parse allocation estimate,
// including input bytes, decoded characters, cells and row-vector overhead.
// The allocation estimate is distinct from upload size and process RSS.
CsvRows parse_csv(const std::string& content);
std::string encode_csv(const json& columns, const json& rows, bool header = true);
std::string quote_identifier(const std::string& name);
std::string sql_literal(const json& value);
std::string dump_table(const std::string& name, const json& table);
// The SQL script has the same 64 MiB input bound and at most 100K statements.
std::vector<std::string> split_sql_script(const std::string& content);
void validate_utf8(const std::string& content);
}
