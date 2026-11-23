#pragma once
#include "edudb.hpp"

namespace edudb::input {
constexpr std::size_t json_depth_limit=64;
constexpr std::size_t json_node_limit=65536;
constexpr std::size_t json_estimate_limit=96u*1024u*1024u;
constexpr std::size_t sql_byte_limit=64u*1024u*1024u;
constexpr std::size_t sql_delimiter_limit=127;
constexpr std::size_t sql_query_nesting_limit=16;
constexpr std::size_t sql_ast_node_limit=250000;
constexpr std::size_t sql_ast_walk_depth_limit=160;
constexpr std::size_t sql_import_ast_estimate_limit=384u*1024u*1024u;
constexpr std::size_t csv_parse_estimate_limit=128u*1024u*1024u;

// Iterative lexical preflight happens before recursive DOM operations. String
// and escaped-quote contents do not contribute to structural depth.
void check_json_text(const std::string& text);
json parse_json(const std::string& text);
void check_json_value(const json& value);
// Parentheses inside comments, strings and quoted identifiers are skipped.
void check_sql_delimiters(const std::string& text);
json describe();
}
