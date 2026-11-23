#pragma once
#include "json.hpp"
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>
namespace edudb::record {
using json=nlohmann::json;
constexpr std::size_t maximum_record_bytes=8192-64-8;
enum class Format { Json=0,Fixed=1,Variable=2 };
struct Error:std::runtime_error{std::string code;Error(std::string c,std::string m):std::runtime_error(std::move(m)),code(std::move(c)){};};
struct Column {std::string name,type;std::uint64_t length=0;bool not_null=false;};
struct Schema {
    std::vector<Column> columns;Format format=Format::Json;std::size_t blob_ref_bytes=1024;std::uint64_t fingerprint=0;
};
struct Decoded {std::uint64_t logical_id=0;json row;Format format=Format::Json;};
Format parse_format(const std::string& name);
const char* format_name(Format f)noexcept;
std::size_t utf8_characters(const std::string& text);
Schema schema(const json& table_metadata);
// Throws before creating/converting a table whose fixed maximum or variable
// directory cannot fit in one ordinary 8KiB heap slot.
void validate_layout(const Schema& schema);
json layout(const Schema& schema);
std::string encode(const Schema& schema,std::uint64_t logical_id,const json& encoded_row);
// Old JSON is recognized from its bytes. Binary headers carry their own
// layout and a schema fingerprint; no native structs are written to disk.
Decoded decode(const Schema& schema,const std::string& bytes);
} // namespace edudb::record
