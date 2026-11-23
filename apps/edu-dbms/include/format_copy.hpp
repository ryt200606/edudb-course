#pragma once
#include "json.hpp"
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
namespace edudb::format_copy {
using json=nlohmann::json;namespace fs=std::filesystem;
struct Limits {std::uint64_t bytes=1024ull*1024*1024;std::size_t files=1024;std::chrono::seconds time{300};};
std::string sha256(const std::string& value);
std::string file_sha256(const fs::path& path);
json read_control(const fs::path& path);
void write_control(const fs::path& directory,json value);
// Holds a real directory handle: Windows resolves junctions through the handle,
// and does not share DELETE while the plan remains alive.
class Destination {
    struct Impl;std::unique_ptr<Impl> impl_;
public:
    explicit Destination(const fs::path& requested);
    ~Destination();
    const fs::path& path() const;const fs::path& parent() const;
    void separate_from(const fs::path& resolved_source) const;
};
class OwnedDirectory {
    struct Impl;std::unique_ptr<Impl> impl_;
public:
    OwnedDirectory(const fs::path& parent,const std::string& purpose);
    ~OwnedDirectory();
    const fs::path& path() const;
    const std::string& token() const;
    void publish(const Destination& destination);
};
// Source exclusive locks remain held through verification/publication. Every
// source file is opened READ-ONLY; only the private owned copy may recover.
class LegacySnapshot {
    struct Impl;std::unique_ptr<Impl> impl_;
public:
    LegacySnapshot(const fs::path& source,const fs::path& temporary_parent=fs::temp_directory_path(),Limits limits={},const Destination* destination=nullptr);
    ~LegacySnapshot();
    const fs::path& source() const;const fs::path& path() const;
    const json& control() const;const json& source_files() const;
    void promote_owned_copy();
    void verify_source_unchanged() const;
};
}
