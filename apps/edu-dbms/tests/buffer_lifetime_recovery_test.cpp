#include "edudb.hpp"
#include "recovery_archive.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>

// Independent regression for buffer-object lifetime during physical replay.
// This test does not investigate the separate dynamic-TLS thread-exit crash.
using namespace edudb;
using namespace std::chrono_literals;
namespace fs = std::filesystem;
namespace {
std::atomic<unsigned> checks{0};
void check(bool condition, const std::string& message) {
    ++checks;
    if (!condition) throw std::runtime_error(message);
}

// Bound the entire native test, including a future destructor waiting on a
// broken implementation. The normal path joins the watchdog immediately.
class TestDeadline {
    std::mutex mutex_;
    std::condition_variable cv_;
    bool done_ = false;
    std::thread worker_;
public:
    TestDeadline() : worker_([this] {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, 60s, [this] { return done_; })) {
            std::cerr << "FAIL buffer lifetime test exceeded 60 seconds\n";
            std::_Exit(3);
        }
    }) {}
    ~TestDeadline() {
        { std::lock_guard<std::mutex> lock(mutex_); done_ = true; }
        cv_.notify_all();
        worker_.join();
    }
    TestDeadline(const TestDeadline&) = delete;
    TestDeadline& operator=(const TestDeadline&) = delete;
};

struct ReplayGate {
    std::mutex mutex;
    std::condition_variable cv;
    bool armed = false, entered = false, released = true;
    void arm() {
        std::lock_guard<std::mutex> lock(mutex);
        armed = true; entered = false; released = false;
    }
    void phase(const char* point) {
        if (std::string(point) != "standby_before_mutation") return;
        std::unique_lock<std::mutex> lock(mutex);
        if (!armed) return;
        armed = false; entered = true; cv.notify_all();
        if (!cv.wait_for(lock, 10s, [this] { return released; }))
            throw std::runtime_error("test replay gate exceeded 10 seconds");
    }
    bool wait_entered() {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, 2s, [this] { return entered; });
    }
    void release() {
        { std::lock_guard<std::mutex> lock(mutex); released = true; armed = false; }
        cv.notify_all();
    }
};

json sql(Engine& engine, const std::string& session, const std::string& text) {
    auto result = engine.execute(session, text);
    check(result.value("ok", false), text + ": " + result.dump());
    return result;
}
json buffer_request(const json& configuration) {
    return {{"policy", configuration.at("policy")},
            {"read_ahead_pages", configuration.at("read_ahead_pages")},
            {"wait_ms", configuration.at("wait_ms")}};
}
void duplicate(const json& result, std::uint64_t lsn) {
    check(result.at("appended_wal_bytes") == 0, "duplicate replay appended WAL");
    check(result.at("data_pages_written") == 0, "duplicate replay wrote data pages");
    check(result.at("replayed_lsn") == lsn, "duplicate replay changed replay boundary");
}
void diagnostic(const json& result, std::uint64_t lsn, std::uint64_t epoch) {
    check(!result.at("recovery_required").get<bool>(), "storage entered fail-stop");
    check(result.at("durable_lsn") == lsn, "diagnostic observed wrong durable LSN");
    const auto& identity = result.at("recovery_identity");
    check(identity.at("role") == "STANDBY", "diagnostic changed standby role");
    check(identity.at("standby").at("standby_replayed_lsn") == lsn,
          "diagnostic observed wrong persistent replay boundary");
    const auto& graph = result.at("transactions").at("page_version_graph");
    check(graph.at("epoch_resets") == epoch, "duplicate replay reset graph epoch");
    check(!graph.at("disabled").get<bool>() && graph.at("invalid_events") == 0,
          "diagnostic graph became disabled or invalid");
}

struct Fixture {
    std::string manifest;
    std::uint64_t target_lsn = 0;
};
Fixture fixture(const fs::path& root, const StorageOptions& options) {
    Engine primary((root / "primary").u8string(), 2, 32u * 1024u * 1024u, options);
    auto session = primary.open_session();
    sql(primary, session, "CREATE TABLE t(id INTEGER PRIMARY KEY,v VARCHAR(80))");
    sql(primary, session, "INSERT INTO t VALUES(1,'baseline'),(2,'retained')");
    auto base = primary.backup("buffer_lifetime");
    sql(primary, session, "UPDATE t SET v='first' WHERE id=1");
    auto first = primary.archive_wal();
    sql(primary, session, "UPDATE t SET v='second' WHERE id=1");
    auto second = primary.archive_wal();
    recovery::restore_backup(base.at("directory").get<std::string>(),
        {first.at("manifest_path").get<std::string>(),
         second.at("manifest_path").get<std::string>()},
        first.at("end_lsn").get<std::uint64_t>(), (root / "standby").u8string());
    return {second.at("manifest_path").get<std::string>(),
            second.at("end_lsn").get<std::uint64_t>()};
}

json gated_round(Storage& standby, ReplayGate& gate, const Fixture& f,
                 const json& original, std::uint64_t epoch) {
    gate.arm();
    auto replay = std::async(std::launch::async, [&] {
        return standby.replay_archive(f.manifest);
    });
    const bool entered = gate.wait_entered();
    if (!entered) {
        gate.release();
        check(replay.wait_for(12s) == std::future_status::ready,
              "replay never reached or exited the phase hook");
        replay.get();
        check(false, "replay did not reach standby_before_mutation");
    }

    std::promise<void> diagnostic_started, configuration_started;
    auto ds = diagnostic_started.get_future();
    auto cs = configuration_started.get_future();
    auto inspect = std::async(std::launch::async, [&] {
        diagnostic_started.set_value();
        return standby.diagnostics();
    });
    auto configure = std::async(std::launch::async, [&] {
        configuration_started.set_value();
        return standby.configure_buffer(original);
    });

    // Do not throw while the gate is closed: a waiting future destructor must
    // never prevent the controller from releasing the replay thread.
    const bool ds_ready = ds.wait_for(2s) == std::future_status::ready;
    const bool cs_ready = cs.wait_for(2s) == std::future_status::ready;
    const bool diagnostic_waited = inspect.wait_for(150ms) == std::future_status::timeout;
    const bool configuration_waited = configure.wait_for(150ms) == std::future_status::timeout;
    gate.release();
    check(replay.wait_for(12s) == std::future_status::ready, "gated replay did not finish");
    check(inspect.wait_for(12s) == std::future_status::ready, "diagnostic stayed blocked");
    check(configure.wait_for(12s) == std::future_status::ready, "configuration stayed blocked");
    auto replay_result = replay.get();
    auto diagnostic_result = inspect.get();
    auto configuration_result = configure.get();
    check(ds_ready && cs_ready, "management workers did not start in time");
    check(diagnostic_waited, "diagnostics returned before WAL-owning replay was released");
    check(configuration_waited, "configure_buffer returned before WAL-owning replay was released");
    duplicate(replay_result, f.target_lsn);
    diagnostic(diagnostic_result, f.target_lsn, epoch);
    check(buffer_request(configuration_result) == original, "configuration values changed");
    return {{"diagnostics_waited_for_release", diagnostic_waited},
            {"configure_buffer_waited_for_release", configuration_waited},
            {"hold_observation_ms_per_future", 150}};
}

json concurrent_round(Storage& standby, const Fixture& f, const json& original,
                      std::uint64_t epoch) {
    constexpr unsigned replays = 100, inspections = 150, configurations = 200;
    std::promise<void> start;
    auto signal = start.get_future().share();
    auto replay = std::async(std::launch::async, [&, signal] {
        signal.wait();
        for (unsigned i = 0; i < replays; ++i) {
            duplicate(standby.replay_archive(f.manifest), f.target_lsn);
            std::this_thread::yield();
        }
        return replays;
    });
    auto inspect = std::async(std::launch::async, [&, signal] {
        signal.wait();
        for (unsigned i = 0; i < inspections; ++i) {
            diagnostic(standby.diagnostics(), f.target_lsn, epoch);
            std::this_thread::yield();
        }
        return inspections;
    });
    auto configure = std::async(std::launch::async, [&, signal] {
        signal.wait();
        for (unsigned i = 0; i < configurations; ++i) {
            check(buffer_request(standby.configure_buffer(original)) == original,
                  "concurrent configuration changed original values");
            std::this_thread::yield();
        }
        return configurations;
    });
    start.set_value();
    check(replay.wait_for(30s) == std::future_status::ready, "concurrent replay timed out");
    check(inspect.wait_for(15s) == std::future_status::ready, "concurrent diagnostic timed out");
    check(configure.wait_for(15s) == std::future_status::ready, "concurrent configuration timed out");
    const auto replayed = replay.get();
    const auto inspected = inspect.get();
    const auto configured = configure.get();
    return {{"duplicate_replays", replayed}, {"diagnostics", inspected},
            {"same_value_configurations", configured}};
}
} // namespace

int main(int argc, char** argv) {
    try {
        TestDeadline deadline;
        check(argc == 1 || argc == 2, "usage: buffer_lifetime_recovery_test [NEW_DIRECTORY]");
        auto root = argc == 2 ? fs::absolute(fs::u8path(argv[1])) :
            fs::absolute(fs::path("buffer-lifetime-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count())));
        check(!fs::exists(root), "test directory must be absent; existing data is never reused");
        fs::create_directories(root);
        StorageOptions options;
        options.fencing_directory = (root / "fence").u8string();
        auto f = fixture(root, options);
        auto gate = std::make_shared<ReplayGate>();
        options.recovery_phase_hook = [gate](const char* point) { gate->phase(point); };
        Storage standby((root / "standby").u8string(), 2, 32u * 1024u * 1024u, options);
        standby.become_standby();
        const auto original = buffer_request(standby.configure_buffer(json::object()));
        auto first_replay = standby.replay_archive(f.manifest);
        check(first_replay.at("appended_wal_bytes").get<std::uint64_t>() > 0 &&
              first_replay.at("data_pages_written").get<std::uint64_t>() > 0,
              "fixture did not perform an actual first physical replay");
        const auto initial = standby.diagnostics();
        const auto epoch = initial.at("transactions").at("page_version_graph")
                                  .at("epoch_resets").get<std::uint64_t>();
        check(epoch == 1, "first physical replay did not reset graph epoch exactly once");
        diagnostic(initial, f.target_lsn, epoch);
        auto gated = gated_round(standby, *gate, f, original, epoch);
        auto stress = concurrent_round(standby, f, original, epoch);
        const auto final = standby.diagnostics();
        diagnostic(final, f.target_lsn, epoch);
        check(buffer_request(standby.configure_buffer(json::object())) == original,
              "original configuration was not preserved");
        auto tx = standby.begin();
        auto table = standby.read_table(tx, "t", true, false);
        standby.rollback(tx);
        check(table.at("rows") == json::array({json::array({1, "second"}),
                                               json::array({2, "retained"})}),
              "duplicate replay changed user rows");
        std::cout << json{{"status", "PASS"}, {"checks", checks.load()},
            {"test_directory", root.u8string()}, {"scope", "native buffer-object lifetime during standby replay"},
            {"gated_replay", gated}, {"concurrent_calls", stress},
            {"original_buffer_configuration", original}, {"replayed_lsn", f.target_lsn},
            {"graph_epoch_resets", epoch},
            {"limits", "bounded concurrency coverage; does not prove absence of all races; unrelated to dynamic-TLS thread-exit diagnosis"}}
                  .dump(2) << '\n';
        return 0;
    } catch (const DbError& error) {
        std::cerr << json{{"status", "FAIL"}, {"code", error.code},
                          {"message", error.what()}, {"checks", checks.load()}}.dump() << '\n';
        return 1;
    } catch (const std::exception& error) {
        std::cerr << json{{"status", "FAIL"}, {"message", error.what()},
                          {"checks", checks.load()}}.dump() << '\n';
        return 2;
    }
}
