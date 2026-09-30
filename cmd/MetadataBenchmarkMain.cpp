#include "MetadataStore.hpp"
#include "Models.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Targeted micro-benchmarks for the MetadataStore relief changes (ADR-0006
// amendment): the connection pool and the list() JOIN. Unlike BenchmarkMain,
// this driver talks to MetadataStore directly so the measurements isolate the
// metadata layer from crypto and HTTP transport.
//
//   * list sweep   - list() (one JOIN) vs the same objects fetched one-by-one
//                    (the N+1 access pattern), as object count grows.
//   * throughput   - concurrent get() throughput as pool_size varies. A pool of
//                    1 reproduces the old single-connection serialization; a
//                    larger pool lets independent operations run in parallel.

namespace {

using Clock = std::chrono::steady_clock;

std::string env_or(const char* name, const std::string& fallback) {
    const char* val = std::getenv(name);
    return val ? std::string(val) : fallback;
}

std::uint32_t env_uint(const char* name, std::uint32_t fallback) {
    const char* val = std::getenv(name);
    return val ? static_cast<std::uint32_t>(std::stoul(val)) : fallback;
}

template <typename T>
std::vector<T> parse_list(const std::string& csv, std::vector<T> fallback) {
    if (csv.empty()) {
        return fallback;
    }

    std::vector<T> out;
    std::stringstream ss(csv);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            out.push_back(static_cast<T>(std::stoull(token)));
        }
    }

    return out.empty() ? fallback : out;
}

double ms_since(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

// Build a valid UUID in a dedicated namespace (the 4th group) so benchmark ids
// never collide with each other or with BenchmarkMain's run ids.
std::string make_uuid(std::uint32_t group, std::uint64_t n) {
    std::stringstream ss;
    ss << "00000000-0000-0000-"
       << std::hex << std::setw(4) << std::setfill('0') << group << "-"
       << std::setw(12) << std::setfill('0') << n;
    return ss.str();
}

ObjectMetadata make_metadata(const std::string& id, std::uint32_t shards) {
    ObjectMetadata m;
    m.id = id;
    m.size = 1024;
    m.checksum = "sha256:metadata-benchmark";
    m.erasure.data_shards = shards > 0 ? shards : 1;
    m.erasure.parity_shards = 0;
    m.erasure.shard_size = 256;
    m.encrypted_dek = Bytes(16, 0x00);
    m.kek_id = "kek-benchmark";
    for (std::uint32_t i = 0; i < shards; ++i) {
        m.shard_locations.push_back("node-bench:/shard-" + std::to_string(i));
    }
    return m;
}

constexpr std::uint32_t kListGroup = 0x0001;
constexpr std::uint32_t kThroughputGroup = 0x0002;

// ---------------------------------------------------------------------------
// list() sweep: one JOIN vs per-object (N+1) access.
// ---------------------------------------------------------------------------
void run_list_sweep(const std::string& conn_str) {
    const std::vector<std::uint64_t> sizes = parse_list<std::uint64_t>(
        env_or("META_LIST_SIZES", ""), {10, 50, 100, 250, 500, 1000});
    const std::uint32_t shards = env_uint("META_LIST_SHARDS", 6);
    const std::string csv_path = env_or(
        "META_LIST_CSV", "docs/metrics/metadata_list_sweep.csv");

    std::vector<std::uint64_t> targets = sizes;
    std::sort(targets.begin(), targets.end());
    const std::uint64_t max_n = targets.empty() ? 0 : targets.back();

    std::ofstream out(csv_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        throw std::runtime_error("cannot open: " + csv_path);
    }
    out << "num_objects,shards_per_object,list_ms,per_object_ms\n";

    MetadataStore store(conn_str, 1);

    std::cout << "== list() sweep ==\n";

    std::uint64_t inserted = 0;
    for (std::uint64_t target : targets) {
        for (; inserted < target; ++inserted) {
            store.put(make_metadata(make_uuid(kListGroup, inserted), shards));
        }

        // One JOIN that returns every object with its shard locations.
        const auto list_start = Clock::now();
        const std::vector<ObjectMetadata> all = store.list();
        const double list_ms = ms_since(list_start);

        // The same objects fetched individually - the N+1 access pattern.
        const auto per_object_start = Clock::now();
        for (const auto& object : all) {
            const auto fetched = store.get(object.id);
            (void)fetched;
        }
        const double per_object_ms = ms_since(per_object_start);

        // Report the actual row count so the x-axis is correct even if the
        // database is not perfectly clean.
        const std::size_t num_objects = all.size();
        out << num_objects << "," << shards << ","
            << list_ms << "," << per_object_ms << "\n";

        std::cout << "  N=" << num_objects
                  << " list_ms=" << list_ms
                  << " per_object_ms=" << per_object_ms << "\n";
    }

    for (std::uint64_t i = 0; i < max_n; ++i) {
        store.remove(make_uuid(kListGroup, i));
    }

    std::cout << "  results -> " << csv_path << "\n";
}

// ---------------------------------------------------------------------------
// Concurrent throughput: get() throughput as pool_size varies.
// ---------------------------------------------------------------------------
void run_throughput(const std::string& conn_str) {
    const std::vector<std::uint32_t> pool_sizes = parse_list<std::uint32_t>(
        env_or("META_POOL_SIZES", ""), {1, 2, 4, 8});
    const std::vector<std::uint32_t> thread_counts = parse_list<std::uint32_t>(
        env_or("META_THREADS", ""), {1, 2, 4, 8});
    const std::uint32_t ops_per_thread = env_uint("META_OPS_PER_THREAD", 200);
    const std::uint32_t dataset = env_uint("META_DATASET", 200);
    const std::uint32_t shards = env_uint("META_TP_SHARDS", 6);
    const std::string csv_path = env_or(
        "META_TP_CSV", "docs/metrics/metadata_throughput.csv");

    std::ofstream out(csv_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        throw std::runtime_error("cannot open: " + csv_path);
    }
    out << "pool_size,threads,total_ops,duration_ms,throughput_ops_per_sec,mean_latency_ms\n";

    std::cout << "== concurrent throughput ==\n";

    // Populate a fixed read dataset once.
    {
        MetadataStore setup(conn_str, 4);
        for (std::uint32_t i = 0; i < dataset; ++i) {
            setup.put(make_metadata(make_uuid(kThroughputGroup, i), shards));
        }
    }

    for (std::uint32_t pool : pool_sizes) {
        for (std::uint32_t threads : thread_counts) {
            MetadataStore store(conn_str, pool);

            auto worker = [&](std::uint32_t tid) {
                for (std::uint32_t i = 0; i < ops_per_thread; ++i) {
                    // Cheap deterministic spread across the dataset.
                    const std::uint32_t idx =
                        (tid * 2654435761u + i * 40503u) % dataset;
                    const auto fetched = store.get(make_uuid(kThroughputGroup, idx));
                    (void)fetched;
                }
            };

            const auto run_start = Clock::now();
            std::vector<std::thread> workers;
            workers.reserve(threads);
            for (std::uint32_t t = 0; t < threads; ++t) {
                workers.emplace_back(worker, t);
            }
            for (auto& w : workers) {
                w.join();
            }
            const double duration_ms = ms_since(run_start);

            const std::uint64_t total_ops =
                static_cast<std::uint64_t>(threads) * ops_per_thread;
            const double throughput =
                total_ops / (duration_ms / 1000.0);
            // Each thread runs ops_per_thread operations back-to-back, so the
            // mean wall latency per operation is the run duration divided by
            // the per-thread op count.
            const double mean_latency_ms =
                ops_per_thread > 0 ? duration_ms / ops_per_thread : 0.0;

            out << pool << "," << threads << "," << total_ops << ","
                << duration_ms << "," << throughput << ","
                << mean_latency_ms << "\n";

            std::cout << "  pool=" << pool << " threads=" << threads
                      << " throughput=" << throughput << " ops/s"
                      << " mean_latency_ms=" << mean_latency_ms << "\n";
        }
    }

    {
        MetadataStore cleanup(conn_str, 4);
        for (std::uint32_t i = 0; i < dataset; ++i) {
            cleanup.remove(make_uuid(kThroughputGroup, i));
        }
    }

    std::cout << "  results -> " << csv_path << "\n";
}

} // namespace

int main() {
    const std::string conn_str = env_or(
        "BENCHMARK_METADATA_CONN",
        "host=localhost port=5433 dbname=myc_test user=test_user password=test_password"
    );

    const std::string mode = env_or("META_BENCH_MODE", "all");

    try {
        if (mode == "list" || mode == "all") {
            run_list_sweep(conn_str);
        }
        if (mode == "throughput" || mode == "all") {
            run_throughput(conn_str);
        }
    } catch (const std::exception& e) {
        std::cerr << "metadata_benchmark failed: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
