#include "MetricsCollector.hpp"
#include "ShardTransport.hpp"
#include "StorageClient.hpp"

#include <botan/auto_rng.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <iomanip>
#include <sstream>

namespace {

std::string env_or(const char* name, const std::string& fallback) {
    const char* val = std::getenv(name);
    return val ? std::string(val) : fallback;
}

uint32_t env_uint(const char* name, uint32_t fallback) {
    const char* val = std::getenv(name);
    return val ? static_cast<uint32_t>(std::stoul(val)) : fallback;
}

// Parse a comma-separated list (e.g. "2,4,8") into a numeric vector. Returns
// the fallback when the variable is unset or yields no usable values.
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

Bytes generate_random_bytes(std::size_t size) {
    Botan::AutoSeeded_RNG rng;
    Bytes data(size);
    rng.randomize(data.data(), data.size());
    return data;
}

struct BenchmarkConfig {
    std::vector<uint32_t> k_values;
    std::vector<uint32_t> m_values;
    std::vector<std::size_t> object_sizes;
};

BenchmarkConfig default_config() {
    BenchmarkConfig cfg;
    cfg.k_values = parse_list<uint32_t>(
        env_or("BENCHMARK_K_VALUES", ""), {2, 4, 6, 8});
    cfg.m_values = parse_list<uint32_t>(
        env_or("BENCHMARK_M_VALUES", ""), {1, 2, 3, 4});
    cfg.object_sizes = parse_list<std::size_t>(
        env_or("BENCHMARK_OBJECT_SIZES", ""), {1024, 4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024});
    return cfg;
}

// Simulate node failures by deleting up to `count` of an object's shards from
// their storage nodes (clamped to `m` by the caller so >= k shards survive and
// REPAIR can still reconstruct). Reuses the StorageClient transport layer rather
// than duplicating orchestration logic. Returns the number of shards dropped.
uint32_t inject_failures(
    MetadataStore& metadata_store,
    const std::string& object_id,
    uint32_t count
) {
    if (count == 0) {
        return 0;
    }

    const auto metadata_opt = metadata_store.get(object_id);
    if (!metadata_opt.has_value()) {
        return 0;
    }

    ShardLocationMap to_fail;
    uint32_t dropped = 0;
    for (const auto& shard_location : metadata_opt->shard_locations) {
        if (dropped >= count) {
            break;
        }
        const auto pos = shard_location.find('/');
        if (pos == std::string::npos) {
            continue;
        }
        to_fail[shard_location.substr(0, pos)].push_back(
            shard_location.substr(pos + 1));
        ++dropped;
    }

    if (!to_fail.empty()) {
        remove_from_nodes(to_fail);
    }

    return dropped;
}

} // namespace


int main() {
    const std::string metadata_conn_str = env_or(
        "BENCHMARK_METADATA_CONN",
        "host=localhost port=5433 dbname=myc_test user=test_user password=test_password"
    );

    const std::string csv_path = env_or("BENCHMARK_CSV_PATH", "docs/metrics/benchmark_results.csv");

    MetricsCollector collector;

    StorageClient client(metadata_conn_str);
    client.set_collector(&collector);
    client.init();

    MetadataStore metadata_store(metadata_conn_str); 
    const std::vector<std::string> eligible_nodes = metadata_store.list_nodes();

    BenchmarkConfig cfg = default_config();

    // Number of shards to fail before REPAIR; clamped per-combination to m so
    // that at least k shards survive and reconstruction remains possible.
    const uint32_t requested_failures = env_uint("BENCHMARK_FAILED_NODES", 0);

    int run_id = 0;

    for (uint32_t k : cfg.k_values) {
        for (uint32_t m : cfg.m_values) {

            if (k + m > eligible_nodes.size()) {
                continue;
            }

            for (std::size_t object_size : cfg.object_sizes) {
                // shard_size must satisfy: k * shard_size >= object_size
                std::size_t shard_size = (object_size + k - 1) / k;

                ErasureSpec spec;
                spec.data_shards = k;
                spec.parity_shards = m;
                spec.shard_size = shard_size;

                std::stringstream ss;
                ss << "00000000-0000-0000-0000-"
                << std::hex
                << std::setw(12)
                << std::setfill('0')
                << run_id++;
                std::string object_id = ss.str();

                Bytes data = generate_random_bytes(object_size);

                std::cout << "RUN k=" << k << " m=" << m
                          << " object_size=" << object_size
                          << " shard_size=" << shard_size << "\n";

                // PUT
                try {
                    client.put(object_id, data, spec);
                } catch (const std::exception& e) {
                    std::cerr << "  PUT failed: " << e.what() << "\n";
                    continue;
                }

                // GET
                try {
                    Bytes retrieved = client.get(object_id);
                    if (retrieved != data) {
                        std::cerr << "  GET data mismatch\n";
                    }
                } catch (const std::exception& e) {
                    std::cerr << "  GET failed: " << e.what() << "\n";
                }

                // const uint32_t to_drop = std::min(requested_failures, m);
                const uint32_t to_drop = requested_failures;
                if (to_drop > 0) {
                    inject_failures(metadata_store, object_id, to_drop);
                }
                
                // REPAIR (only meaningful if nodes can be failed externally)
                try {
                    client.repair(object_id);
                } catch (const std::exception& e) {
                    std::cerr << "  REPAIR failed: " << e.what() << "\n";
                }

                // Cleanup
                try {
                    client.remove(object_id);
                } catch (...) {}
            }
        }
    }

    const bool append = !env_or("BENCHMARK_CSV_APPEND", "").empty();
    collector.export_csv(csv_path, append);

    std::cout << "Results written to " << csv_path << "\n";
    std::cout << "Total records: " << collector.records().size() << "\n";

    return 0;
}
