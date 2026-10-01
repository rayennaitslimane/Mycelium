#include <gtest/gtest.h>
#include <httplib.h>

#include "MetadataStore.hpp"
#include "StorageClient.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

namespace {

Bytes ToBytes(const std::string& s) {
    return Bytes(s.begin(), s.end());
}

std::string ToString(const Bytes& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

bool ContainsObjectId(
    const std::vector<ObjectMetadata>& items,
    const std::string& object_id
) {
    return std::any_of(items.begin(), items.end(), [&](const ObjectMetadata& m) {
        return m.id == object_id;
    });
}

const ObjectMetadata* FindObjectMetadata(
    const std::vector<ObjectMetadata>& items,
    const std::string& object_id
) {
    auto it = std::find_if(items.begin(), items.end(), [&](const ObjectMetadata& m) {
        return m.id == object_id;
    });
    return it == items.end() ? nullptr : &(*it);
}

ErasureSpec TestErasureSpec() {
    ErasureSpec spec;
    spec.data_shards = 2;
    spec.parity_shards = 1;
    spec.shard_size = 20;
    return spec;
}

class StorageClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use a temp keystore file and metadata dir per test
        const std::string suffix =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        kek_path_ = "/tmp/myc_kek_" + suffix + ".json";
        setenv("MYC_KEYSTORE_PATH", kek_path_.c_str(), 1);

        metadata_dir_ = std::filesystem::temp_directory_path() / ("myc_metadata_" + suffix);
        store_ = std::make_shared<MetadataStore>(metadata_dir_.string());

        client_ = std::make_unique<StorageClient>(store_);

        // Register test nodes
        client_->metadata_store().register_node("localhost:9001");
        client_->metadata_store().register_node("localhost:9002");
        client_->metadata_store().register_node("localhost:9003");

        client_->init();
    }

    void TearDown() override {
        client_.reset();
        store_.reset();
        std::error_code ec;
        std::filesystem::remove_all(metadata_dir_, ec);
        std::filesystem::remove(kek_path_);
        unsetenv("MYC_KEYSTORE_PATH");
    }

    StorageClient& client() { return *client_; }

    // LMDB forbids opening one env twice per process, so all clients share it.
    std::shared_ptr<MetadataStore> store_;
    std::filesystem::path metadata_dir_;
    std::string kek_path_;
    std::unique_ptr<StorageClient> client_;
};

TEST_F(StorageClientTest, PutTest) {
    const std::string object_id = "00000000-0000-0000-0000-000000000101";
    const Bytes payload = ToBytes("Hello from put test");

    ASSERT_NO_THROW(client().put(object_id, payload, TestErasureSpec()));

    const auto items = client().list();
    ASSERT_TRUE(ContainsObjectId(items, object_id));

    const ObjectMetadata* meta = FindObjectMetadata(items, object_id);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->id, object_id);
    EXPECT_EQ(meta->size, payload.size());
    EXPECT_EQ(meta->shard_locations.size(), 3u);
}

TEST_F(StorageClientTest, GetTest) {
    const std::string object_id = "00000000-0000-0000-0000-000000000102";
    const std::string original = "Hello from get test 1234567890";

    client().put(object_id, ToBytes(original), TestErasureSpec());

    const Bytes restored = client().get(object_id);
    EXPECT_EQ(ToString(restored), original);
}

TEST_F(StorageClientTest, ListTest) {
    const std::string id1 = "00000000-0000-0000-0000-000000000103";
    const std::string id2 = "00000000-0000-0000-0000-000000000104";
    const std::string id3 = "00000000-0000-0000-0000-000000000105";

    client().put(id1, ToBytes("payload-1"), TestErasureSpec());
    client().put(id2, ToBytes("payload-2"), TestErasureSpec());
    client().put(id3, ToBytes("payload-3"), TestErasureSpec());

    const auto items = client().list();

    EXPECT_EQ(items.size(), 3u);
    EXPECT_TRUE(ContainsObjectId(items, id1));
    EXPECT_TRUE(ContainsObjectId(items, id2));
    EXPECT_TRUE(ContainsObjectId(items, id3));
}

TEST_F(StorageClientTest, DeleteTest) {
    const std::string object_id = "00000000-0000-0000-0000-000000000107";
    client().put(object_id, ToBytes("to-be-deleted"), TestErasureSpec());

    EXPECT_TRUE(client().remove(object_id));
    EXPECT_FALSE(client().remove(object_id));

    const auto items = client().list();
    EXPECT_FALSE(ContainsObjectId(items, object_id));
    EXPECT_TRUE(items.empty());

    EXPECT_THROW(
        {
            const auto _ = client().get(object_id);
            (void)_;
        },
        std::runtime_error
    );
}

TEST_F(StorageClientTest, RotationTest) {
    const std::string id1 = "00000000-0000-0000-0000-000000000201";
    const std::string id2 = "00000000-0000-0000-0000-000000000202";
    const std::string payload1 = "payload-before-rotation";
    const std::string payload2 = "payload-after-rotation";

    // Write object with initial KEK
    client().put(id1, ToBytes(payload1), TestErasureSpec());

    // Rotate KEK
    client().rotate();

    // Write object with new active KEK
    client().put(id2, ToBytes(payload2), TestErasureSpec());

    // Destroy and recreate client (simulates restart, loads keyring from file)
    client_.reset();
    client_ = std::make_unique<StorageClient>(store_);
    client_->init();

    // Both objects must be readable
    EXPECT_EQ(ToString(client().get(id1)), payload1);
    EXPECT_EQ(ToString(client().get(id2)), payload2);
}

TEST_F(StorageClientTest, RapidConsecutiveRotationsPreserveReadability) {
    const std::string id1 = "00000000-0000-0000-0000-000000000301";
    const std::string id2 = "00000000-0000-0000-0000-000000000302";
    const std::string id3 = "00000000-0000-0000-0000-000000000303";

    const std::string payload1 = "payload-before-any-rotation";
    const std::string payload2 = "payload-after-first-rotation";
    const std::string payload3 = "payload-after-second-rotation";

    client().put(id1, ToBytes(payload1), TestErasureSpec());
    client().rotate();
    client().put(id2, ToBytes(payload2), TestErasureSpec());

    client().rotate();
    client().put(id3, ToBytes(payload3), TestErasureSpec());

    // Restart client and verify all generations remain readable.
    client_.reset();
    client_ = std::make_unique<StorageClient>(store_);
    client_->init();

    EXPECT_EQ(ToString(client().get(id1)), payload1);
    EXPECT_EQ(ToString(client().get(id2)), payload2);
    EXPECT_EQ(ToString(client().get(id3)), payload3);
}

// =============================================================================
// Stress / Concurrency Tests
// =============================================================================

// Helper: create N independent StorageClient instances, each with a unique KEK
// file. Must be called from the main (test) thread because setenv is not
// thread-safe.
struct MultiClientEnv {
    std::vector<std::unique_ptr<StorageClient>> clients;

    MultiClientEnv(const std::shared_ptr<IMetadataStore>& store, int count) {
        for (int i = 0; i < count; ++i) {
            clients.push_back(std::make_unique<StorageClient>(store));
            clients.back()->init();
        }
    }

    ~MultiClientEnv() = default;

    MultiClientEnv(const MultiClientEnv&) = delete;
    MultiClientEnv& operator=(const MultiClientEnv&) = delete;
};

// ---------------------------------------------------------------------------
// T1: Race conditions - repeated PUT/GET under load
// ---------------------------------------------------------------------------
TEST_F(StorageClientTest, ConcurrentWriteReadRoundTrip) {
    constexpr int kNumClients = 8;

    MultiClientEnv env(store_, kNumClients);

    std::vector<std::string> ids(kNumClients);
    std::vector<std::string> payloads(kNumClients);
    for (int i = 0; i < kNumClients; ++i) {
        // IDs: 00000000-0000-0000-0000-000000000401 .. 0408
        char id_buf[48];
        snprintf(id_buf, sizeof(id_buf),
                 "00000000-0000-0000-0000-0000000004%02d", i + 1);
        ids[i] = id_buf;
        payloads[i] = "stress-t1-" + std::to_string(i);
    }

    std::vector<Bytes> results(kNumClients);
    std::atomic<bool> any_failure{false};

    std::vector<std::thread> threads;
    threads.reserve(kNumClients);
    for (int i = 0; i < kNumClients; ++i) {
        threads.emplace_back([&, i]() {
            try {
                env.clients[i]->put(ids[i], ToBytes(payloads[i]), TestErasureSpec());
                results[i] = env.clients[i]->get(ids[i]);
            } catch (...) {
                any_failure.store(true);
            }
        });
    }
    for (auto& t : threads) t.join();

    ASSERT_FALSE(any_failure.load()) << "At least one thread threw an exception";

    for (int i = 0; i < kNumClients; ++i) {
        EXPECT_EQ(ToString(results[i]), payloads[i])
            << "Mismatch for client " << i;
    }
}

// ---------------------------------------------------------------------------
// T2: Partial failure - missing/unavailable node (shard deleted)
// ---------------------------------------------------------------------------
TEST_F(StorageClientTest, GetSucceedsWithOneShardDeleted) {
    const std::string object_id = "00000000-0000-0000-0000-000000000410";
    const std::string original = "erasure-tolerance-test!";

    client().put(object_id, ToBytes(original), TestErasureSpec());

    // Look up the actual shard location for shard 0
    const auto items = client().list();
    const ObjectMetadata* meta = FindObjectMetadata(items, object_id);
    ASSERT_NE(meta, nullptr);
    ASSERT_GE(meta->shard_locations.size(), 1u);
    // shard_locations[0] is "host:port/location" - extract path after '/'
    const std::string& loc0 = meta->shard_locations[0];
    const std::string shard_path = loc0.substr(loc0.find('/') + 1);

    // Directly delete shard 0 from node on port 9001
    {
        httplib::Client node0("localhost", 9001);
        auto res = node0.Delete("/shards?location=" + shard_path);
        ASSERT_TRUE(res);
        ASSERT_EQ(res->status, 200);
    }

    // GET must still succeed - k=2 shards from nodes 9002 and 9003 suffice
    Bytes restored;
    ASSERT_NO_THROW(restored = client().get(object_id));
    EXPECT_EQ(ToString(restored), original);
}

// ---------------------------------------------------------------------------
// T3: Aggregation bugs - large payload (max size) reconstruction
// ---------------------------------------------------------------------------
TEST_F(StorageClientTest, ConcurrentMaxSizePayloadFidelity) {
    constexpr int kNumClients = 6;
    constexpr std::size_t kMaxPayload = 40; // kDataShards(2) * kShardSize(20)

    MultiClientEnv env(store_, kNumClients);

    std::vector<std::string> ids(kNumClients);
    std::vector<Bytes> payloads(kNumClients);
    for (int i = 0; i < kNumClients; ++i) {
        char id_buf[48];
        snprintf(id_buf, sizeof(id_buf),
                 "00000000-0000-0000-0000-0000000004%02d", 20 + i);
        ids[i] = id_buf;
        // Fill 40 bytes with repeating byte value i
        payloads[i] = Bytes(kMaxPayload, static_cast<uint8_t>(i));
    }

    // Concurrent writes
    std::atomic<bool> write_fail{false};
    {
        std::vector<std::thread> threads;
        for (int i = 0; i < kNumClients; ++i) {
            threads.emplace_back([&, i]() {
                try {
                    env.clients[i]->put(ids[i], payloads[i], TestErasureSpec());
                } catch (...) {
                    write_fail.store(true);
                }
            });
        }
        for (auto& t : threads) t.join();
    }
    ASSERT_FALSE(write_fail.load());

    // Concurrent reads
    std::vector<Bytes> results(kNumClients);
    std::atomic<bool> read_fail{false};
    {
        std::vector<std::thread> threads;
        for (int i = 0; i < kNumClients; ++i) {
            threads.emplace_back([&, i]() {
                try {
                    results[i] = env.clients[i]->get(ids[i]);
                } catch (...) {
                    read_fail.store(true);
                }
            });
        }
        for (auto& t : threads) t.join();
    }
    ASSERT_FALSE(read_fail.load());

    for (int i = 0; i < kNumClients; ++i) {
        ASSERT_EQ(results[i].size(), kMaxPayload)
            << "Wrong size for client " << i;
        // Check shard-boundary bytes (byte 19 = end of shard 0, byte 20 = start of shard 1)
        EXPECT_EQ(results[i][0], static_cast<uint8_t>(i));
        EXPECT_EQ(results[i][19], static_cast<uint8_t>(i));
        EXPECT_EQ(results[i][20], static_cast<uint8_t>(i));
        EXPECT_EQ(results[i][39], static_cast<uint8_t>(i));
        EXPECT_EQ(results[i], payloads[i])
            << "Full byte mismatch for client " << i;
    }
}

// ---------------------------------------------------------------------------
// T4: DB consistency - many objects written/read concurrently
// ---------------------------------------------------------------------------
TEST_F(StorageClientTest, HighVolumeWriteListConsistency) {
    constexpr int kNumClients = 4;
    constexpr int kObjectsPerClient = 5;
    constexpr int kTotalObjects = kNumClients * kObjectsPerClient;

    MultiClientEnv env(store_, kNumClients);

    // Prepare IDs and payloads: range 0501..0520
    std::vector<std::vector<std::string>> ids(kNumClients);
    std::vector<std::vector<std::string>> payloads(kNumClients);
    int counter = 1;
    for (int c = 0; c < kNumClients; ++c) {
        for (int o = 0; o < kObjectsPerClient; ++o) {
            char id_buf[48];
            snprintf(id_buf, sizeof(id_buf),
                     "00000000-0000-0000-0000-0000000005%02d", counter);
            ids[c].push_back(id_buf);
            payloads[c].push_back("vol-" + std::to_string(counter));
            ++counter;
        }
    }

    // Concurrent writes: each client writes its 5 objects sequentially
    std::atomic<bool> write_fail{false};
    {
        std::vector<std::thread> threads;
        for (int c = 0; c < kNumClients; ++c) {
            threads.emplace_back([&, c]() {
                try {
                    for (int o = 0; o < kObjectsPerClient; ++o) {
                        env.clients[c]->put(ids[c][o], ToBytes(payloads[c][o]), TestErasureSpec());
                    }
                } catch (...) {
                    write_fail.store(true);
                }
            });
        }
        for (auto& t : threads) t.join();
    }
    ASSERT_FALSE(write_fail.load());

    // Fresh client for verification (new DB connection)
    std::string verify_kek = "/tmp/myc_stress_verify_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
        ".json";
    setenv("MYC_KEYSTORE_PATH", verify_kek.c_str(), 1);
    StorageClient verify_client(store_);
    verify_client.init();
    setenv("MYC_KEYSTORE_PATH", kek_path_.c_str(), 1);

    // list() must return all 20 objects
    const auto items = verify_client.list();
    EXPECT_EQ(items.size(), static_cast<std::size_t>(kTotalObjects));

    // Each object must be individually readable with correct content
    for (int c = 0; c < kNumClients; ++c) {
        for (int o = 0; o < kObjectsPerClient; ++o) {
            EXPECT_TRUE(ContainsObjectId(items, ids[c][o]));
            Bytes got = env.clients[c]->get(ids[c][o]);
            EXPECT_EQ(ToString(got), payloads[c][o]);
        }
    }

    // Remove half the objects (client 0 and client 1's objects)
    for (int c = 0; c < 2; ++c) {
        for (int o = 0; o < kObjectsPerClient; ++o) {
            EXPECT_TRUE(env.clients[c]->remove(ids[c][o]));
        }
    }

    const auto remaining = verify_client.list();
    EXPECT_EQ(remaining.size(), static_cast<std::size_t>(kTotalObjects / 2));

    std::filesystem::remove(verify_kek);
}

// ---------------------------------------------------------------------------
// T5: Timing / non-blocking I/O - slow node does not block fast nodes
// ---------------------------------------------------------------------------
TEST_F(StorageClientTest, GetCompletesWithMissingShardInBoundedTime) {
    const std::string object_id = "00000000-0000-0000-0000-000000000430";
    const std::string original = "timing-test-payload-abc";

    client().put(object_id, ToBytes(original), TestErasureSpec());

    // Look up the actual shard location for shard 2
    const auto items = client().list();
    const ObjectMetadata* meta = FindObjectMetadata(items, object_id);
    ASSERT_NE(meta, nullptr);
    ASSERT_GE(meta->shard_locations.size(), 3u);
    // shard_locations[2] is "host:port/location" - extract path after '/'
    const std::string& loc2 = meta->shard_locations[2];
    const std::string shard_path = loc2.substr(loc2.find('/') + 1);

    // Delete shard 2 from node on port 9003 - simulates unavailable shard
    {
        httplib::Client node2("localhost", 9003);
        auto res = node2.Delete("/shards?location=" + shard_path);
        ASSERT_TRUE(res);
        ASSERT_EQ(res->status, 200);
    }

    // Time the GET - parallel fetch should not be blocked by the missing shard
    auto start = std::chrono::steady_clock::now();
    Bytes restored;
    ASSERT_NO_THROW(restored = client().get(object_id));
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Correctness first
    EXPECT_EQ(ToString(restored), original);

    // Timing: must complete well under 3 seconds on loopback.
    // A sequential implementation with per-node timeouts would exceed this.
    EXPECT_LT(elapsed, std::chrono::seconds(3))
        << "GET took too long - parallel dispatch may be broken";
}

// ---------------------------------------------------------------------------
// T6: Race conditions + aggregation - concurrent overwrite same object
// ---------------------------------------------------------------------------
TEST_F(StorageClientTest, ConcurrentOverwriteSameObjectYieldsConsistentState) {
    constexpr int kNumClients = 4;
    const std::string shared_id = "00000000-0000-0000-0000-000000000601";

    MultiClientEnv env(store_, kNumClients);

    // Each client writes a distinct 12-byte payload to the SAME object ID
    std::vector<std::string> candidate_payloads = {
        "aaaaaaaaaaaa",
        "bbbbbbbbbbbb",
        "cccccccccccc",
        "dddddddddddd"
    };

    // Concurrent overwrites
    std::atomic<bool> write_fail{false};
    {
        std::vector<std::thread> threads;
        for (int i = 0; i < kNumClients; ++i) {
            threads.emplace_back([&, i]() {
                try {
                    env.clients[i]->put(shared_id, ToBytes(candidate_payloads[i]), TestErasureSpec());
                } catch (...) {
                    write_fail.store(true);
                }
            });
        }
        for (auto& t : threads) t.join();
    }
    ASSERT_FALSE(write_fail.load());

    // The system must reach a consistent readable state.
    // If metadata from client A is stored but shards from client B won the
    // LMDB write race, the DEK will not decrypt correctly - this surfaces
    // the known atomicity gap (ADR-0004 / ADR-0005).
    Bytes result;
    ASSERT_NO_THROW(result = client().get(shared_id))
        << "GET threw - possible shard/metadata atomicity mismatch";

    // The result must be exactly one of the four candidates (no garbled mix)
    const std::string result_str = ToString(result);
    bool matches_one = std::any_of(
        candidate_payloads.begin(), candidate_payloads.end(),
        [&](const std::string& p) { return p == result_str; }
    );
    EXPECT_TRUE(matches_one)
        << "Result '" << result_str << "' does not match any candidate payload";

    // Metadata must show exactly 1 entry for this ID
    const auto items = client().list();
    int count = std::count_if(items.begin(), items.end(),
        [&](const ObjectMetadata& m) { return m.id == shared_id; });
    EXPECT_EQ(count, 1);
}

// ===========================================================================
// Concurrency contract validation tests
// ===========================================================================

// Verifies that a single StorageClient instance can handle concurrent
// put/get/remove/list operations from multiple threads without crashes or
// data races (contract points 1 & 4).
TEST_F(StorageClientTest, SameClientConcurrentAccess) {
    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 5;

    std::vector<std::thread> threads;
    std::atomic<int> errors{0};

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kOpsPerThread; ++i) {
                // Each thread operates on its own object to avoid contention
                // on the same UUID (per-object ordering is not guaranteed).
                std::string id = "00000000-0000-0000-0000-0000000" +
                    std::to_string(10000 + t * 100 + i);

                std::string payload = "thread-" + std::to_string(t) +
                    "-iter-" + std::to_string(i) + "-pad1234567890";

                try {
                    client().put(id, ToBytes(payload), TestErasureSpec());

                    Bytes got = client().get(id);
                    if (ToString(got) != payload) {
                        ++errors;
                    }

                    client().list();

                    client().remove(id);
                } catch (const std::exception&) {
                    ++errors;
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(errors.load(), 0)
        << "Concurrent operations on distinct objects must not fail";
}

// Verifies that rotate() can execute concurrently with put()/get() without
// causing data races or crashes (contract point 2: KEK ring coherence).
TEST_F(StorageClientTest, ConcurrentRotateWithReadWrite) {
    // Use valid UUIDs (36 chars total)
    const std::string id1 = "00000000-0000-0000-0000-000000009001";
    const std::string id2 = "00000000-0000-0000-0000-000000009002";

    const std::string base_payload = "rotation-test-payload-pad";

    // Seed objects
    client().put(id1, ToBytes(base_payload + "1"), TestErasureSpec());
    client().put(id2, ToBytes(base_payload + "2"), TestErasureSpec());

    constexpr int kRotations = 5;
    constexpr int kReadWriteOps = 10;

    std::atomic<int> errors{0};

    // Thread 1: repeatedly rotates the KEK
    std::thread rotator([&]() {
        for (int i = 0; i < kRotations; ++i) {
            try {
                client().rotate();
            } catch (const std::exception&) {
                ++errors;
            }
        }
    });

    // Thread 2: reads existing objects
    std::thread reader([&]() {
        for (int i = 0; i < kReadWriteOps; ++i) {
            try {
                Bytes got = client().get(id1);
                if (ToString(got) != base_payload + "1") {
                    ++errors;
                }
            } catch (const std::exception&) {
                ++errors;
            }
        }
    });

    // Thread 3: writes new objects (with whichever KEK is active)
    std::thread writer([&]() {
        for (int i = 0; i < kReadWriteOps; ++i) {
            // Ensure valid UUID suffix: always 12 digits
            char buf[37];
            snprintf(buf, sizeof(buf),
                     "00000000-0000-0000-0000-%012d", 100 + i);
            std::string id = buf;

            try {
                client().put(id, ToBytes("written-during-rotation-" + std::to_string(i)), TestErasureSpec());
            } catch (const std::exception&) {
                ++errors;
            }
        }
    });

    rotator.join();
    reader.join();
    writer.join();

    EXPECT_EQ(errors.load(), 0)
        << "Concurrent rotate with read/write must not crash or corrupt data";

    // Objects written before rotation must still be readable after all rotations
    EXPECT_EQ(ToString(client().get(id1)), base_payload + "1");
    EXPECT_EQ(ToString(client().get(id2)), base_payload + "2");
}

// Designed to trigger ThreadSanitizer (TSAN) reports if run under
// -fsanitize=thread. Exercises all code paths that touch shared mutable state
// (MetadataStore, kek_ring_, active_kek_id_) from
// multiple threads simultaneously.
TEST_F(StorageClientTest, ThreadSanitizerCleanRun) {
    constexpr int kThreads = 6;

    // Pre-populate some objects
    for (int i = 0; i < 3; ++i) {
        std::string id = "00000000-0000-0000-0000-000000002" +
            std::to_string(100 + i);
        client().put(id, ToBytes("tsan-seed-" + std::to_string(i) + "-padding"), TestErasureSpec());
    }

    std::atomic<int> errors{0};
    std::vector<std::thread> threads;

    // Mix of all operation types running simultaneously
    auto worker = [&](int tid) {
        try {
            switch (tid % 6) {
                case 0: // put
                {
                    std::string id = "00000000-0000-0000-0000-000000003" +
                        std::to_string(100 + tid);
                    client().put(id, ToBytes("tsan-put-" + std::to_string(tid)), TestErasureSpec());
                    break;
                }
                case 1: // get
                {
                    std::string id = "00000000-0000-0000-0000-000000002100";
                    Bytes got = client().get(id);
                    if (got.empty()) ++errors;
                    break;
                }
                case 2: // list
                    client().list();
                    break;
                case 3: // remove
                {
                    std::string id = "00000000-0000-0000-0000-000000002" +
                        std::to_string(100 + (tid % 3));
                    // May return false if already removed by another thread
                    (void)client().remove(id);
                    break;
                }
                case 4: // rotate
                    client().rotate();
                    break;
                case 5: // put + get same object
                {
                    std::string id = "00000000-0000-0000-0000-000000004" +
                        std::to_string(100 + tid);
                    client().put(id, ToBytes("tsan-rw-" + std::to_string(tid)), TestErasureSpec());
                    Bytes got = client().get(id);
                    if (got.empty()) ++errors;
                    break;
                }
            }
        } catch (const std::exception&) {
            // get() after remove() may throw "not found" - that's expected
        }
    };

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& th : threads) {
        th.join();
    }

    // The primary assertion is that we reach here without TSAN reports,
    // crashes, or deadlocks. The error counter guards against silent corruption.
    EXPECT_EQ(errors.load(), 0);
}

// ===========================================================================
// Health and Repair tests
// ===========================================================================

TEST_F(StorageClientTest, HealthReportsFullyReplicatedAfterPut) {
    const std::string object_id = "00000000-0000-0000-0000-000000005001";
    client().put(object_id, ToBytes("health-check-payload"), TestErasureSpec());

    ObjectHealth h = client().health(object_id);

    EXPECT_EQ(h.object_id, object_id);
    EXPECT_EQ(h.total_shards, 3u);
    EXPECT_EQ(h.required_shards, 2u);
    EXPECT_EQ(h.available_shards, 3u);
    EXPECT_TRUE(h.healthy);
    EXPECT_TRUE(h.fully_replicated);
    EXPECT_TRUE(h.missing_indices.empty());
}

TEST_F(StorageClientTest, HealthReportsDegradedAfterShardLoss) {
    const std::string object_id = "00000000-0000-0000-0000-000000005002";
    client().put(object_id, ToBytes("degraded-health-test"), TestErasureSpec());

    // Delete shard 0 from node 9001
    const auto items = client().list();
    const ObjectMetadata* meta = FindObjectMetadata(items, object_id);
    ASSERT_NE(meta, nullptr);
    ASSERT_GE(meta->shard_locations.size(), 1u);

    const std::string& loc0 = meta->shard_locations[0];
    const std::string shard_path = loc0.substr(loc0.find('/') + 1);

    {
        httplib::Client node0("localhost", 9001);
        auto res = node0.Delete("/shards?location=" + shard_path);
        ASSERT_TRUE(res);
        ASSERT_EQ(res->status, 200);
    }

    ObjectHealth h = client().health(object_id);

    EXPECT_EQ(h.available_shards, 2u);
    EXPECT_TRUE(h.healthy);         // still readable (k=2 available)
    EXPECT_FALSE(h.fully_replicated);
    ASSERT_EQ(h.missing_indices.size(), 1u);
    EXPECT_EQ(h.missing_indices[0], 0u);
}

TEST_F(StorageClientTest, HealthThrowsForMissingObject) {
    EXPECT_THROW(
        client().health("00000000-0000-0000-0000-ffffffffffff"),
        std::runtime_error
    );
}

TEST_F(StorageClientTest, RepairRestoresMissingShard) {
    const std::string object_id = "00000000-0000-0000-0000-000000005003";
    const std::string payload = "repair-test-payload!";
    client().put(object_id, ToBytes(payload), TestErasureSpec());

    // Delete shard 0
    const auto items = client().list();
    const ObjectMetadata* meta = FindObjectMetadata(items, object_id);
    ASSERT_NE(meta, nullptr);

    const std::string& loc0 = meta->shard_locations[0];
    const std::string shard_path = loc0.substr(loc0.find('/') + 1);

    {
        httplib::Client node0("localhost", 9001);
        auto res = node0.Delete("/shards?location=" + shard_path);
        ASSERT_TRUE(res);
        ASSERT_EQ(res->status, 200);
    }

    // Verify degraded
    ObjectHealth h_before = client().health(object_id);
    ASSERT_FALSE(h_before.fully_replicated);

    // Repair
    bool repaired = client().repair(object_id);
    EXPECT_TRUE(repaired);

    // Verify fully replicated after repair
    ObjectHealth h_after = client().health(object_id);
    EXPECT_TRUE(h_after.fully_replicated);
    EXPECT_EQ(h_after.available_shards, 3u);
    EXPECT_TRUE(h_after.missing_indices.empty());

    // Data still readable and correct
    Bytes restored = client().get(object_id);
    EXPECT_EQ(ToString(restored), payload);
}

TEST_F(StorageClientTest, RepairReturnsTrueWhenNothingToRepair) {
    const std::string object_id = "00000000-0000-0000-0000-000000005004";
    client().put(object_id, ToBytes("already-healthy-data"), TestErasureSpec());

    bool repaired = client().repair(object_id);
    EXPECT_TRUE(repaired);
}

TEST_F(StorageClientTest, RepairThrowsWhenTooFewShardsRemain) {
    const std::string object_id = "00000000-0000-0000-0000-000000005005";
    client().put(object_id, ToBytes("unrecoverable-test!!"), TestErasureSpec());

    // Delete 2 of 3 shards (need k=2 to reconstruct, only 1 remains)
    const auto items = client().list();
    const ObjectMetadata* meta = FindObjectMetadata(items, object_id);
    ASSERT_NE(meta, nullptr);
    ASSERT_EQ(meta->shard_locations.size(), 3u);

    for (int i = 0; i < 2; ++i) {
        const std::string& loc = meta->shard_locations[i];
        const auto slash = loc.find('/');
        const std::string node_addr = loc.substr(0, slash);
        const std::string shard_path = loc.substr(slash + 1);

        auto [host, port] = std::pair{
            node_addr.substr(0, node_addr.rfind(':')),
            std::stoi(node_addr.substr(node_addr.rfind(':') + 1))
        };

        httplib::Client node(host, port);
        auto res = node.Delete("/shards?location=" + shard_path);
        ASSERT_TRUE(res);
    }

    EXPECT_THROW(client().repair(object_id), std::runtime_error);
}

TEST_F(StorageClientTest, RepairSkipsDownButRegisteredNodes) {
    const std::string object_id = "00000000-0000-0000-0000-000000005006";
    const std::string payload = "repair-skip-down-registered";
    client().put(object_id, ToBytes(payload), TestErasureSpec());

    // Delete shard 0 from node 9001 so repair has one missing shard.
    const auto items = client().list();
    const ObjectMetadata* meta = FindObjectMetadata(items, object_id);
    ASSERT_NE(meta, nullptr);
    ASSERT_EQ(meta->shard_locations.size(), 3u);

    const std::string& loc0 = meta->shard_locations[0];
    const std::string shard_path = loc0.substr(loc0.find('/') + 1);

    {
        httplib::Client node0("localhost", 9001);
        auto res = node0.Delete("/shards?location=" + shard_path);
        ASSERT_TRUE(res);
        ASSERT_EQ(res->status, 200);
    }

    ObjectHealth h_before = client().health(object_id);
    ASSERT_FALSE(h_before.fully_replicated);

    // Keep one down-but-registered node in the registry and remove 9001
    // to force repair candidate selection to include a dead endpoint if
    // health filtering is missing.
    EXPECT_TRUE(client().metadata_store().unregister_node("localhost:9001"));
    client().metadata_store().register_node("localhost:9199");

    bool repaired = false;
    EXPECT_NO_THROW(repaired = client().repair(object_id));
    EXPECT_TRUE(repaired);

    ObjectHealth h_after = client().health(object_id);
    EXPECT_TRUE(h_after.fully_replicated);
    EXPECT_EQ(h_after.available_shards, 3u);

    Bytes restored = client().get(object_id);
    EXPECT_EQ(ToString(restored), payload);
}

// ===========================================================================
// Validation tests for put() admission checks
// ===========================================================================

TEST_F(StorageClientTest, PutRejectsZeroDataShards) {
    ErasureSpec spec = TestErasureSpec();
    spec.data_shards = 0;
    EXPECT_THROW(
        client().put("00000000-0000-0000-0000-000000006001", ToBytes("data"), spec),
        std::invalid_argument
    );
}

TEST_F(StorageClientTest, PutRejectsZeroParityShards) {
    ErasureSpec spec = TestErasureSpec();
    spec.parity_shards = 0;
    EXPECT_THROW(
        client().put("00000000-0000-0000-0000-000000006002", ToBytes("data"), spec),
        std::invalid_argument
    );
}

TEST_F(StorageClientTest, PutRejectsKPlusMOver255) {
    ErasureSpec spec;
    spec.data_shards = 200;
    spec.parity_shards = 56;
    spec.shard_size = 20;
    EXPECT_THROW(
        client().put("00000000-0000-0000-0000-000000006003", ToBytes("data"), spec),
        std::invalid_argument
    );
}

TEST_F(StorageClientTest, PutRejectsZeroShardSize) {
    ErasureSpec spec = TestErasureSpec();
    spec.shard_size = 0;
    EXPECT_THROW(
        client().put("00000000-0000-0000-0000-000000006004", ToBytes("data"), spec),
        std::invalid_argument
    );
}

TEST_F(StorageClientTest, PutRejectsPayloadExceedingKTimesShardSize) {
    ErasureSpec spec = TestErasureSpec(); // k=2, shard_size=20 → max 40 bytes
    Bytes oversized(41, 'x');
    EXPECT_THROW(
        client().put("00000000-0000-0000-0000-000000006005", oversized, spec),
        std::invalid_argument
    );
}

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}