#include "LmdbTxn.hpp"
#include "MetadataStore.hpp"
#include "Models.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

ErasureSpec makeErasureSpec() {
    // Adapt this if your ErasureSpec constructor differs.
    return ErasureSpec{4, 2};
}

ObjectMetadata makeMetadata(
    std::string id,
    std::size_t size,
    std::string checksum,
    std::vector<std::string> shardLocations
) {
    ObjectMetadata metadata;
    metadata.id = std::move(id);
    metadata.size = size;
    metadata.checksum = std::move(checksum);
    metadata.erasure = makeErasureSpec();
    metadata.shard_locations = std::move(shardLocations);
    metadata.encrypted_dek = Bytes{0xDE, 0xAD, 0xBE, 0xEF};
    metadata.kek_id = "test-kek-id";
    return metadata;
}

void expectMetadataEqual(
    const ObjectMetadata& expected,
    const ObjectMetadata& actual
) {
    EXPECT_EQ(expected.id, actual.id);
    EXPECT_EQ(expected.size, actual.size);
    EXPECT_EQ(expected.checksum, actual.checksum);
    EXPECT_EQ(expected.shard_locations, actual.shard_locations);
    EXPECT_EQ(expected.encrypted_dek, actual.encrypted_dek);
    EXPECT_EQ(expected.kek_id, actual.kek_id);

    // Avoid requiring operator== on ErasureSpec.
    EXPECT_EQ(expected.erasure.serialize(), actual.erasure.serialize());
}

class MetadataStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() /
            ("metadata_store_test_" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
             std::to_string(reinterpret_cast<std::uintptr_t>(this)));
        fs::remove_all(test_dir_);
        store_ = std::make_unique<MetadataStore>(test_dir_.string());
    }

    void TearDown() override {
        store_.reset();
        std::error_code ec;
        fs::remove_all(test_dir_, ec);
    }

    void reopen() {
        store_.reset();
        store_ = std::make_unique<MetadataStore>(test_dir_.string());
    }

    fs::path test_dir_;
    std::unique_ptr<MetadataStore> store_;
};

TEST_F(MetadataStoreTest, PutThenGetRoundTripsMetadata) {
    const auto metadata = makeMetadata(
        "00000000-0000-0000-0000-000000000001",
        12345,
        "sha256:abc123",
        {
            "node-a:/objects/1/shard-0",
            "node-b:/objects/1/shard-1",
            "node-c:/objects/1/shard-2"
        }
    );

    store_->put(metadata);

    const std::optional<ObjectMetadata> loaded =
        store_->get("00000000-0000-0000-0000-000000000001");

    ASSERT_TRUE(loaded.has_value());
    expectMetadataEqual(metadata, *loaded);
}

TEST_F(MetadataStoreTest, GetMissingObjectReturnsNullopt) {
    const std::optional<ObjectMetadata> loaded =
        store_->get("00000000-0000-0000-0000-00000000ffff");

    EXPECT_FALSE(loaded.has_value());
}

TEST_F(MetadataStoreTest, PutExistingObjectUpdatesMetadataAndReplacesShardLocations) {
    const std::string id = "00000000-0000-0000-0000-000000000002";

    const auto original = makeMetadata(
        id,
        100,
        "sha256:old",
        {
            "old-node-a:/old/shard-0",
            "old-node-b:/old/shard-1",
            "old-node-c:/old/shard-2"
        }
    );

    store_->put(original);

    ASSERT_EQ(3u, store_->get(id)->shard_locations.size());

    const auto updated = makeMetadata(
        id,
        999,
        "sha256:new",
        {
            "new-node-a:/new/shard-0"
        }
    );

    store_->put(updated);

    const std::optional<ObjectMetadata> loaded = store_->get(id);

    ASSERT_TRUE(loaded.has_value());
    expectMetadataEqual(updated, *loaded);

    // Verifies old shard locations were replaced instead of appended to.
    EXPECT_EQ(1u, loaded->shard_locations.size());
}

TEST_F(MetadataStoreTest, RemoveExistingObjectDeletesMetadataAndCascadesShardLocations) {
    const std::string id = "00000000-0000-0000-0000-000000000003";

    const auto metadata = makeMetadata(
        id,
        2048,
        "sha256:delete-me",
        {
            "node-a:/delete/shard-0",
            "node-b:/delete/shard-1"
        }
    );

    store_->put(metadata);

    ASSERT_TRUE(store_->get(id).has_value());

    const bool removed = store_->remove(id);

    EXPECT_TRUE(removed);
    EXPECT_FALSE(store_->get(id).has_value());
    EXPECT_TRUE(store_->list().empty());
}

TEST_F(MetadataStoreTest, RemoveMissingObjectReturnsFalse) {
    const bool removed =
        store_->remove("00000000-0000-0000-0000-00000000ffff");

    EXPECT_FALSE(removed);
}

TEST_F(MetadataStoreTest, ListReturnsAllObjectsOrderedById) {
    const auto objectB = makeMetadata(
        "00000000-0000-0000-0000-000000000020",
        20,
        "sha256:b",
        {
            "node-b:/object-b/shard-0"
        }
    );

    const auto objectA = makeMetadata(
        "00000000-0000-0000-0000-000000000010",
        10,
        "sha256:a",
        {
            "node-a:/object-a/shard-0",
            "node-a:/object-a/shard-1"
        }
    );

    const auto objectC = makeMetadata(
        "00000000-0000-0000-0000-000000000030",
        30,
        "sha256:c",
        {}
    );

    // Insert out of order.
    store_->put(objectB);
    store_->put(objectC);
    store_->put(objectA);

    const std::vector<ObjectMetadata> objects = store_->list();

    ASSERT_EQ(3u, objects.size());

    // MetadataStore::list orders by id ASC.
    expectMetadataEqual(objectA, objects[0]);
    expectMetadataEqual(objectB, objects[1]);
    expectMetadataEqual(objectC, objects[2]);
}

TEST_F(MetadataStoreTest, PutAllowsObjectWithNoShardLocations) {
    const auto metadata = makeMetadata(
        "00000000-0000-0000-0000-000000000004",
        4096,
        "sha256:no-shards-yet",
        {}
    );

    store_->put(metadata);

    const std::optional<ObjectMetadata> loaded =
        store_->get("00000000-0000-0000-0000-000000000004");

    ASSERT_TRUE(loaded.has_value());
    expectMetadataEqual(metadata, *loaded);
    EXPECT_TRUE(loaded->shard_locations.empty());
}

TEST_F(MetadataStoreTest, PutRejectsEmptyId) {
    auto metadata = makeMetadata(
        "",
        123,
        "sha256:abc",
        {
            "node-a:/shard-0"
        }
    );

    EXPECT_THROW(store_->put(metadata), std::invalid_argument);
}

TEST_F(MetadataStoreTest, PutRejectsEmptyChecksum) {
    auto metadata = makeMetadata(
        "00000000-0000-0000-0000-000000000005",
        123,
        "",
        {
            "node-a:/shard-0"
        }
    );

    EXPECT_THROW(store_->put(metadata), std::invalid_argument);
}

TEST_F(MetadataStoreTest, PutRejectsEmptyShardLocation) {
    auto metadata = makeMetadata(
        "00000000-0000-0000-0000-000000000006",
        123,
        "sha256:abc",
        {
            "node-a:/shard-0",
            ""
        }
    );

    EXPECT_THROW(store_->put(metadata), std::invalid_argument);
}

TEST_F(MetadataStoreTest, PutRejectsInvalidUuid) {
    auto metadata = makeMetadata(
        "not-a-valid-uuid",
        123,
        "sha256:abc",
        {
            "node-a:/shard-0"
        }
    );

    EXPECT_THROW(store_->put(metadata), std::invalid_argument);
}

TEST_F(MetadataStoreTest, PutAssignsIncrementingVersions) {
    const std::string id = "00000000-0000-0000-0000-000000000007";
    auto metadata = makeMetadata(id, 1, "sha256:v", {"node-a:/v/shard-0"});

    store_->put(metadata);
    EXPECT_EQ(1, store_->get(id)->version);

    store_->put(metadata);
    EXPECT_EQ(2, store_->get(id)->version);
}

TEST_F(MetadataStoreTest, ConditionalPutIsCompareAndSwapOnVersion) {
    const std::string id = "00000000-0000-0000-0000-000000000008";
    auto metadata = makeMetadata(id, 1, "sha256:cas", {"node-a:/cas/shard-0"});

    EXPECT_FALSE(store_->conditional_put(metadata, 0)) << "missing object";

    store_->put(metadata);
    metadata.shard_locations = {"node-b:/cas/shard-0"};

    EXPECT_FALSE(store_->conditional_put(metadata, 7)) << "version mismatch";
    EXPECT_EQ("node-a:/cas/shard-0", store_->get(id)->shard_locations[0]);

    EXPECT_TRUE(store_->conditional_put(metadata, 1));
    const auto loaded = store_->get(id);
    EXPECT_EQ(2, loaded->version);
    EXPECT_EQ("node-b:/cas/shard-0", loaded->shard_locations[0]);
}

TEST_F(MetadataStoreTest, ListNodesPreservesRegistrationOrder) {
    store_->register_node("localhost:9003");
    store_->register_node("localhost:9001");
    store_->register_node("localhost:9002");
    store_->register_node("localhost:9003");  // no-op

    EXPECT_EQ(
        (std::vector<std::string>{"localhost:9003", "localhost:9001", "localhost:9002"}),
        store_->list_nodes()
    );

    EXPECT_TRUE(store_->unregister_node("localhost:9003"));
    EXPECT_FALSE(store_->unregister_node("localhost:9003"));
    store_->register_node("localhost:9003");

    EXPECT_EQ(
        (std::vector<std::string>{"localhost:9001", "localhost:9002", "localhost:9003"}),
        store_->list_nodes()
    );
}

TEST_F(MetadataStoreTest, ApplySkipsAlreadyAppliedLogIndexes) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::Put;
    cmd.metadata = makeMetadata(
        "00000000-0000-0000-0000-000000000009", 1, "sha256:log", {"node-a:/log/shard-0"});

    EXPECT_TRUE(store_->apply(cmd, 5).ok);
    EXPECT_EQ(5u, store_->last_applied());

    // Replays at or below last_applied are no-ops.
    EXPECT_FALSE(store_->apply(cmd, 5).ok);
    EXPECT_FALSE(store_->apply(cmd, 3).ok);
    EXPECT_EQ(1, store_->get(cmd.metadata.id)->version);

    EXPECT_TRUE(store_->apply(cmd, 6).ok);
    EXPECT_EQ(2, store_->get(cmd.metadata.id)->version);
    EXPECT_EQ(6u, store_->last_applied());
}

TEST_F(MetadataStoreTest, StatePersistsAcrossReopen) {
    const auto metadata = makeMetadata(
        "00000000-0000-0000-0000-00000000000a", 42, "sha256:persist", {"node-a:/p/shard-0"});
    store_->put(metadata);
    store_->register_node("localhost:9001");
    const std::uint64_t applied = store_->last_applied();

    reopen();

    const auto loaded = store_->get(metadata.id);
    ASSERT_TRUE(loaded.has_value());
    expectMetadataEqual(metadata, *loaded);
    EXPECT_EQ(std::vector<std::string>{"localhost:9001"}, store_->list_nodes());
    EXPECT_EQ(applied, store_->last_applied());
}

TEST_F(MetadataStoreTest, ApplyDeduplicatesRequestIds) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::Put;
    cmd.request_id = "req-1";
    cmd.metadata = makeMetadata(
        "00000000-0000-0000-0000-00000000000b", 1, "sha256:dedup", {"node-a:/d/shard-0"});

    EXPECT_TRUE(store_->apply(cmd, 1).ok);
    EXPECT_TRUE(store_->apply(cmd, 2).ok) << "duplicate returns the original result";
    EXPECT_EQ(1, store_->get(cmd.metadata.id)->version);
    EXPECT_EQ(2u, store_->last_applied());

    cmd.request_id = "req-2";
    EXPECT_TRUE(store_->apply(cmd, 3).ok);
    EXPECT_EQ(2, store_->get(cmd.metadata.id)->version);
}

TEST_F(MetadataStoreTest, SnapshotRoundTripsIntoAnotherStore) {
    const auto metadata = makeMetadata(
        "00000000-0000-0000-0000-00000000000c", 7, "sha256:snap", {"node-a:/s/shard-0"});
    store_->put(metadata);
    store_->register_node("localhost:9001");

    const fs::path chunks = test_dir_ / "chunks";
    const fs::path other_dir = test_dir_ / "other";
    fs::create_directories(chunks);
    std::size_t count = 0;
    {
        auto txn = store_->begin_snapshot_read();
        count = store_->write_snapshot_chunks(*txn, chunks.string());
    }
    ASSERT_GE(count, 1u);

    MetadataStore other(other_dir.string());
    other.register_node("stale:1");
    other.restore_snapshot(chunks.string(), count, 42);

    const auto loaded = other.get(metadata.id);
    ASSERT_TRUE(loaded.has_value());
    expectMetadataEqual(metadata, *loaded);
    EXPECT_EQ(std::vector<std::string>{"localhost:9001"}, other.list_nodes());
    EXPECT_EQ(42u, other.last_applied());
}

// ===========================================================================
// Concurrency contract validation
// ===========================================================================

// Verifies that concurrent put/get/remove/list on a single MetadataStore
// instance (LMDB MVCC readers, single writer) does not crash, deadlock, or
// produce data races. Validates contract point 1.
TEST_F(MetadataStoreTest, ConcurrentMetadataStoreAccessOnSingleClient) {
    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 10;

    std::atomic<int> errors{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kOpsPerThread; ++i) {
                // Each thread uses its own UUID range to avoid logical conflicts
                std::string id = "00000000-0000-0000-0000-0000000" +
                    std::to_string(50000 + t * 100 + i);

                auto metadata = makeMetadata(
                    id,
                    static_cast<std::size_t>(100 + i),
                    "sha256:concurrent-" + std::to_string(t) + "-" + std::to_string(i),
                    {"node-a:/shard-" + std::to_string(t) + "-" + std::to_string(i)}
                );

                try {
                    // put
                    store_->put(metadata);

                    // get
                    auto retrieved = store_->get(id);
                    if (!retrieved.has_value()) {
                        ++errors;
                        continue;
                    }
                    if (retrieved->id != id) {
                        ++errors;
                    }

                    // list (exercises read path concurrently)
                    auto items = store_->list();
                    (void)items;

                    // remove
                    bool removed = store_->remove(id);
                    if (!removed) {
                        ++errors;
                    }
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
        << "Concurrent MetadataStore operations must not crash or corrupt";

    // After all threads complete, the store should be empty
    // (each thread removes what it inserted)
    auto remaining = store_->list();
    EXPECT_TRUE(remaining.empty())
        << "All objects should have been removed; found " << remaining.size();
}

} // namespace