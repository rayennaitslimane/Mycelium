#include "MetadataStore.hpp"
#include "Models.hpp"

#include <gtest/gtest.h>
#include <pqxx/pqxx>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

std::string testConnectionString() {
    if (const char* env = std::getenv("METADATASTORE_TEST_CONN")) {
        return std::string{env};
    }

    // Matches docker-compose.yml
    return "host=localhost "
           "port=5433 "
           "dbname=myc_test "
           "user=test_user "
           "password=test_password";
}

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
        connStr_ = testConnectionString();

        // Constructor creates schema and prepares statements.
        store_ = std::make_unique<MetadataStore>(connStr_);

        clearDatabase();
    }

    void TearDown() override {
        clearDatabase();
        store_.reset();
    }

    void clearDatabase() {
        pqxx::connection conn{connStr_};
        pqxx::work tx{conn};

        tx.exec(R"SQL(
            TRUNCATE TABLE
                object_shard_locations,
                object_metadata,
                nodes
            RESTART IDENTITY
            CASCADE
        )SQL");

        tx.commit();
    }

    std::int64_t countShardLocationsForObject(const std::string& objectId) {
        pqxx::connection conn{connStr_};
        pqxx::work tx{conn};

        const pqxx::result result = tx.exec(
            R"SQL(
                SELECT COUNT(*)
                FROM object_shard_locations
                WHERE object_id = $1::uuid
            )SQL",
            pqxx::params{
                objectId
            }
        );

        return result[0][0].as<std::int64_t>();
    }

    std::string connStr_;
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

    ASSERT_EQ(3, countShardLocationsForObject(id));

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

    // Verifies old shard rows were deleted instead of appended to.
    EXPECT_EQ(1, countShardLocationsForObject(id));
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
    ASSERT_EQ(2, countShardLocationsForObject(id));

    const bool removed = store_->remove(id);

    EXPECT_TRUE(removed);
    EXPECT_FALSE(store_->get(id).has_value());

    // object_shard_locations has ON DELETE CASCADE.
    EXPECT_EQ(0, countShardLocationsForObject(id));
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

TEST_F(MetadataStoreTest, PutRejectsInvalidUuidFromDatabaseCast) {
    auto metadata = makeMetadata(
        "not-a-valid-uuid",
        123,
        "sha256:abc",
        {
            "node-a:/shard-0"
        }
    );

    // validate_metadata only checks non-empty ID.
    // PostgreSQL should reject this because prepared SQL casts $1::uuid.
    EXPECT_THROW(store_->put(metadata), std::exception);
}

// ===========================================================================
// Concurrency contract validation
// ===========================================================================

// Verifies that concurrent put/get/remove/list on a single MetadataStore
// instance (bounded pqxx::connection pool) does not crash, deadlock, or produce
// data races. Validates contract point 1: MetadataStore connection isolation.
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