#include <gtest/gtest.h>
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <botan/base64.h>

#include "MetadataClient.hpp"

#include <cstdlib>
#include <string>

namespace {

constexpr const char* kDefaultHost = "localhost";
constexpr int kDefaultPort = 8080;

std::string testHost() {
    if (const char* env = std::getenv("STORAGE_CLIENT_SERVER_TEST_HOST")) {
        return std::string{env};
    }
    return kDefaultHost;
}

int testPort() {
    if (const char* env = std::getenv("STORAGE_CLIENT_SERVER_TEST_PORT")) {
        return std::atoi(env);
    }
    return kDefaultPort;
}

// Metadata cluster used by the running storage_client (see setup-integration.sh).
std::string testMetadataEndpoints() {
    if (const char* env = std::getenv("METADATA_TEST_ENDPOINTS")) {
        return std::string{env};
    }
    return "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003";
}

class StorageClientServerTest : public ::testing::Test {
protected:
    httplib::Client client_{testHost(), testPort()};

    void cleanup(const std::string& id) {
        (void)client_.Delete("/objects/" + id);
    }
};

TEST_F(StorageClientServerTest, PutObjectReturnsOk) {
    const std::string id = "00000000-0000-0000-0000-000000000201";
    const std::string data = "hello world";

    nlohmann::json payload;
    payload["data"] = Botan::base64_encode(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
    payload["k"] = 2;
    payload["m"] = 1;
    payload["shard_size"] = 20;

    auto res = client_.Put(
        "/objects/" + id,
        payload.dump(),
        "application/json"
    );

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["status"], "ok");

    cleanup(id);
}

TEST_F(StorageClientServerTest, GetObjectReturnsStoredData) {
    const std::string id = "00000000-0000-0000-0000-000000000202";
    const std::string data = "stored content";

    nlohmann::json payload;
    payload["data"] = Botan::base64_encode(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
    payload["k"] = 2;
    payload["m"] = 1;
    payload["shard_size"] = 20;

    client_.Put("/objects/" + id, payload.dump(), "application/json");

    auto res = client_.Get("/objects/" + id);

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    ASSERT_TRUE(body.contains("data"));

    std::string b64_result = body["data"].get<std::string>();
    auto decoded = Botan::base64_decode(b64_result);
    std::string result(decoded.begin(), decoded.end());
    EXPECT_EQ(result, data);

    cleanup(id);
}

TEST_F(StorageClientServerTest, DeleteObjectReturnsOk) {
    const std::string id = "00000000-0000-0000-0000-000000000203";
    const std::string data = "to be deleted";

    nlohmann::json payload;
    payload["data"] = Botan::base64_encode(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
    payload["k"] = 2;
    payload["m"] = 1;
    payload["shard_size"] = 20;

    client_.Put("/objects/" + id, payload.dump(), "application/json");

    auto res = client_.Delete("/objects/" + id);

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["status"], "ok");
}

TEST_F(StorageClientServerTest, DeleteMissingObjectReturns404) {
    auto res = client_.Delete("/objects/00000000-0000-0000-0000-ffffffffffff");

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 404);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["error"], "not found");
}

TEST_F(StorageClientServerTest, ListObjectsReturnsArray) {
    const std::string id = "00000000-0000-0000-0000-000000000204";
    const std::string data = "list me";

    nlohmann::json payload;
    payload["data"] = Botan::base64_encode(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
    payload["k"] = 2;
    payload["m"] = 1;
    payload["shard_size"] = 20;

    client_.Put("/objects/" + id, payload.dump(), "application/json");

    auto res = client_.Get("/objects");

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    ASSERT_TRUE(body.is_array());
    EXPECT_GE(body.size(), 1u);

    bool found = false;
    for (const auto& item : body) {
        if (item["id"] == id) {
            found = true;
            EXPECT_TRUE(item.contains("size"));
            EXPECT_TRUE(item.contains("checksum"));
            break;
        }
    }
    EXPECT_TRUE(found);

    cleanup(id);
}

TEST_F(StorageClientServerTest, PutInvalidJsonReturns400) {
    auto res = client_.Put(
        "/objects/00000000-0000-0000-0000-000000000205",
        "not json at all",
        "application/json"
    );

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 400);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["error"], "invalid JSON body");
}

TEST_F(StorageClientServerTest, PutMissingDataFieldReturns400) {
    nlohmann::json payload;
    payload["wrong_field"] = "value";

    auto res = client_.Put(
        "/objects/00000000-0000-0000-0000-000000000206",
        payload.dump(),
        "application/json"
    );

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 400);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["error"], "missing or invalid 'data' field");
}

// ===========================================================================
// Health endpoint tests
// ===========================================================================

TEST_F(StorageClientServerTest, HealthReturnsFullyReplicatedForHealthyObject) {
    const std::string id = "00000000-0000-0000-0000-000000000301";
    const std::string data = "health-route-test";

    nlohmann::json payload;
    payload["data"] = Botan::base64_encode(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
    payload["k"] = 2;
    payload["m"] = 1;
    payload["shard_size"] = 20;

    auto put_res = client_.Put("/objects/" + id, payload.dump(), "application/json");
    ASSERT_TRUE(put_res);
    ASSERT_EQ(put_res->status, 200);

    auto res = client_.Get("/objects/" + id + "/health");

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["object_id"], id);
    EXPECT_EQ(body["total_shards"], 3);
    EXPECT_EQ(body["available_shards"], 3);
    EXPECT_EQ(body["required_shards"], 2);
    EXPECT_TRUE(body["healthy"].get<bool>());
    EXPECT_TRUE(body["fully_replicated"].get<bool>());
    EXPECT_TRUE(body["missing_indices"].empty());

    cleanup(id);
}

TEST_F(StorageClientServerTest, HealthReturns500ForMissingObject) {
    auto res = client_.Get("/objects/00000000-0000-0000-0000-ffffffffffff/health");

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 500);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_TRUE(body.contains("error"));
}

// ===========================================================================
// Repair endpoint tests
// ===========================================================================

TEST_F(StorageClientServerTest, RepairReturnsOkForHealthyObject) {
    const std::string id = "00000000-0000-0000-0000-000000000302";
    const std::string data = "repair-route-test!";

    nlohmann::json payload;
    payload["data"] = Botan::base64_encode(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
    payload["k"] = 2;
    payload["m"] = 1;
    payload["shard_size"] = 20;

    auto put_res = client_.Put("/objects/" + id, payload.dump(), "application/json");
    ASSERT_TRUE(put_res);
    ASSERT_EQ(put_res->status, 200);

    auto res = client_.Post("/objects/" + id + "/repair", "", "application/json");

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["status"], "ok");
    EXPECT_TRUE(body["repaired"].get<bool>());

    cleanup(id);
}

TEST_F(StorageClientServerTest, RepairRestoresDegradedObject) {
    const std::string id = "00000000-0000-0000-0000-000000000303";
    const std::string data = "repair-degrade-test";

    nlohmann::json payload;
    payload["data"] = Botan::base64_encode(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
    payload["k"] = 2;
    payload["m"] = 1;
    payload["shard_size"] = 20;

    auto put_res = client_.Put("/objects/" + id, payload.dump(), "application/json");
    ASSERT_TRUE(put_res);
    ASSERT_EQ(put_res->status, 200);

    // Use metadata to find the shard key
    // We know shard 0 is on node 9001 (first eligible node by registry order)
    // Construct the shard location from the object metadata
    {
        MetadataClient store(MetadataClient::parse_endpoints(testMetadataEndpoints()));
        const auto metadata = store.get(id);
        ASSERT_TRUE(metadata.has_value());
        ASSERT_FALSE(metadata->shard_locations.empty());
        const std::string full_location = metadata->shard_locations[0];
        // full_location is "host:port/key" - extract path after first /
        std::string shard_key = full_location.substr(full_location.find('/') + 1);

        httplib::Client node("localhost", 9001);
        auto del_res = node.Delete("/shards?location=" + shard_key);
        ASSERT_TRUE(del_res);
        ASSERT_EQ(del_res->status, 200);
    }

    // Verify degraded
    {
        auto health_res = client_.Get("/objects/" + id + "/health");
        auto body = nlohmann::json::parse(health_res->body);
        EXPECT_FALSE(body["fully_replicated"].get<bool>());
    }

    // Repair
    auto repair_res = client_.Post("/objects/" + id + "/repair", "", "application/json");
    ASSERT_TRUE(repair_res);
    EXPECT_EQ(repair_res->status, 200);

    auto repair_body = nlohmann::json::parse(repair_res->body);
    EXPECT_TRUE(repair_body["repaired"].get<bool>());

    // Verify restored
    {
        auto health_res = client_.Get("/objects/" + id + "/health");
        auto body = nlohmann::json::parse(health_res->body);
        EXPECT_TRUE(body["fully_replicated"].get<bool>());
    }

    // Data still correct
    {
        auto get_res = client_.Get("/objects/" + id);
        ASSERT_TRUE(get_res);
        ASSERT_EQ(get_res->status, 200);
        auto body = nlohmann::json::parse(get_res->body);
        auto decoded = Botan::base64_decode(body["data"].get<std::string>());
        std::string result(decoded.begin(), decoded.end());
        EXPECT_EQ(result, data);
    }

    cleanup(id);
}

TEST_F(StorageClientServerTest, RepairReturns500ForMissingObject) {
    auto res = client_.Post(
        "/objects/00000000-0000-0000-0000-ffffffffffff/repair", "", "application/json");

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 500);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_TRUE(body.contains("error"));
}

} // namespace
