#pragma once

#include "IMetadataStore.hpp"

#include <nlohmann/json.hpp>

#include <cstddef>
#include <mutex>
#include <string>
#include <vector>

// IMetadataStore backed by a metadata_server cluster. Follows leader redirects,
// retries through failover, and tags each mutation with a request_id so retries
// are applied once.
class MetadataClient : public IMetadataStore {
public:
    explicit MetadataClient(std::vector<std::string> endpoints);

    // Splits "host:port,host:port" into endpoints.
    static std::vector<std::string> parse_endpoints(const std::string& csv);

    void put(const ObjectMetadata& metadata) override;

    bool conditional_put(const ObjectMetadata& metadata, int expected_version) override;

    std::optional<ObjectMetadata> get(const std::string& id) override;

    bool remove(const std::string& id) override;

    std::vector<ObjectMetadata> list() override;

    void register_node(const std::string& address) override;

    bool unregister_node(const std::string& address) override;

    std::vector<std::string> list_nodes() override;

private:
    struct Reply {
        int status = 0;
        nlohmann::json body;
    };

    std::vector<std::string> endpoints_;
    std::mutex mu_;
    std::string leader_;
    std::size_t next_ = 0;

    Reply call(const std::string& method, const std::string& path, const nlohmann::json& body = {});

    bool mutate(const std::string& method, const std::string& path, nlohmann::json body);
};
