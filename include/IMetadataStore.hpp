#pragma once

#include "Models.hpp"

#include <optional>
#include <string>
#include <vector>

class IMetadataStore {
public:
    virtual ~IMetadataStore() = default;

    virtual void put(const ObjectMetadata& metadata) = 0;

    virtual bool conditional_put(const ObjectMetadata& metadata, int expected_version) = 0;

    virtual std::optional<ObjectMetadata> get(const std::string& id) = 0;

    virtual bool remove(const std::string& id) = 0;

    virtual std::vector<ObjectMetadata> list() = 0;

    virtual void register_node(const std::string& address) = 0;

    virtual bool unregister_node(const std::string& address) = 0;

    virtual std::vector<std::string> list_nodes() = 0;
};
