#pragma once

#include "Models.hpp"

#include <msgpack.hpp>

#include <cctype>
#include <cstdint>
#include <stdexcept>
#include <string>

enum class MetadataOp : uint8_t {
    Put,
    ConditionalPut,
    Remove,
    RegisterNode,
    UnregisterNode,
};

MSGPACK_ADD_ENUM(MetadataOp);

struct MetadataCommand {
    MetadataOp op = MetadataOp::Put;
    ObjectMetadata metadata;
    int expected_version = 0;
    // Object id for Remove, node address for RegisterNode/UnregisterNode.
    std::string key;
    // Client-chosen id; a retried command with the same id is applied once.
    std::string request_id;

    MSGPACK_DEFINE(op, metadata, expected_version, key, request_id);

    Bytes serialize() const {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, *this);
        return Bytes(sbuf.data(), sbuf.data() + sbuf.size());
    }

    static MetadataCommand deserialize(const uint8_t* data, std::size_t size) {
        if (size == 0) {
            throw std::invalid_argument("MetadataCommand::deserialize: empty buffer");
        }
        msgpack::object_handle handle = msgpack::unpack(reinterpret_cast<const char*>(data), size);
        MetadataCommand result;
        handle.get().convert(result);
        return result;
    }
};

struct MetadataResult {
    bool ok = false;

    MSGPACK_DEFINE(ok);

    Bytes serialize() const {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, *this);
        return Bytes(sbuf.data(), sbuf.data() + sbuf.size());
    }

    static MetadataResult deserialize(const uint8_t* data, std::size_t size) {
        if (size == 0) {
            throw std::invalid_argument("MetadataResult::deserialize: empty buffer");
        }
        msgpack::object_handle handle = msgpack::unpack(reinterpret_cast<const char*>(data), size);
        MetadataResult result;
        handle.get().convert(result);
        return result;
    }
};

inline bool is_uuid(const std::string& s) {
    if (s.size() != 36) {
        return false;
    }
    for (std::size_t i = 0; i < s.size(); ++i) {
        const bool dash = (i == 8 || i == 13 || i == 18 || i == 23);
        if (dash ? s[i] != '-' : !std::isxdigit(static_cast<unsigned char>(s[i]))) {
            return false;
        }
    }
    return true;
}

inline void validate_command(const MetadataCommand& cmd) {
    switch (cmd.op) {
    case MetadataOp::Put:
    case MetadataOp::ConditionalPut:
        if (cmd.metadata.id.empty()) {
            throw std::invalid_argument("ObjectMetadata.id must not be empty");
        }
        if (!is_uuid(cmd.metadata.id)) {
            throw std::invalid_argument("ObjectMetadata.id must be a UUID");
        }
        if (cmd.metadata.checksum.empty()) {
            throw std::invalid_argument("ObjectMetadata.checksum must not be empty");
        }
        for (const auto& location : cmd.metadata.shard_locations) {
            if (location.empty()) {
                throw std::invalid_argument("ObjectMetadata.shard_locations must not contain empty locations");
            }
        }
        break;
    case MetadataOp::Remove:
    case MetadataOp::RegisterNode:
    case MetadataOp::UnregisterNode:
        if (cmd.key.empty()) {
            throw std::invalid_argument("MetadataCommand.key must not be empty");
        }
        break;
    }
}
