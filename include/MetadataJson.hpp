#pragma once

#include "Models.hpp"

#include <botan/base64.h>
#include <nlohmann/json.hpp>

#include <string>

inline void to_json(nlohmann::json& j, const ErasureSpec& e) {
    j = {{"data_shards", e.data_shards},
         {"parity_shards", e.parity_shards},
         {"shard_size", e.shard_size}};
}

inline void from_json(const nlohmann::json& j, ErasureSpec& e) {
    j.at("data_shards").get_to(e.data_shards);
    j.at("parity_shards").get_to(e.parity_shards);
    j.at("shard_size").get_to(e.shard_size);
}

inline void to_json(nlohmann::json& j, const ObjectMetadata& m) {
    j = {{"id", m.id},
         {"size", m.size},
         {"checksum", m.checksum},
         {"erasure", m.erasure},
         {"shard_locations", m.shard_locations},
         {"encrypted_dek", Botan::base64_encode(m.encrypted_dek)},
         {"kek_id", m.kek_id},
         {"version", m.version}};
}

inline void from_json(const nlohmann::json& j, ObjectMetadata& m) {
    j.at("id").get_to(m.id);
    j.at("size").get_to(m.size);
    j.at("checksum").get_to(m.checksum);
    j.at("erasure").get_to(m.erasure);
    j.at("shard_locations").get_to(m.shard_locations);
    const auto dek = Botan::base64_decode(j.at("encrypted_dek").get<std::string>());
    m.encrypted_dek.assign(dek.begin(), dek.end());
    j.at("kek_id").get_to(m.kek_id);
    m.version = j.value("version", 0);
}
