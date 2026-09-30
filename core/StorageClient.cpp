#include "StorageClient.hpp"

#include "Crypto.hpp"
#include "ErasureCodec.hpp"
#include "ShardTransport.hpp"

#include <botan/auto_rng.h>
#include <botan/mem_ops.h>
#include <botan/pk_algs.h>
#include <httplib.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

// =========================
// Constants
// =========================

const std::string kDefaultKekFilePath = "/tmp/myc_kek.json";

std::string parseKekFilePath() {
    const char* env = std::getenv("MYC_KEYSTORE_PATH");
    if (env) {
        return env;
    }
    return kDefaultKekFilePath;
}

std::string generateKekId() {
    Botan::AutoSeeded_RNG rng;
    std::array<uint8_t, 16> bytes{};
    rng.randomize(bytes.data(), bytes.size());

    // RFC4122 UUIDv4: set version and variant bits.
    bytes[6] = static_cast<uint8_t>((bytes[6] & 0x0F) | 0x40);
    bytes[8] = static_cast<uint8_t>((bytes[8] & 0x3F) | 0x80);

    constexpr char kHex[] = "0123456789abcdef";
    std::string uuid;
    uuid.reserve(36);

    for (std::size_t i = 0; i < bytes.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            uuid.push_back('-');
        }
        uuid.push_back(kHex[(bytes[i] >> 4) & 0x0F]);
        uuid.push_back(kHex[bytes[i] & 0x0F]);
    }

    return uuid;
}

std::string generateVersion() {
    Botan::AutoSeeded_RNG rng;
    std::array<uint8_t, 8> bytes{};
    rng.randomize(bytes.data(), bytes.size());

    constexpr char kHex[] = "0123456789abcdef";
    std::string version;
    version.reserve(16);

    for (uint8_t b : bytes) {
        version.push_back(kHex[(b >> 4) & 0x0F]);
        version.push_back(kHex[b & 0x0F]);
    }

    return version;
}

// =========================
// Helpers
// =========================

std::string bytes_to_hex(const Bytes& bytes) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');

    for (uint8_t b : bytes) {
        oss << std::setw(2) << static_cast<int>(b);
    }

    return oss.str();
}

bool is_node_healthy(const std::string& node_address) {
    try {
        auto& client = get_node_client(node_address);

        auto res = client.Get("/health");
        return res && res->status == 200;
    } catch (...) {
        return false;
    }
}

std::array<uint8_t, 32> recover_dek(
    const Bytes& encrypted_dek,
    const std::string& kek_id,
    const std::unordered_map<
        std::string,
        std::unique_ptr<Botan::Private_Key>
    >& kek_ring,
    std::shared_mutex& kek_mu
) {
    std::array<uint8_t, 32> dek{};

    std::shared_lock<std::shared_mutex> kek_lock(kek_mu);

    auto try_decrypt = [&](const Botan::Private_Key& key) -> bool {
        try {
            dek = decrypt_dek(encrypted_dek, key);
            return true;
        } catch (...) {
            return false;
        }
    };

    auto it = kek_ring.find(kek_id);
    if (it != kek_ring.end()) {
        if (try_decrypt(*it->second)) {
            return dek;
        }
    }

    for (const auto& [id, key] : kek_ring) {
        if (id == kek_id) {
            continue;
        }

        if (try_decrypt(*key)) {
            return dek;
        }
    }

    throw std::runtime_error(
        "recover_dek: cannot decrypt DEK with any available KEK"
    );
}

} // namespace

// =========================
// StorageClient
// =========================

StorageClient::StorageClient(const std::string& metadata_conn_str)
    : metadata_store_(metadata_conn_str),
      kek_file_path_(parseKekFilePath()) {
    if (std::filesystem::exists(kek_file_path_)) {
        auto data = load_kek_file(kek_file_path_);
        kek_ring_ = std::move(data.keys);
        active_kek_id_ = std::move(data.active_kek_id);
    } else {
        Botan::AutoSeeded_RNG rng;
        auto key = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");
        if (!key) {
            throw std::runtime_error("StorageClient: failed to create ML-KEM-768 key");
        }

        do {
            active_kek_id_ = generateKekId();
        } while (kek_ring_.count(active_kek_id_) != 0);

        const auto inserted = kek_ring_.emplace(active_kek_id_, std::move(key));
        if (!inserted.second) {
            throw std::runtime_error("StorageClient: failed to insert initial KEK");
        }

        save_kek_file(kek_file_path_, kek_ring_, active_kek_id_);
    }
}

StorageClient::~StorageClient() = default;

void StorageClient::init() {
    for (const auto& node_address : metadata_store_.list_nodes()) {
        auto& client = get_node_client(node_address);

        auto res = client.Get("/health");

        if (!res || res->status != 200) {
            throw std::runtime_error(
                "StorageClient::init: node not healthy: " + node_address
            );
        }
    }
}

void StorageClient::put(const std::string& object_id, const Bytes& bytes, const ErasureSpec& erasure_spec) {
    if (object_id.empty()) {
        throw std::invalid_argument("StorageClient::put: object_id cannot be empty");
    }

    if (bytes.empty()) {
        throw std::invalid_argument("StorageClient::put: bytes cannot be empty");
    }

    // Validate erasure parameters
    if (erasure_spec.data_shards == 0) {
        throw std::invalid_argument("StorageClient::put: k (data_shards) must be > 0");
    }

    if (erasure_spec.parity_shards == 0) {
        throw std::invalid_argument("StorageClient::put: m (parity_shards) must be > 0");
    }

    if (erasure_spec.data_shards + erasure_spec.parity_shards > 255) {
        throw std::invalid_argument("StorageClient::put: k + m must be <= 255");
    }

    if (erasure_spec.shard_size == 0) {
        throw std::invalid_argument("StorageClient::put: shard_size must be > 0");
    }

    if (bytes.size() > static_cast<std::size_t>(erasure_spec.data_shards) * erasure_spec.shard_size) {
        throw std::invalid_argument("StorageClient::put: bytes.size() exceeds k * shard_size");
    }

    // Validate eligible node count
    const std::vector<std::string> eligible_nodes = metadata_store_.list_nodes();
    const std::size_t required_nodes =
        static_cast<std::size_t>(erasure_spec.data_shards) + erasure_spec.parity_shards;

    if (eligible_nodes.size() < required_nodes) {
        throw std::invalid_argument(
            "StorageClient::put: eligible nodes (" + std::to_string(eligible_nodes.size()) +
            ") < k + m (" + std::to_string(required_nodes) + ")"
        );
    }

    // 1) Generate checksum from bytes
    const Bytes checksum_bytes = checksum_sha256(bytes);
    const std::string checksum_hex = bytes_to_hex(checksum_bytes);

    // Metrics: phase timers
    double crypto_ms = 0.0;
    double transport_ms = 0.0;
    double metadata_ms = 0.0;
    auto put_start = std::chrono::steady_clock::now();

    // 3) Generate plain shards from bytes
    std::vector<PlainShard> plain_shards;
    std::vector<EncryptedShard> encrypted_shards;
    Bytes wrapped_dek;
    std::vector<Bytes> serialized_shards;

    {
        ScopedTimer crypto_timer(&crypto_ms);

        plain_shards = encode(bytes, erasure_spec);

        const std::size_t expected_total_shards =
            static_cast<std::size_t>(erasure_spec.data_shards + erasure_spec.parity_shards);

        if (plain_shards.size() != expected_total_shards) {
            throw std::runtime_error(
                "StorageClient::put: encode() returned unexpected number of shards"
            );
        }

        // 4) Generate per-object DEK, encrypt shards, wrap DEK with KEK
        Botan::AutoSeeded_RNG rng;
        std::array<uint8_t, 32> dek;
        rng.randomize(dek.data(), dek.size());

        encrypted_shards = encrypt_shards(plain_shards, dek);

        if (encrypted_shards.size() != plain_shards.size()) {
            throw std::runtime_error(
                "StorageClient::put: encrypt_shards() returned unexpected number of shards"
            );
        }

        {
            std::shared_lock<std::shared_mutex> kek_lock(kek_mu_);
            wrapped_dek = encrypt_dek(dek, *kek_ring_.at(active_kek_id_)->public_key());
        }
        Botan::secure_scrub_memory(dek.data(), dek.size());

        serialized_shards.reserve(encrypted_shards.size());
        for (const auto& encrypted_shard : encrypted_shards) {
            serialized_shards.push_back(encrypted_shard.serialize());
        }
    }

    // 5) placement_strategy(serialized_shards)
    const std::string version = generateVersion();
    PlacementMap placement = placement_strategy(object_id, version, serialized_shards, eligible_nodes);

    // 6) apply_placement(map) over HTTP
    {
        ScopedTimer transport_timer(&transport_ms);
        apply_placement(placement);
    }

    // 7) Generate metadata and store using MetadataStore
    ObjectMetadata metadata;
    metadata.id = object_id;
    metadata.size = bytes.size();
    metadata.checksum = checksum_hex;
    metadata.erasure = erasure_spec;
    metadata.shard_locations = build_shard_locations(placement);
    metadata.encrypted_dek = wrapped_dek;
    metadata.kek_id = active_kek_id_;

    {
        ScopedTimer metadata_timer(&metadata_ms);
        metadata_store_.put(metadata);
    }

    // Record metrics
    if (collector_) {
        double total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - put_start).count();

        std::size_t total_stored = 0;
        for (const auto& s : serialized_shards) {
            total_stored += s.size();
        }

        OperationRecord rec;
        rec.operation = "PUT";
        rec.params.k = erasure_spec.data_shards;
        rec.params.m = erasure_spec.parity_shards;
        rec.params.shard_size = erasure_spec.shard_size;
        rec.params.object_size = bytes.size();
        rec.params.total_nodes = static_cast<uint32_t>(eligible_nodes.size());
        rec.timing.crypto_ms = crypto_ms;
        rec.timing.transport_ms = transport_ms;
        rec.timing.metadata_ms = metadata_ms;
        rec.timing.total_ms = total_ms;
        rec.success = true;
        rec.storage_overhead = static_cast<double>(total_stored) / static_cast<double>(bytes.size());
        collector_->record(std::move(rec));
    }
}

Bytes StorageClient::get(const std::string& object_id) {
    if (object_id.empty()) {
        throw std::invalid_argument("StorageClient::get: object_id cannot be empty");
    }

    // Metrics: phase timers
    double crypto_ms = 0.0;
    double transport_ms = 0.0;
    double metadata_ms = 0.0;
    auto get_start = std::chrono::steady_clock::now();

    // 1) Load metadata
    std::optional<ObjectMetadata> metadata_opt;
    {
        ScopedTimer metadata_timer(&metadata_ms);
        metadata_opt = metadata_store_.get(object_id);
    }
    if (!metadata_opt.has_value()) {
        throw std::runtime_error("StorageClient::get: object not found: " + object_id);
    }

    const ObjectMetadata& metadata = *metadata_opt;

    // 2) Parse shard locations into:
    //    { "host:port" -> vector<"object_id/version/shard_index"> }
    const ShardLocationMap shard_location_map =
        parse_shard_locations(metadata.shard_locations);

    // 3) Fetch encrypted shard payloads from nodes over HTTP
    std::vector<EncryptedShard> encrypted_shards;
    {
        ScopedTimer transport_timer(&transport_ms);
        encrypted_shards = fetch_encrypted_shards(shard_location_map);
    }

    if (encrypted_shards.size() < metadata.erasure.data_shards) {
        throw std::runtime_error(
            "StorageClient::get: not enough shards to reconstruct object"
        );
    }

    // 4) Recover DEK, decrypt, decode
    std::vector<PlainShard> plain_shards;
    Bytes decoded;
    {
        ScopedTimer crypto_timer(&crypto_ms);

        std::array<uint8_t, 32> dek = recover_dek(
            metadata.encrypted_dek,
            metadata.kek_id,
            kek_ring_,
            kek_mu_
        );

        plain_shards = decrypt_shards(encrypted_shards, dek);
        Botan::secure_scrub_memory(dek.data(), dek.size());

        // 5) Decode original bytes
        if (plain_shards.size() < metadata.erasure.data_shards) {
            throw std::runtime_error(
                "StorageClient::get: not enough valid shards to reconstruct object"
            );
        }

        decoded = decode(plain_shards, metadata.erasure, metadata.size);
    }

    // 6) Verify checksum
    const std::string checksum_hex = bytes_to_hex(checksum_sha256(decoded));
    if (checksum_hex != metadata.checksum) {
        throw std::runtime_error("StorageClient::get: checksum mismatch");
    }

    // Record metrics
    if (collector_) {
        double total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - get_start).count();

        OperationRecord rec;
        rec.operation = "GET";
        rec.params.k = metadata.erasure.data_shards;
        rec.params.m = metadata.erasure.parity_shards;
        rec.params.shard_size = metadata.erasure.shard_size;
        rec.params.object_size = metadata.size;
        rec.params.total_nodes = static_cast<uint32_t>(metadata.shard_locations.size());
        rec.timing.crypto_ms = crypto_ms;
        rec.timing.transport_ms = transport_ms;
        rec.timing.metadata_ms = metadata_ms;
        rec.timing.total_ms = total_ms;
        rec.success = true;
        collector_->record(std::move(rec));
    }

    return decoded;
}

bool StorageClient::remove(const std::string& object_id) {
    if (object_id.empty()) {
        throw std::invalid_argument("StorageClient::remove: object_id cannot be empty");
    }

    // 1) Load metadata
    const auto metadata_opt = metadata_store_.get(object_id);
    if (!metadata_opt.has_value()) {
        return false;
    }

    const ObjectMetadata& metadata = *metadata_opt;

    // 2) Parse metadata shard locations into:
    //    { "host:port" -> vector<"object_id/version/shard_index"> }
    const ShardLocationMap shard_location_map =
        parse_shard_locations(metadata.shard_locations);

    // 3) Remove shard payloads from storage nodes over HTTP
    remove_from_nodes(shard_location_map);

    // 4) Remove metadata
    return metadata_store_.remove(object_id);
}

std::vector<ObjectMetadata> StorageClient::list() {
    return metadata_store_.list();
}

void StorageClient::rotate() {
    std::unique_lock<std::shared_mutex> kek_lock(kek_mu_);

    Botan::AutoSeeded_RNG rng;
    auto key = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");
    if (!key) {
        throw std::runtime_error("StorageClient::rotate: failed to create ML-KEM-768 key");
    }

    do {
        active_kek_id_ = generateKekId();
    } while (kek_ring_.count(active_kek_id_) != 0);

    const auto inserted = kek_ring_.emplace(active_kek_id_, std::move(key));
    if (!inserted.second) {
        throw std::runtime_error("StorageClient::rotate: generated duplicate KEK ID");
    }

    save_kek_file(kek_file_path_, kek_ring_, active_kek_id_);
}

ObjectHealth StorageClient::health(const std::string& object_id) {
    if (object_id.empty()) {
        throw std::invalid_argument("StorageClient::health: object_id cannot be empty");
    }

    const auto metadata_opt = metadata_store_.get(object_id);
    if (!metadata_opt.has_value()) {
        throw std::runtime_error("StorageClient::health: object not found: " + object_id);
    }

    const ObjectMetadata& metadata = *metadata_opt;

    ObjectHealth result;
    result.object_id = object_id;
    result.total_shards = metadata.erasure.data_shards + metadata.erasure.parity_shards;
    result.required_shards = metadata.erasure.data_shards;
    result.available_shards = 0;

    for (std::size_t i = 0; i < metadata.shard_locations.size(); ++i) {
        const auto& shard_location = metadata.shard_locations[i];
        const auto pos = shard_location.find('/');
        if (pos == std::string::npos) {
            result.missing_indices.push_back(static_cast<uint32_t>(i));
            continue;
        }

        const std::string node_address = shard_location.substr(0, pos);
        const std::string location = shard_location.substr(pos + 1);

        if (probe_shard(node_address, location)) {
            ++result.available_shards;
        } else {
            result.missing_indices.push_back(static_cast<uint32_t>(i));
        }
    }

    result.healthy = result.available_shards >= result.required_shards;
    result.fully_replicated = result.available_shards == result.total_shards;

    return result;
}

bool StorageClient::repair(const std::string& object_id) {
    if (object_id.empty()) {
        throw std::invalid_argument("StorageClient::repair: object_id cannot be empty");
    }

    // Metrics: phase timers
    double crypto_ms = 0.0;
    double transport_fetch_ms = 0.0;
    double transport_apply_ms = 0.0;
    double metadata_get_ms = 0.0;
    double metadata_put_ms = 0.0;
    auto repair_start = std::chrono::steady_clock::now();

    // 1) Load metadata and record version
    std::optional<ObjectMetadata> metadata_opt;
    {
        ScopedTimer metadata_timer(&metadata_get_ms);
        metadata_opt = metadata_store_.get(object_id);
    }
    if (!metadata_opt.has_value()) {
        throw std::runtime_error("StorageClient::repair: object not found: " + object_id);
    }

    const ObjectMetadata& metadata = *metadata_opt;
    const int expected_version = metadata.version;

    // Metrics: number of shards needing repair (set after probing). A REPAIR
    // record is emitted on both success and failure so that recovery success
    // rate is derivable from REPAIR records (ADR-0007). Non-exception early
    // returns (nothing to repair, concurrent mutation) are not recorded.
    uint32_t failed_count = 0;

    auto record_repair = [&](bool success) {
        if (!collector_) {
            return;
        }

        double total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - repair_start).count();

        OperationRecord rec;
        rec.operation = "REPAIR";
        rec.params.k = metadata.erasure.data_shards;
        rec.params.m = metadata.erasure.parity_shards;
        rec.params.shard_size = metadata.erasure.shard_size;
        rec.params.object_size = metadata.size;
        rec.params.failed_nodes = failed_count;
        rec.params.total_nodes = static_cast<uint32_t>(metadata.shard_locations.size());
        rec.timing.crypto_ms = crypto_ms;
        rec.timing.transport_ms = transport_fetch_ms + transport_apply_ms;
        rec.timing.metadata_ms = metadata_get_ms + metadata_put_ms;
        rec.timing.total_ms = total_ms;
        rec.success = success;
        collector_->record(std::move(rec));
    };

    try {
        // 2) Probe shards to identify missing/surviving
        std::vector<uint32_t> missing_indices;
        std::vector<uint32_t> surviving_indices;

        for (std::size_t i = 0; i < metadata.shard_locations.size(); ++i) {
            const auto& shard_location = metadata.shard_locations[i];
            const auto pos = shard_location.find('/');
            if (pos == std::string::npos) {
                missing_indices.push_back(static_cast<uint32_t>(i));
                continue;
            }

            const std::string node_address = shard_location.substr(0, pos);
            const std::string location = shard_location.substr(pos + 1);

            if (probe_shard(node_address, location)) {
                surviving_indices.push_back(static_cast<uint32_t>(i));
            } else {
                missing_indices.push_back(static_cast<uint32_t>(i));
            }
        }

        failed_count = static_cast<uint32_t>(missing_indices.size());

        // 3) Nothing to repair
        if (missing_indices.empty()) {
            return true;
        }

        // 4) Not enough surviving shards
        if (surviving_indices.size() < metadata.erasure.data_shards) {
            throw std::runtime_error(
                "StorageClient::repair: not enough surviving shards to reconstruct"
            );
        }

        // 5) Recover DEK
        std::array<uint8_t, 32> dek = recover_dek(
            metadata.encrypted_dek,
            metadata.kek_id,
            kek_ring_,
            kek_mu_
        );

        // 6) Fetch surviving encrypted shards
        ShardLocationMap surviving_location_map;
        for (uint32_t idx : surviving_indices) {
            const auto& shard_location = metadata.shard_locations[idx];
            const auto pos = shard_location.find('/');
            const std::string node_address = shard_location.substr(0, pos);
            const std::string location = shard_location.substr(pos + 1);
            surviving_location_map[node_address].push_back(location);
        }

        std::vector<EncryptedShard> encrypted_shards;
        {
            ScopedTimer transport_timer(&transport_fetch_ms);
            encrypted_shards = fetch_encrypted_shards(surviving_location_map);
        }

        if (encrypted_shards.size() < metadata.erasure.data_shards) {
            throw std::runtime_error(
                "StorageClient::repair: failed to fetch enough surviving shards"
            );
        }

        // 7) Decrypt surviving shards, reconstruct, re-encrypt missing
        std::vector<EncryptedShard> repaired_encrypted;
        {
            ScopedTimer crypto_timer(&crypto_ms);

            std::vector<PlainShard> plain_shards = decrypt_shards(encrypted_shards, dek);

            if (plain_shards.size() < metadata.erasure.data_shards) {
                throw std::runtime_error(
                    "StorageClient::repair: not enough valid decrypted shards"
                );
            }

            // 8) Reconstruct all shards
            Bytes decoded = decode(plain_shards, metadata.erasure, metadata.size);

            const std::string checksum_hex =
                bytes_to_hex(checksum_sha256(decoded));

            if (checksum_hex != metadata.checksum) {
                throw std::runtime_error(
                    "StorageClient::repair: checksum mismatch after reconstruction"
                );
            }

            std::vector<PlainShard> all_plain_shards = encode(decoded, metadata.erasure);

            // 9) Re-encrypt only the missing shards
            std::vector<PlainShard> missing_plain_shards;
            for (uint32_t idx : missing_indices) {
                for (const auto& shard : all_plain_shards) {
                    if (shard.index == idx) {
                        missing_plain_shards.push_back(shard);
                        break;
                    }
                }
            }

            repaired_encrypted = encrypt_shards(missing_plain_shards, dek);
        }

        Botan::secure_scrub_memory(dek.data(), dek.size());

        // 10) Place repaired shards on eligible nodes
        const std::vector<std::string> eligible_nodes = metadata_store_.list_nodes();
        std::vector<std::string> healthy_nodes;
        healthy_nodes.reserve(eligible_nodes.size());

        for (const auto& node : eligible_nodes) {
            if (is_node_healthy(node)) {
                healthy_nodes.push_back(node);
            }
        }

        // Build set of nodes already used by surviving shards
        std::vector<std::string> available_for_repair;
        std::unordered_set<std::string> used_nodes;
        for (uint32_t idx : surviving_indices) {
            const auto& shard_location = metadata.shard_locations[idx];
            const auto pos = shard_location.find('/');
            if (pos != std::string::npos) {
                used_nodes.insert(shard_location.substr(0, pos));
            }
        }

        for (const auto& node : healthy_nodes) {
            if (used_nodes.find(node) == used_nodes.end()) {
                available_for_repair.push_back(node);
            }
        }

        // If not enough unused nodes, allow reuse of existing nodes
        if (available_for_repair.size() < missing_indices.size()) {
            for (const auto& node : healthy_nodes) {
                if (used_nodes.count(node) > 0) {
                    available_for_repair.push_back(node);
                }
                if (available_for_repair.size() >= missing_indices.size()) {
                    break;
                }
            }
        }

        if (available_for_repair.size() < missing_indices.size()) {
            throw std::runtime_error(
                "StorageClient::repair: not enough healthy eligible nodes for repaired shards"
            );
        }

        // Extract version from existing shard keys.
        std::string version;
        for (const auto& loc : metadata.shard_locations) {
            const auto slash_pos = loc.find('/');
            if (slash_pos != std::string::npos) {
                const std::string key = loc.substr(slash_pos + 1);
                try {
                    const ParsedShardKey parsed = parse_shard_key(key);
                    version = parsed.version;
                    break;
                } catch (...) {
                    continue;
                }
            }
        }

        if (version.empty()) {
            version = generateVersion();
        }

        // Write repaired shards
        std::vector<Bytes> serialized_repaired;
        serialized_repaired.reserve(repaired_encrypted.size());
        for (const auto& es : repaired_encrypted) {
            serialized_repaired.push_back(es.serialize());
        }

        PlacementMap repair_placement;
        for (std::size_t i = 0; i < missing_indices.size(); ++i) {
            const std::string& node_address = available_for_repair[i];
            const std::string location = make_shard_key(
                object_id, version, missing_indices[i]);
            repair_placement[node_address].push_back({location, serialized_repaired[i]});
        }

        // 11) Write repaired shard payloads first
        {
            ScopedTimer transport_timer(&transport_apply_ms);
            apply_placement(repair_placement);
        }

        // 12) Build updated shard_locations
        std::vector<std::string> updated_shard_locations = metadata.shard_locations;
        for (std::size_t i = 0; i < missing_indices.size(); ++i) {
            uint32_t idx = missing_indices[i];
            const std::string& node_address = available_for_repair[i];
            const std::string location = make_shard_key(object_id, version, idx);
            updated_shard_locations[idx] = node_address + "/" + location;
        }

        // 13) Conditional metadata commit
        ObjectMetadata updated_metadata = metadata;
        updated_metadata.shard_locations = updated_shard_locations;

        bool committed = false;
        {
            ScopedTimer metadata_timer(&metadata_put_ms);
            committed = metadata_store_.conditional_put(updated_metadata, expected_version);
        }
        if (!committed) {
            return false; // version changed - concurrent mutation
        }

        // Record metrics
        record_repair(true);

        return true;
    } catch (...) {
        // Record the failed recovery attempt, then propagate.
        record_repair(false);
        throw;
    }
}