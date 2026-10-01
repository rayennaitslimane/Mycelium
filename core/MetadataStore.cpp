#include "MetadataStore.hpp"

#include "LmdbTxn.hpp"

#include <msgpack.hpp>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace {

constexpr const char* kLastApplied = "last_applied";
constexpr const char* kNextNodeSeq = "next_node_seq";

// Dedup entries older than kDedupWindow log entries are pruned every kDedupPruneEvery entries.
constexpr std::uint64_t kDedupWindow = 100000;
constexpr std::uint64_t kDedupPruneEvery = 1000;

constexpr std::size_t kSnapshotChunkBytes = 1024 * 1024;

// (dbi index, key, value)
using SnapshotRecord = std::tuple<std::uint8_t, std::string, std::string>;

// (log index, result.ok)
using DedupEntry = std::tuple<std::uint64_t, bool>;

using Txn = LmdbTxn;

void check_lmdb(int rc, const char* operation) {
    check_lmdb_rc(rc, operation);
}

MDB_val as_val(const std::string& s) {
    return MDB_val{s.size(), const_cast<char*>(s.data())};
}

ObjectMetadata decode_object(const MDB_val& value) {
    msgpack::object_handle handle =
        msgpack::unpack(static_cast<const char*>(value.mv_data), value.mv_size);
    ObjectMetadata metadata;
    handle.get().convert(metadata);
    return metadata;
}

std::uint64_t decode_u64(const MDB_val& value) {
    if (value.mv_size != sizeof(std::uint64_t)) {
        throw std::runtime_error("MetadataStore: corrupt u64 value");
    }
    std::uint64_t result = 0;
    std::memcpy(&result, value.mv_data, sizeof(result));
    return result;
}

std::string as_string(const MDB_val& value) {
    return std::string(static_cast<const char*>(value.mv_data), value.mv_size);
}

void for_each_entry(
    MDB_txn* txn,
    MDB_dbi dbi,
    const std::function<void(const MDB_val&, const MDB_val&)>& fn
) {
    MDB_cursor* cursor = nullptr;
    check_lmdb(mdb_cursor_open(txn, dbi, &cursor), "mdb_cursor_open");
    MDB_val key{};
    MDB_val value{};
    int rc = mdb_cursor_get(cursor, &key, &value, MDB_FIRST);
    try {
        while (rc == 0) {
            fn(key, value);
            rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
        }
    } catch (...) {
        mdb_cursor_close(cursor);
        throw;
    }
    mdb_cursor_close(cursor);
    if (rc != MDB_NOTFOUND) {
        check_lmdb(rc, "mdb_cursor_get");
    }
}

}  // namespace

MetadataStore::MetadataStore(const std::string& dir) {
    std::filesystem::create_directories(dir);

    check_lmdb(mdb_env_create(&env_), "mdb_env_create");

    try {
        check_lmdb(mdb_env_set_maxdbs(env_, 4), "mdb_env_set_maxdbs");
        check_lmdb(mdb_env_set_mapsize(env_, 1UL * 1024 * 1024 * 1024), "mdb_env_set_mapsize");
        check_lmdb(mdb_env_open(env_, dir.c_str(), MDB_NOTLS, 0664), "mdb_env_open");

        Txn txn(env_, 0);
        check_lmdb(mdb_dbi_open(txn, "objects", MDB_CREATE, &objects_), "mdb_dbi_open(objects)");
        check_lmdb(mdb_dbi_open(txn, "nodes", MDB_CREATE, &nodes_), "mdb_dbi_open(nodes)");
        check_lmdb(mdb_dbi_open(txn, "meta", MDB_CREATE, &meta_), "mdb_dbi_open(meta)");
        check_lmdb(mdb_dbi_open(txn, "dedup", MDB_CREATE, &dedup_), "mdb_dbi_open(dedup)");
        txn.commit();
    } catch (...) {
        mdb_env_close(env_);
        env_ = nullptr;
        throw;
    }
}

MetadataStore::~MetadataStore() {
    if (env_ != nullptr) {
        mdb_env_close(env_);
        env_ = nullptr;
    }
}

MetadataResult MetadataStore::apply(const MetadataCommand& cmd, std::uint64_t log_idx) {
    return commit(cmd, log_idx);
}

std::uint64_t MetadataStore::last_applied() {
    Txn txn(env_, MDB_RDONLY);
    return read_u64(txn, kLastApplied);
}

std::unique_ptr<LmdbTxn> MetadataStore::begin_snapshot_read() {
    return std::make_unique<LmdbTxn>(env_, MDB_RDONLY);
}

std::size_t MetadataStore::write_snapshot_chunks(MDB_txn* txn, const std::string& dir) {
    const MDB_dbi dbis[] = {objects_, nodes_, meta_, dedup_};
    std::vector<SnapshotRecord> records;
    std::size_t bytes = 0;
    std::size_t chunks = 0;

    auto flush = [&] {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, records);
        const auto path = std::filesystem::path(dir) / ("chunk_" + std::to_string(chunks++));
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(sbuf.data(), static_cast<std::streamsize>(sbuf.size()));
        if (!out) {
            throw std::runtime_error("MetadataStore: failed to write " + path.string());
        }
        records.clear();
        bytes = 0;
    };

    for (std::uint8_t i = 0; i < std::size(dbis); ++i) {
        for_each_entry(txn, dbis[i], [&](const MDB_val& key, const MDB_val& value) {
            records.emplace_back(i, as_string(key), as_string(value));
            bytes += key.mv_size + value.mv_size;
            if (bytes >= kSnapshotChunkBytes) {
                flush();
            }
        });
    }
    if (!records.empty() || chunks == 0) {
        flush();
    }
    return chunks;
}

void MetadataStore::restore_snapshot(
    const std::string& dir,
    std::size_t chunks,
    std::uint64_t log_idx
) {
    const MDB_dbi dbis[] = {objects_, nodes_, meta_, dedup_};
    Txn txn(env_, 0);
    for (const MDB_dbi dbi : dbis) {
        check_lmdb(mdb_drop(txn, dbi, 0), "mdb_drop");
    }

    for (std::size_t n = 0; n < chunks; ++n) {
        const auto path = std::filesystem::path(dir) / ("chunk_" + std::to_string(n));
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            throw std::runtime_error("MetadataStore: missing snapshot chunk " + path.string());
        }
        const std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        msgpack::object_handle handle = msgpack::unpack(data.data(), data.size());
        std::vector<SnapshotRecord> records;
        handle.get().convert(records);

        for (auto& [dbi_index, key, value] : records) {
            if (dbi_index >= std::size(dbis)) {
                throw std::runtime_error("MetadataStore: corrupt snapshot chunk " + path.string());
            }
            MDB_val k = as_val(key);
            MDB_val v = as_val(value);
            check_lmdb(mdb_put(txn, dbis[dbi_index], &k, &v, 0), "mdb_put(snapshot)");
        }
    }

    write_u64(txn, kLastApplied, log_idx);
    txn.commit();
}

void MetadataStore::put(const ObjectMetadata& metadata) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::Put;
    cmd.metadata = metadata;
    validate_command(cmd);
    commit(cmd, std::nullopt);
}

bool MetadataStore::conditional_put(const ObjectMetadata& metadata, int expected_version) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::ConditionalPut;
    cmd.metadata = metadata;
    cmd.expected_version = expected_version;
    validate_command(cmd);
    return commit(cmd, std::nullopt).ok;
}

std::optional<ObjectMetadata> MetadataStore::get(const std::string& id) {
    Txn txn(env_, MDB_RDONLY);
    return get_in_txn(txn, id);
}

bool MetadataStore::remove(const std::string& id) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::Remove;
    cmd.key = id;
    validate_command(cmd);
    return commit(cmd, std::nullopt).ok;
}

std::vector<ObjectMetadata> MetadataStore::list() {
    Txn txn(env_, MDB_RDONLY);
    MDB_cursor* cursor = nullptr;
    check_lmdb(mdb_cursor_open(txn, objects_, &cursor), "mdb_cursor_open");

    std::vector<ObjectMetadata> result;
    MDB_val key{};
    MDB_val value{};
    int rc = mdb_cursor_get(cursor, &key, &value, MDB_FIRST);
    try {
        while (rc == 0) {
            result.push_back(decode_object(value));
            rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
        }
    } catch (...) {
        mdb_cursor_close(cursor);
        throw;
    }
    mdb_cursor_close(cursor);

    if (rc != MDB_NOTFOUND) {
        check_lmdb(rc, "mdb_cursor_get");
    }
    return result;
}

void MetadataStore::register_node(const std::string& address) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::RegisterNode;
    cmd.key = address;
    validate_command(cmd);
    commit(cmd, std::nullopt);
}

bool MetadataStore::unregister_node(const std::string& address) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::UnregisterNode;
    cmd.key = address;
    validate_command(cmd);
    return commit(cmd, std::nullopt).ok;
}

std::vector<std::string> MetadataStore::list_nodes() {
    Txn txn(env_, MDB_RDONLY);
    MDB_cursor* cursor = nullptr;
    check_lmdb(mdb_cursor_open(txn, nodes_, &cursor), "mdb_cursor_open");

    std::vector<std::pair<std::uint64_t, std::string>> entries;
    MDB_val key{};
    MDB_val value{};
    int rc = mdb_cursor_get(cursor, &key, &value, MDB_FIRST);
    try {
        while (rc == 0) {
            entries.emplace_back(
                decode_u64(value),
                std::string(static_cast<const char*>(key.mv_data), key.mv_size)
            );
            rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
        }
    } catch (...) {
        mdb_cursor_close(cursor);
        throw;
    }
    mdb_cursor_close(cursor);

    if (rc != MDB_NOTFOUND) {
        check_lmdb(rc, "mdb_cursor_get");
    }

    std::sort(entries.begin(), entries.end());
    std::vector<std::string> result;
    result.reserve(entries.size());
    for (auto& entry : entries) {
        result.push_back(std::move(entry.second));
    }
    return result;
}

MetadataResult MetadataStore::commit(
    const MetadataCommand& cmd,
    std::optional<std::uint64_t> log_idx
) {
    Txn txn(env_, 0);
    const std::uint64_t last = read_u64(txn, kLastApplied);
    if (log_idx.has_value() && *log_idx <= last) {
        return MetadataResult{};
    }
    const std::uint64_t idx = log_idx.value_or(last + 1);

    MetadataResult result;
    MDB_val request_key = as_val(cmd.request_id);
    MDB_val dedup_value{};
    const int rc = cmd.request_id.empty()
        ? MDB_NOTFOUND
        : mdb_get(txn, dedup_, &request_key, &dedup_value);
    if (rc == 0) {
        DedupEntry entry;
        msgpack::unpack(static_cast<const char*>(dedup_value.mv_data), dedup_value.mv_size)
            .get().convert(entry);
        result.ok = std::get<1>(entry);
    } else {
        if (rc != MDB_NOTFOUND) {
            check_lmdb(rc, "mdb_get(dedup)");
        }
        result = apply_in_txn(txn, cmd);
        if (!cmd.request_id.empty()) {
            msgpack::sbuffer sbuf;
            msgpack::pack(sbuf, DedupEntry{idx, result.ok});
            MDB_val v{sbuf.size(), sbuf.data()};
            check_lmdb(mdb_put(txn, dedup_, &request_key, &v, 0), "mdb_put(dedup)");
        }
    }

    // Pruning depends only on idx, so every replica prunes identically.
    if (idx % kDedupPruneEvery == 0 && idx > kDedupWindow) {
        std::vector<std::string> expired;
        for_each_entry(txn, dedup_, [&](const MDB_val& key, const MDB_val& value) {
            DedupEntry entry;
            msgpack::unpack(static_cast<const char*>(value.mv_data), value.mv_size)
                .get().convert(entry);
            if (std::get<0>(entry) + kDedupWindow < idx) {
                expired.push_back(as_string(key));
            }
        });
        for (const auto& key : expired) {
            MDB_val k = as_val(key);
            check_lmdb(mdb_del(txn, dedup_, &k, nullptr), "mdb_del(dedup)");
        }
    }

    write_u64(txn, kLastApplied, idx);
    txn.commit();
    return result;
}

MetadataResult MetadataStore::apply_in_txn(MDB_txn* txn, const MetadataCommand& cmd) {
    MetadataResult result;

    switch (cmd.op) {
    case MetadataOp::Put: {
        const auto existing = get_in_txn(txn, cmd.metadata.id);
        ObjectMetadata metadata = cmd.metadata;
        metadata.version = existing.has_value() ? existing->version + 1 : 1;
        put_object(txn, metadata);
        result.ok = true;
        break;
    }
    case MetadataOp::ConditionalPut: {
        const auto existing = get_in_txn(txn, cmd.metadata.id);
        if (existing.has_value() && existing->version == cmd.expected_version) {
            ObjectMetadata metadata = cmd.metadata;
            metadata.version = existing->version + 1;
            put_object(txn, metadata);
            result.ok = true;
        }
        break;
    }
    case MetadataOp::Remove:
    case MetadataOp::UnregisterNode: {
        MDB_val key = as_val(cmd.key);
        const MDB_dbi dbi = cmd.op == MetadataOp::Remove ? objects_ : nodes_;
        const int rc = mdb_del(txn, dbi, &key, nullptr);
        if (rc != MDB_NOTFOUND) {
            check_lmdb(rc, "mdb_del");
            result.ok = true;
        }
        break;
    }
    case MetadataOp::RegisterNode: {
        MDB_val key = as_val(cmd.key);
        MDB_val value{};
        const int rc = mdb_get(txn, nodes_, &key, &value);
        if (rc == MDB_NOTFOUND) {
            const std::uint64_t seq = read_u64(txn, kNextNodeSeq);
            value = MDB_val{sizeof(seq), const_cast<std::uint64_t*>(&seq)};
            check_lmdb(mdb_put(txn, nodes_, &key, &value, 0), "mdb_put(nodes)");
            write_u64(txn, kNextNodeSeq, seq + 1);
        } else {
            check_lmdb(rc, "mdb_get(nodes)");
        }
        result.ok = true;
        break;
    }
    }

    return result;
}

std::optional<ObjectMetadata> MetadataStore::get_in_txn(MDB_txn* txn, const std::string& id) {
    MDB_val key = as_val(id);
    MDB_val value{};
    const int rc = mdb_get(txn, objects_, &key, &value);
    if (rc == MDB_NOTFOUND) {
        return std::nullopt;
    }
    check_lmdb(rc, "mdb_get(objects)");
    return decode_object(value);
}

void MetadataStore::put_object(MDB_txn* txn, const ObjectMetadata& metadata) {
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, metadata);
    MDB_val key = as_val(metadata.id);
    MDB_val value{sbuf.size(), sbuf.data()};
    check_lmdb(mdb_put(txn, objects_, &key, &value, 0), "mdb_put(objects)");
}

std::uint64_t MetadataStore::read_u64(MDB_txn* txn, const std::string& key) {
    MDB_val k = as_val(key);
    MDB_val value{};
    const int rc = mdb_get(txn, meta_, &k, &value);
    if (rc == MDB_NOTFOUND) {
        return 0;
    }
    check_lmdb(rc, "mdb_get(meta)");
    return decode_u64(value);
}

void MetadataStore::write_u64(MDB_txn* txn, const std::string& key, std::uint64_t value) {
    MDB_val k = as_val(key);
    MDB_val v{sizeof(value), &value};
    check_lmdb(mdb_put(txn, meta_, &k, &v, 0), "mdb_put(meta)");
}
