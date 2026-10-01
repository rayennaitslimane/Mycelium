#pragma once

#include "IMetadataStore.hpp"
#include "MetadataCommand.hpp"
#include "Models.hpp"

#include <lmdb.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

class LmdbTxn;

class MetadataStore : public IMetadataStore {
public:
    explicit MetadataStore(const std::string& dir);

    ~MetadataStore() override;

    MetadataStore(const MetadataStore&) = delete;
    MetadataStore& operator=(const MetadataStore&) = delete;

    MetadataStore(MetadataStore&&) = delete;
    MetadataStore& operator=(MetadataStore&&) = delete;

    // Applies a replicated log entry; entries at or below last_applied() are skipped.
    MetadataResult apply(const MetadataCommand& cmd, std::uint64_t log_idx);

    std::uint64_t last_applied();

    // Read txn pinning the current state; may be used from another thread (MDB_NOTLS).
    std::unique_ptr<LmdbTxn> begin_snapshot_read();

    // Writes every DBI visible in txn as msgpack files dir/chunk_<n>; returns the chunk count.
    std::size_t write_snapshot_chunks(MDB_txn* txn, const std::string& dir);

    // Replaces all state with the chunks in dir and sets last_applied, in one write txn.
    void restore_snapshot(const std::string& dir, std::size_t chunks, std::uint64_t log_idx);

    void put(const ObjectMetadata& metadata) override;

    bool conditional_put(const ObjectMetadata& metadata, int expected_version) override;

    std::optional<ObjectMetadata> get(const std::string& id) override;

    bool remove(const std::string& id) override;

    std::vector<ObjectMetadata> list() override;

    void register_node(const std::string& address) override;

    bool unregister_node(const std::string& address) override;

    std::vector<std::string> list_nodes() override;

private:
    MDB_env* env_ = nullptr;
    MDB_dbi objects_{};
    MDB_dbi nodes_{};
    MDB_dbi meta_{};
    MDB_dbi dedup_{};

    MetadataResult commit(const MetadataCommand& cmd, std::optional<std::uint64_t> log_idx);

    MetadataResult apply_in_txn(MDB_txn* txn, const MetadataCommand& cmd);

    std::optional<ObjectMetadata> get_in_txn(MDB_txn* txn, const std::string& id);

    void put_object(MDB_txn* txn, const ObjectMetadata& metadata);

    std::uint64_t read_u64(MDB_txn* txn, const std::string& key);

    void write_u64(MDB_txn* txn, const std::string& key, std::uint64_t value);
};
