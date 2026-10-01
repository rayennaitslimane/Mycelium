#pragma once

#include <libnuraft/nuraft.hxx>
#include <lmdb.h>

#include <cstdint>
#include <mutex>
#include <string>

// Raft log persisted in its own LMDB env (DBIs `log` and `state`).
// The `state` DBI also stores LmdbStateMgr's cluster config and server state.
class LmdbLogStore : public nuraft::log_store {
public:
    explicit LmdbLogStore(const std::string& dir);

    ~LmdbLogStore() override;

    LmdbLogStore(const LmdbLogStore&) = delete;
    LmdbLogStore& operator=(const LmdbLogStore&) = delete;

    nuraft::ulong next_slot() const override;

    nuraft::ulong start_index() const override;

    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    nuraft::ulong append(nuraft::ptr<nuraft::log_entry>& entry) override;

    void write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
    log_entries(nuraft::ulong start, nuraft::ulong end) override;

    nuraft::ptr<nuraft::log_entry> entry_at(nuraft::ulong index) override;

    nuraft::ulong term_at(nuraft::ulong index) override;

    nuraft::ptr<nuraft::buffer> pack(nuraft::ulong index, nuraft::int32 cnt) override;

    void apply_pack(nuraft::ulong index, nuraft::buffer& pack) override;

    bool compact(nuraft::ulong last_log_index) override;

    bool flush() override { return true; }

    void save_state_blob(const std::string& key, const nuraft::buffer& value);

    // Returns nullptr when the key is absent.
    nuraft::ptr<nuraft::buffer> load_state_blob(const std::string& key) const;

private:
    MDB_env* env_ = nullptr;
    MDB_dbi log_{};
    MDB_dbi state_{};
    mutable std::mutex mu_;
    std::uint64_t start_idx_ = 1;
    std::uint64_t next_slot_ = 1;

    nuraft::ptr<nuraft::log_entry> read_entry(std::uint64_t index) const;

    void put_entry(MDB_txn* txn, std::uint64_t index, nuraft::log_entry& entry);

    // Re-derives start_idx_/next_slot_ from the stored keys and persists start_idx_.
    void reload_bounds(MDB_txn* txn);
};
