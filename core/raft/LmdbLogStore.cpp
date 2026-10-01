#include "raft/LmdbLogStore.hpp"

#include "LmdbTxn.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <limits>
#include <vector>

using namespace nuraft;

namespace {

constexpr const char* kStartIndex = "start_index";

ptr<buffer> to_buffer(const MDB_val& value) {
    ptr<buffer> buf = buffer::alloc(value.mv_size);
    buf->put_raw(static_cast<const byte*>(value.mv_data), value.mv_size);
    buf->pos(0);
    return buf;
}

ptr<log_entry> dummy_entry() {
    return cs_new<log_entry>(0, buffer::alloc(sz_ulong));
}

MDB_val as_val(const std::string& s) {
    return MDB_val{s.size(), const_cast<char*>(s.data())};
}

MDB_val as_val(std::uint64_t& index) {
    return MDB_val{sizeof(index), &index};
}

std::uint64_t key_of(const MDB_val& key) {
    std::uint64_t index = 0;
    std::memcpy(&index, key.mv_data, sizeof(index));
    return index;
}

// Deletes all log keys in [from, to].
void delete_range(MDB_txn* txn, MDB_dbi dbi, std::uint64_t from, std::uint64_t to) {
    MDB_cursor* cursor = nullptr;
    check_lmdb_rc(mdb_cursor_open(txn, dbi, &cursor), "mdb_cursor_open");
    std::vector<std::uint64_t> keys;
    MDB_val k = as_val(from);
    MDB_val value{};
    int rc = mdb_cursor_get(cursor, &k, &value, MDB_SET_RANGE);
    while (rc == 0 && key_of(k) <= to) {
        keys.push_back(key_of(k));
        rc = mdb_cursor_get(cursor, &k, &value, MDB_NEXT);
    }
    mdb_cursor_close(cursor);
    if (rc != 0 && rc != MDB_NOTFOUND) {
        check_lmdb_rc(rc, "mdb_cursor_get");
    }

    for (std::uint64_t key : keys) {
        MDB_val dk = as_val(key);
        check_lmdb_rc(mdb_del(txn, dbi, &dk, nullptr), "mdb_del(log)");
    }
}

}  // namespace

LmdbLogStore::LmdbLogStore(const std::string& dir) {
    std::filesystem::create_directories(dir);

    check_lmdb_rc(mdb_env_create(&env_), "mdb_env_create");

    try {
        check_lmdb_rc(mdb_env_set_maxdbs(env_, 2), "mdb_env_set_maxdbs");
        check_lmdb_rc(mdb_env_set_mapsize(env_, 1UL * 1024 * 1024 * 1024), "mdb_env_set_mapsize");
        check_lmdb_rc(mdb_env_open(env_, dir.c_str(), 0, 0664), "mdb_env_open");

        LmdbTxn txn(env_, 0);
        check_lmdb_rc(mdb_dbi_open(txn, "log", MDB_CREATE | MDB_INTEGERKEY, &log_), "mdb_dbi_open(log)");
        check_lmdb_rc(mdb_dbi_open(txn, "state", MDB_CREATE, &state_), "mdb_dbi_open(state)");

        std::string key = kStartIndex;
        MDB_val k = as_val(key);
        MDB_val value{};
        const int rc = mdb_get(txn, state_, &k, &value);
        if (rc == 0) {
            start_idx_ = key_of(value);
        } else if (rc != MDB_NOTFOUND) {
            check_lmdb_rc(rc, "mdb_get(start_index)");
        }

        MDB_cursor* cursor = nullptr;
        check_lmdb_rc(mdb_cursor_open(txn, log_, &cursor), "mdb_cursor_open");
        MDB_val last{};
        const int last_rc = mdb_cursor_get(cursor, &last, &value, MDB_LAST);
        mdb_cursor_close(cursor);
        next_slot_ = start_idx_;
        if (last_rc == 0) {
            next_slot_ = std::max(next_slot_, key_of(last) + 1);
        } else if (last_rc != MDB_NOTFOUND) {
            check_lmdb_rc(last_rc, "mdb_cursor_get");
        }

        txn.commit();
    } catch (...) {
        mdb_env_close(env_);
        env_ = nullptr;
        throw;
    }
}

LmdbLogStore::~LmdbLogStore() {
    if (env_ != nullptr) {
        mdb_env_close(env_);
        env_ = nullptr;
    }
}

ulong LmdbLogStore::next_slot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return next_slot_;
}

ulong LmdbLogStore::start_index() const {
    std::lock_guard<std::mutex> lock(mu_);
    return start_idx_;
}

ptr<log_entry> LmdbLogStore::last_entry() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (next_slot_ <= start_idx_) {
        return dummy_entry();
    }
    return read_entry(next_slot_ - 1);
}

ulong LmdbLogStore::append(ptr<log_entry>& entry) {
    std::lock_guard<std::mutex> lock(mu_);
    LmdbTxn txn(env_, 0);
    const std::uint64_t index = next_slot_;
    put_entry(txn, index, *entry);
    txn.commit();
    ++next_slot_;
    return index;
}

void LmdbLogStore::write_at(ulong index, ptr<log_entry>& entry) {
    std::lock_guard<std::mutex> lock(mu_);
    LmdbTxn txn(env_, 0);

    // Discard all logs at or after `index`.
    delete_range(txn, log_, index, std::numeric_limits<std::uint64_t>::max());

    put_entry(txn, index, *entry);
    txn.commit();
    next_slot_ = index + 1;
}

ptr<std::vector<ptr<log_entry>>> LmdbLogStore::log_entries(ulong start, ulong end) {
    auto result = cs_new<std::vector<ptr<log_entry>>>();
    std::lock_guard<std::mutex> lock(mu_);
    for (ulong index = start; index < end; ++index) {
        result->push_back(read_entry(index));
    }
    return result;
}

ptr<log_entry> LmdbLogStore::entry_at(ulong index) {
    std::lock_guard<std::mutex> lock(mu_);
    return read_entry(index);
}

ulong LmdbLogStore::term_at(ulong index) {
    std::lock_guard<std::mutex> lock(mu_);
    return read_entry(index)->get_term();
}

ptr<buffer> LmdbLogStore::pack(ulong index, int32 cnt) {
    std::vector<ptr<buffer>> logs;
    size_t size_total = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (ulong i = index; i < index + cnt; ++i) {
            ptr<buffer> buf = read_entry(i)->serialize();
            size_total += buf->size();
            logs.push_back(buf);
        }
    }

    ptr<buffer> out = buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + size_total);
    out->pos(0);
    out->put(static_cast<int32>(cnt));
    for (auto& buf : logs) {
        out->put(static_cast<int32>(buf->size()));
        out->put(*buf);
    }
    return out;
}

void LmdbLogStore::apply_pack(ulong index, buffer& pack) {
    pack.pos(0);
    const int32 num_logs = pack.get_int();

    std::lock_guard<std::mutex> lock(mu_);
    LmdbTxn txn(env_, 0);
    for (int32 i = 0; i < num_logs; ++i) {
        const int32 buf_size = pack.get_int();
        ptr<buffer> local = buffer::alloc(buf_size);
        pack.get(local);
        ptr<log_entry> entry = log_entry::deserialize(*local);
        put_entry(txn, index + i, *entry);
    }
    reload_bounds(txn);
    txn.commit();
}

bool LmdbLogStore::compact(ulong last_log_index) {
    std::lock_guard<std::mutex> lock(mu_);
    LmdbTxn txn(env_, 0);
    delete_range(txn, log_, 0, last_log_index);

    // Even if nothing was erased, start_idx_ must move past last_log_index.
    const std::uint64_t new_start = std::max<std::uint64_t>(start_idx_, last_log_index + 1);
    std::string key = kStartIndex;
    MDB_val sk = as_val(key);
    std::uint64_t start_copy = new_start;
    MDB_val sv = as_val(start_copy);
    check_lmdb_rc(mdb_put(txn, state_, &sk, &sv, 0), "mdb_put(start_index)");
    txn.commit();

    start_idx_ = new_start;
    next_slot_ = std::max(next_slot_, start_idx_);
    return true;
}

void LmdbLogStore::save_state_blob(const std::string& key, const buffer& value) {
    std::lock_guard<std::mutex> lock(mu_);
    LmdbTxn txn(env_, 0);
    MDB_val k = as_val(key);
    MDB_val v{value.size(), value.data_begin()};
    check_lmdb_rc(mdb_put(txn, state_, &k, &v, 0), "mdb_put(state)");
    txn.commit();
}

ptr<buffer> LmdbLogStore::load_state_blob(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mu_);
    LmdbTxn txn(env_, MDB_RDONLY);
    MDB_val k = as_val(key);
    MDB_val value{};
    const int rc = mdb_get(txn, state_, &k, &value);
    if (rc == MDB_NOTFOUND) {
        return nullptr;
    }
    check_lmdb_rc(rc, "mdb_get(state)");
    return to_buffer(value);
}

ptr<log_entry> LmdbLogStore::read_entry(std::uint64_t index) const {
    LmdbTxn txn(env_, MDB_RDONLY);
    MDB_val k = as_val(index);
    MDB_val value{};
    const int rc = mdb_get(txn, log_, &k, &value);
    if (rc == MDB_NOTFOUND) {
        return dummy_entry();
    }
    check_lmdb_rc(rc, "mdb_get(log)");
    ptr<buffer> buf = to_buffer(value);
    return log_entry::deserialize(*buf);
}

void LmdbLogStore::put_entry(MDB_txn* txn, std::uint64_t index, log_entry& entry) {
    ptr<buffer> buf = entry.serialize();
    MDB_val k = as_val(index);
    MDB_val v{buf->size(), buf->data_begin()};
    check_lmdb_rc(mdb_put(txn, log_, &k, &v, 0), "mdb_put(log)");
}

void LmdbLogStore::reload_bounds(MDB_txn* txn) {
    MDB_cursor* cursor = nullptr;
    check_lmdb_rc(mdb_cursor_open(txn, log_, &cursor), "mdb_cursor_open");
    MDB_val first{};
    MDB_val last{};
    MDB_val value{};
    const int first_rc = mdb_cursor_get(cursor, &first, &value, MDB_FIRST);
    const int last_rc = mdb_cursor_get(cursor, &last, &value, MDB_LAST);
    mdb_cursor_close(cursor);

    start_idx_ = first_rc == 0 ? key_of(first) : 1;
    next_slot_ = last_rc == 0 ? key_of(last) + 1 : start_idx_;

    std::string key = kStartIndex;
    MDB_val sk = as_val(key);
    std::uint64_t start_copy = start_idx_;
    MDB_val sv = as_val(start_copy);
    check_lmdb_rc(mdb_put(txn, state_, &sk, &sv, 0), "mdb_put(start_index)");
}
