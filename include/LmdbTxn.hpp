#pragma once

#include <lmdb.h>

#include <stdexcept>
#include <string>
#include <utility>

inline void check_lmdb_rc(int rc, const char* operation) {
    if (rc != 0) {
        throw std::runtime_error(std::string(operation) + " failed: " + mdb_strerror(rc));
    }
}

// Aborts the transaction unless commit() was called.
class LmdbTxn {
public:
    LmdbTxn(MDB_env* env, unsigned int flags) {
        check_lmdb_rc(mdb_txn_begin(env, nullptr, flags, &txn_), "mdb_txn_begin");
    }

    ~LmdbTxn() {
        if (txn_ != nullptr) {
            mdb_txn_abort(txn_);
        }
    }

    LmdbTxn(const LmdbTxn&) = delete;
    LmdbTxn& operator=(const LmdbTxn&) = delete;

    void commit() {
        MDB_txn* txn = std::exchange(txn_, nullptr);
        check_lmdb_rc(mdb_txn_commit(txn), "mdb_txn_commit");
    }

    operator MDB_txn*() const { return txn_; }

private:
    MDB_txn* txn_ = nullptr;
};
