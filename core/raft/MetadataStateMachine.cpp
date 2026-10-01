#include "raft/MetadataStateMachine.hpp"

#include "LmdbTxn.hpp"

#include <cstdlib>
#include <fstream>
#include <iterator>
#include <string>

using namespace nuraft;
namespace fs = std::filesystem;

namespace {

constexpr int kFaultExitCode = 86;
constexpr const char* kMetaFile = "snapshot.meta";

void write_file(const fs::path& path, const char* data, std::size_t size) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(data, static_cast<std::streamsize>(size));
    if (!out) {
        throw std::runtime_error("failed to write " + path.string());
    }
}

ptr<buffer> read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return nullptr;
    }
    const std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ptr<buffer> buf = buffer::alloc(data.size());
    buf->put_raw(reinterpret_cast<const byte*>(data.data()), data.size());
    buf->pos(0);
    return buf;
}

void write_meta(const fs::path& dir, snapshot& snp) {
    ptr<buffer> meta = snp.serialize();
    meta->pos(0);
    write_file(dir / kMetaFile, reinterpret_cast<const char*>(meta->data_begin()), meta->size());
}

}  // namespace

MetadataStateMachine::MetadataStateMachine(MetadataStore& store, const fs::path& snapshot_dir)
    : store_(store), snapshot_dir_(snapshot_dir) {
    fs::create_directories(snapshot_dir_);

    // Keep the newest complete snapshot; drop partial (.tmp/.recv) and older ones.
    ptr<snapshot> newest;
    for (const auto& entry : fs::directory_iterator(snapshot_dir_)) {
        const std::string name = entry.path().filename().string();
        if (name.find_first_not_of("0123456789") != std::string::npos) {
            fs::remove_all(entry.path());
            continue;
        }
        if (ptr<buffer> meta = read_file(entry.path() / kMetaFile)) {
            ptr<snapshot> snp = snapshot::deserialize(*meta);
            if (!newest || snp->get_last_log_idx() > newest->get_last_log_idx()) {
                newest = snp;
            }
        }
    }
    if (newest) {
        set_last_snapshot(newest);
    }
}

MetadataStateMachine::~MetadataStateMachine() {
    shutdown();
}

ptr<buffer> MetadataStateMachine::commit(const ulong log_idx, buffer& data) {
    const MetadataCommand cmd = MetadataCommand::deserialize(data.data_begin(), data.size());

    const FaultPoint fault = fault_.exchange(FaultPoint::None);
    if (fault == FaultPoint::BeforeApply) {
        std::_Exit(kFaultExitCode);
    }

    const Bytes result = store_.apply(cmd, log_idx).serialize();

    if (fault == FaultPoint::AfterApply) {
        std::_Exit(kFaultExitCode);
    }

    ptr<buffer> out = buffer::alloc(result.size());
    out->put_raw(result.data(), result.size());
    out->pos(0);
    return out;
}

void MetadataStateMachine::create_snapshot(
    snapshot& s,
    async_result<bool>::handler_type& when_done
) {
    std::lock_guard<std::mutex> lock(thread_mu_);
    if (stopping_) {
        bool ok = false;
        ptr<std::exception> err = nullptr;
        when_done(ok, err);
        return;
    }
    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }

    ptr<buffer> meta = s.serialize();
    meta->pos(0);
    ptr<snapshot> snp = snapshot::deserialize(*meta);

    // Called on the commit thread right after log_idx was applied, so this
    // read txn sees exactly the state at the snapshot index.
    std::shared_ptr<LmdbTxn> txn = store_.begin_snapshot_read();

    snapshot_thread_ = std::thread([this, snp, txn, when_done]() mutable {
        bool ok = true;
        ptr<std::exception> err = nullptr;
        try {
            const fs::path tmp = snapshot_path(snp->get_last_log_idx(), ".tmp");
            fs::remove_all(tmp);
            fs::create_directories(tmp);
            snp->set_size(store_.write_snapshot_chunks(*txn, tmp.string()));
            txn.reset();
            write_meta(tmp, *snp);

            const fs::path final_dir = snapshot_path(snp->get_last_log_idx());
            fs::remove_all(final_dir);
            fs::rename(tmp, final_dir);
            set_last_snapshot(snp);
        } catch (const std::exception& e) {
            ok = false;
            err = cs_new<std::runtime_error>(e.what());
        }
        when_done(ok, err);
    });
}

ptr<snapshot> MetadataStateMachine::last_snapshot() {
    std::lock_guard<std::mutex> lock(snapshot_mu_);
    return last_snapshot_;
}

int MetadataStateMachine::read_logical_snp_obj(
    snapshot& s,
    void*& /*user_snp_ctx*/,
    ulong obj_id,
    ptr<buffer>& data_out,
    bool& is_last_obj
) {
    // A missing chunk means a newer snapshot replaced this one; NuRaft retries with it.
    data_out = read_file(snapshot_path(s.get_last_log_idx()) / ("chunk_" + std::to_string(obj_id)));
    if (!data_out) {
        return -1;
    }
    is_last_obj = obj_id + 1 >= s.size();
    return 0;
}

void MetadataStateMachine::save_logical_snp_obj(
    snapshot& s,
    ulong& obj_id,
    buffer& data,
    bool is_first_obj,
    bool /*is_last_obj*/
) {
    const fs::path recv = snapshot_path(s.get_last_log_idx(), ".recv");
    if (is_first_obj) {
        fs::remove_all(recv);
        fs::create_directories(recv);
    }
    data.pos(0);
    write_file(
        recv / ("chunk_" + std::to_string(obj_id)),
        reinterpret_cast<const char*>(data.data_begin()),
        data.size());
    ++obj_id;
}

bool MetadataStateMachine::apply_snapshot(snapshot& s) {
    {
        std::lock_guard<std::mutex> lock(thread_mu_);
        if (snapshot_thread_.joinable()) {
            snapshot_thread_.join();
        }
    }

    try {
        const fs::path recv = snapshot_path(s.get_last_log_idx(), ".recv");
        store_.restore_snapshot(recv.string(), s.size(), s.get_last_log_idx());

        write_meta(recv, s);
        const fs::path final_dir = snapshot_path(s.get_last_log_idx());
        fs::remove_all(final_dir);
        fs::rename(recv, final_dir);

        ptr<buffer> meta = s.serialize();
        meta->pos(0);
        set_last_snapshot(snapshot::deserialize(*meta));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

void MetadataStateMachine::shutdown() {
    std::lock_guard<std::mutex> lock(thread_mu_);
    stopping_ = true;
    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }
}

fs::path MetadataStateMachine::snapshot_path(ulong idx, const char* suffix) const {
    return snapshot_dir_ / (std::to_string(idx) + suffix);
}

void MetadataStateMachine::set_last_snapshot(const ptr<snapshot>& snp) {
    std::lock_guard<std::mutex> lock(snapshot_mu_);
    last_snapshot_ = snp;

    const std::string keep = std::to_string(snp->get_last_log_idx());
    for (const auto& entry : fs::directory_iterator(snapshot_dir_)) {
        const std::string name = entry.path().filename().string();
        const bool numeric = name.find_first_not_of("0123456789") == std::string::npos;
        if (numeric && name != keep && std::stoull(name) < snp->get_last_log_idx()) {
            std::error_code ec;
            fs::remove_all(entry.path(), ec);
        }
    }
}
