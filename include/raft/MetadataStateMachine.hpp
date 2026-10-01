#pragma once

#include "MetadataStore.hpp"

#include <libnuraft/nuraft.hxx>

#include <atomic>
#include <filesystem>
#include <mutex>
#include <thread>

enum class FaultPoint { None, BeforeApply, AfterApply };

// Applies committed MetadataCommands to the LMDB MetadataStore.
// Logical snapshots live in <snapshot_dir>/<idx>/chunk_<n> + snapshot.meta;
// snapshot::size() holds the chunk count.
class MetadataStateMachine : public nuraft::state_machine {
public:
    MetadataStateMachine(MetadataStore& store, const std::filesystem::path& snapshot_dir);

    ~MetadataStateMachine() override;

    nuraft::ptr<nuraft::buffer> commit(const nuraft::ulong log_idx, nuraft::buffer& data) override;

    nuraft::ulong last_commit_index() override { return store_.last_applied(); }

    void create_snapshot(
        nuraft::snapshot& s,
        nuraft::async_result<bool>::handler_type& when_done
    ) override;

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    int read_logical_snp_obj(
        nuraft::snapshot& s,
        void*& user_snp_ctx,
        nuraft::ulong obj_id,
        nuraft::ptr<nuraft::buffer>& data_out,
        bool& is_last_obj
    ) override;

    void save_logical_snp_obj(
        nuraft::snapshot& s,
        nuraft::ulong& obj_id,
        nuraft::buffer& data,
        bool is_first_obj,
        bool is_last_obj
    ) override;

    bool apply_snapshot(nuraft::snapshot& s) override;

    // Crashes the process (std::_Exit) at the given point of the next commit.
    void arm_fault(FaultPoint point) { fault_.store(point); }

    // Waits for an in-flight snapshot and refuses new ones; call before raft shutdown.
    void shutdown();

private:
    MetadataStore& store_;
    std::filesystem::path snapshot_dir_;
    std::atomic<FaultPoint> fault_{FaultPoint::None};

    std::mutex snapshot_mu_;
    nuraft::ptr<nuraft::snapshot> last_snapshot_;

    std::mutex thread_mu_;
    std::thread snapshot_thread_;
    bool stopping_ = false;

    std::filesystem::path snapshot_path(nuraft::ulong idx, const char* suffix = "") const;

    // Publishes snp as the latest snapshot and deletes older snapshot dirs.
    void set_last_snapshot(const nuraft::ptr<nuraft::snapshot>& snp);
};
