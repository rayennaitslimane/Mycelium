#pragma once

#include "raft/LmdbLogStore.hpp"

#include <libnuraft/nuraft.hxx>

#include <string>

// Persists cluster config and server state in the raft env's `state` DBI.
// Without a saved config, the node starts with a self-only config.
class LmdbStateMgr : public nuraft::state_mgr {
public:
    LmdbStateMgr(
        int id,
        const std::string& raft_endpoint,
        const std::string& http_endpoint,
        const std::string& dir
    );

    nuraft::ptr<nuraft::cluster_config> load_config() override;

    void save_config(const nuraft::cluster_config& config) override;

    void save_state(const nuraft::srv_state& state) override;

    nuraft::ptr<nuraft::srv_state> read_state() override;

    nuraft::ptr<nuraft::log_store> load_log_store() override { return log_store_; }

    nuraft::int32 server_id() override { return id_; }

    void system_exit(const int exit_code) override;

    bool has_config() const;

private:
    int id_;
    std::string raft_endpoint_;
    std::string http_endpoint_;
    nuraft::ptr<LmdbLogStore> log_store_;
};
