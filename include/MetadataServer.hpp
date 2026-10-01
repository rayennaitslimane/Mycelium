#pragma once

#include "MetadataCommand.hpp"
#include "MetadataStore.hpp"

#include <libnuraft/nuraft.hxx>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

class LmdbStateMgr;
class MetadataStateMachine;

struct MetadataServerConfig {
    int id = 1;
    int raft_port = 0;
    std::string raft_endpoint;  // host:port peers use to reach raft_port
    std::string http_endpoint;  // advertised to clients via srv_config aux
    std::string data_dir;       // holds <data_dir>/state, <data_dir>/raft and <data_dir>/snapshots
    int snapshot_distance = 1000;  // also the number of log entries kept after compaction
};

class NotLeaderError : public std::runtime_error {
public:
    explicit NotLeaderError(int leader)
        : std::runtime_error("not the raft leader (leader id " + std::to_string(leader) + ")"),
          leader_id(leader) {}

    int leader_id;  // -1 when unknown
};

// One Raft member replicating MetadataCommands into a local LMDB MetadataStore.
class MetadataServer {
public:
    explicit MetadataServer(MetadataServerConfig config);

    ~MetadataServer();

    MetadataServer(const MetadataServer&) = delete;
    MetadataServer& operator=(const MetadataServer&) = delete;

    void start();

    void stop();

    // Replicates cmd and returns its apply result. Throws NotLeaderError on followers.
    MetadataResult propose(const MetadataCommand& cmd);

    // Returns the local store for a linearizable read; throws NotLeaderError
    // unless this node holds a valid leader lease.
    MetadataStore& read();

    // Local state without any consistency guarantee (tests/diagnostics).
    MetadataStore& local_store() { return *store_; }

    // Adds a member (leader only); returns once it appears in the config.
    void add_server(int id, const std::string& raft_endpoint, const std::string& http_endpoint);

    int leader_id() const;

    int id() const { return config_.id; }

    // Arms a one-shot crash point ("before_apply" or "after_apply") in the state machine.
    void arm_fault(const std::string& point);

    std::string leader_http_endpoint() const;

    bool is_leader() const;

    std::uint64_t last_applied() { return store_->last_applied(); }

private:
    MetadataServerConfig config_;
    std::unique_ptr<MetadataStore> store_;
    nuraft::ptr<LmdbStateMgr> state_mgr_;
    nuraft::ptr<MetadataStateMachine> state_machine_;
    nuraft::raft_launcher launcher_;
    nuraft::ptr<nuraft::raft_server> server_;
};
