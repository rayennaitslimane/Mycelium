#include "MetadataServer.hpp"

#include "raft/LmdbStateMgr.hpp"
#include "raft/MetadataStateMachine.hpp"

#include <chrono>
#include <filesystem>
#include <thread>
#include <utility>

using namespace nuraft;

namespace {

constexpr auto kPollInterval = std::chrono::milliseconds(10);
constexpr auto kStartTimeout = std::chrono::seconds(10);
constexpr auto kAddServerTimeout = std::chrono::seconds(10);

}  // namespace

MetadataServer::MetadataServer(MetadataServerConfig config) : config_(std::move(config)) {}

MetadataServer::~MetadataServer() {
    stop();
}

void MetadataServer::start() {
    const std::filesystem::path dir(config_.data_dir);
    store_ = std::make_unique<MetadataStore>((dir / "state").string());
    state_mgr_ = cs_new<LmdbStateMgr>(
        config_.id, config_.raft_endpoint, config_.http_endpoint, (dir / "raft").string());
    state_machine_ = cs_new<MetadataStateMachine>(*store_, dir / "snapshots");

    raft_params params;
    params.heart_beat_interval_ = 100;
    params.election_timeout_lower_bound_ = 500;
    params.election_timeout_upper_bound_ = 1000;
    // Below the election lower bound so a partitioned leader steps down first.
    params.leadership_expiry_ = 400;
    params.client_req_timeout_ = 3000;
    params.snapshot_distance_ = config_.snapshot_distance;
    params.reserved_log_items_ = config_.snapshot_distance;
    params.return_method_ = raft_params::blocking;

    // Joining nodes wait to be added instead of electing themselves.
    raft_server::init_options options;
    options.skip_initial_election_timeout_ = config_.id != 1 && !state_mgr_->has_config();

    server_ = launcher_.init(
        state_machine_, state_mgr_, nullptr, config_.raft_port,
        asio_service::options{}, params, options);
    if (!server_) {
        throw std::runtime_error("MetadataServer: failed to start raft on port " +
                                 std::to_string(config_.raft_port));
    }

    // A fresh joiner only initializes on its first heartbeat, after add_server.
    if (options.skip_initial_election_timeout_) {
        return;
    }
    const auto deadline = std::chrono::steady_clock::now() + kStartTimeout;
    while (!server_->is_initialized()) {
        if (std::chrono::steady_clock::now() > deadline) {
            throw std::runtime_error("MetadataServer: raft initialization timed out");
        }
        std::this_thread::sleep_for(kPollInterval);
    }
}

void MetadataServer::stop() {
    // The snapshot thread calls back into raft, so it must finish first.
    if (state_machine_) {
        state_machine_->shutdown();
    }
    if (server_) {
        launcher_.shutdown();
        server_.reset();
    }
    state_machine_.reset();
    state_mgr_.reset();
    store_.reset();
}

MetadataResult MetadataServer::propose(const MetadataCommand& cmd) {
    validate_command(cmd);
    if (!server_->is_leader()) {
        throw NotLeaderError(server_->get_leader());
    }

    const Bytes data = cmd.serialize();
    ptr<buffer> buf = buffer::alloc(data.size());
    buf->put_raw(data.data(), data.size());
    buf->pos(0);

    ptr<cmd_result<ptr<buffer>>> ret = server_->append_entries({buf});
    if (ret->get_result_code() == cmd_result_code::NOT_LEADER) {
        throw NotLeaderError(server_->get_leader());
    }
    if (!ret->get_accepted() || ret->get_result_code() != cmd_result_code::OK) {
        throw std::runtime_error("MetadataServer: propose failed: " + ret->get_result_str());
    }

    ptr<buffer> out = ret->get();
    if (!out) {
        throw std::runtime_error("MetadataServer: propose returned no result");
    }
    return MetadataResult::deserialize(out->data_begin(), out->size());
}

MetadataStore& MetadataServer::read() {
    // The leader's first entry of its term is committed and applied only once
    // every earlier entry is applied, so the local store is up to date.
    const bool lease_valid = server_->is_leader() &&
        server_->get_log_store()->term_at(server_->get_committed_log_idx()) == server_->get_term();
    if (!lease_valid) {
        throw NotLeaderError(server_->get_leader());
    }
    return *store_;
}

void MetadataServer::add_server(
    int id,
    const std::string& raft_endpoint,
    const std::string& http_endpoint
) {
    if (!server_->is_leader()) {
        throw NotLeaderError(server_->get_leader());
    }

    if (!server_->get_srv_config(id)) {
        ptr<cmd_result<ptr<buffer>>> ret =
            server_->add_srv(srv_config(id, 0, raft_endpoint, http_endpoint, false));
        if (ret->get_result_code() != cmd_result_code::OK &&
            ret->get_result_code() != cmd_result_code::SERVER_ALREADY_EXISTS) {
            throw std::runtime_error("MetadataServer: add_srv failed: " + ret->get_result_str());
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + kAddServerTimeout;
    while (!server_->get_srv_config(id)) {
        if (std::chrono::steady_clock::now() > deadline) {
            throw std::runtime_error("MetadataServer: timed out adding server " + std::to_string(id));
        }
        std::this_thread::sleep_for(kPollInterval);
    }
}

int MetadataServer::leader_id() const {
    return server_->get_leader();
}

std::string MetadataServer::leader_http_endpoint() const {
    const int leader = server_->get_leader();
    return leader < 0 ? std::string{} : server_->get_aux(leader);
}

bool MetadataServer::is_leader() const {
    return server_->is_leader();
}

void MetadataServer::arm_fault(const std::string& point) {
    if (point == "before_apply") {
        state_machine_->arm_fault(FaultPoint::BeforeApply);
    } else if (point == "after_apply") {
        state_machine_->arm_fault(FaultPoint::AfterApply);
    } else {
        throw std::invalid_argument("unknown fault point: " + point);
    }
}
