#include "raft/LmdbStateMgr.hpp"

#include <cstdlib>
#include <iostream>

using namespace nuraft;

namespace {

constexpr const char* kConfig = "config";
constexpr const char* kServerState = "srv_state";

}  // namespace

LmdbStateMgr::LmdbStateMgr(
    int id,
    const std::string& raft_endpoint,
    const std::string& http_endpoint,
    const std::string& dir
)
    : id_(id),
      raft_endpoint_(raft_endpoint),
      http_endpoint_(http_endpoint),
      log_store_(cs_new<LmdbLogStore>(dir)) {}

ptr<cluster_config> LmdbStateMgr::load_config() {
    if (ptr<buffer> buf = log_store_->load_state_blob(kConfig)) {
        return cluster_config::deserialize(*buf);
    }
    auto config = cs_new<cluster_config>();
    config->get_servers().push_back(
        cs_new<srv_config>(id_, 0, raft_endpoint_, http_endpoint_, false));
    return config;
}

void LmdbStateMgr::save_config(const cluster_config& config) {
    log_store_->save_state_blob(kConfig, *config.serialize());
}

void LmdbStateMgr::save_state(const srv_state& state) {
    log_store_->save_state_blob(kServerState, *state.serialize());
}

ptr<srv_state> LmdbStateMgr::read_state() {
    if (ptr<buffer> buf = log_store_->load_state_blob(kServerState)) {
        return srv_state::deserialize(*buf);
    }
    return nullptr;
}

void LmdbStateMgr::system_exit(const int exit_code) {
    std::cerr << "raft: fatal error, exit code " << exit_code << "\n";
    std::abort();
}

bool LmdbStateMgr::has_config() const {
    return log_store_->load_state_blob(kConfig) != nullptr;
}
