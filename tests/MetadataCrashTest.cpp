#include "MetadataClient.hpp"
#include "MetadataStore.hpp"

#include <gtest/gtest.h>
#include <httplib.h>
#include <nlohmann/json.hpp>

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <thread>

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

constexpr int kNodes = 3;
constexpr int kFaultExitCode = 86;

bool wait_until(const std::function<bool()>& pred,
                std::chrono::milliseconds timeout = std::chrono::seconds(15)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return pred();
}

ObjectMetadata make_metadata(const std::string& id) {
    ObjectMetadata metadata;
    metadata.id = id;
    metadata.size = 11;
    metadata.checksum = "sha256:crash";
    metadata.erasure = ErasureSpec{2, 1, 20};
    metadata.shard_locations = {"localhost:9001/a", "localhost:9002/b", "localhost:9003/c"};
    metadata.encrypted_dek = Bytes{0xDE, 0xAD};
    metadata.kek_id = "kek";
    return metadata;
}

// Runs real metadata_server processes so a fault point can kill one mid-commit.
class MetadataCrashTest : public ::testing::Test {
protected:
    void SetUp() override {
        static int counter = 0;
        base_port_ = 30000 + static_cast<int>(getpid() % 300) * 60 + (counter++) * 2 * kNodes;
        root_ = fs::temp_directory_path() /
            ("metadata_crash_test_" + std::to_string(getpid()) + "_" + std::to_string(base_port_));
        fs::remove_all(root_);
        fs::create_directories(root_);
    }

    void TearDown() override {
        kill_all();
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    std::string raft_endpoint(int id) const {
        return "127.0.0.1:" + std::to_string(base_port_ + id - 1);
    }

    std::string http_endpoint(int id) const {
        return "127.0.0.1:" + std::to_string(base_port_ + kNodes + id - 1);
    }

    fs::path data_dir(int id) const { return root_ / ("node" + std::to_string(id)); }

    void spawn(int id) {
        const std::string id_arg = std::to_string(id);
        const std::string raft = raft_endpoint(id);
        const std::string http = http_endpoint(id);
        const std::string dir = data_dir(id).string();
        const std::string log = (root_ / ("node" + id_arg + ".log")).string();

        const pid_t pid = fork();
        ASSERT_GE(pid, 0);
        if (pid == 0) {
            setenv("MYC_ENABLE_FAULT_INJECTION", "1", 1);
            const int fd = open(log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);
            execl(METADATA_SERVER_BIN, METADATA_SERVER_BIN,
                  id_arg.c_str(), raft.c_str(), http.c_str(), dir.c_str(), nullptr);
            _exit(127);
        }
        pids_[id - 1] = pid;
    }

    void kill_all() {
        for (auto& pid : pids_) {
            if (pid > 0) {
                kill(pid, SIGKILL);
                waitpid(pid, nullptr, 0);
                pid = -1;
            }
        }
    }

    // Returns the exit code once the process exits, or nullopt on timeout.
    std::optional<int> wait_exit(int id) {
        int wstatus = 0;
        const bool exited = wait_until([&] {
            return waitpid(pids_[id - 1], &wstatus, WNOHANG) == pids_[id - 1];
        });
        if (!exited) {
            return std::nullopt;
        }
        pids_[id - 1] = -1;
        return WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : -1;
    }

    std::optional<json> status(int id) {
        httplib::Client http("http://" + http_endpoint(id));
        http.set_connection_timeout(std::chrono::milliseconds(200));
        const auto res = http.Get("/status");
        if (!res || res->status != 200) {
            return std::nullopt;
        }
        return json::parse(res->body);
    }

    bool post(int id, const std::string& path, const json& body) {
        httplib::Client http("http://" + http_endpoint(id));
        const auto res = http.Post(path, body.dump(), "application/json");
        return res && res->status == 200;
    }

    void form_cluster() {
        spawn(1);
        ASSERT_TRUE(wait_until([&] {
            const auto s = status(1);
            return s && s->at("is_leader").get<bool>();
        }));
        for (int id = 2; id <= kNodes; ++id) {
            spawn(id);
            ASSERT_TRUE(wait_until([&] { return status(id).has_value(); }));
            ASSERT_TRUE(post(1, "/admin/servers",
                             {{"id", id},
                              {"raft_endpoint", raft_endpoint(id)},
                              {"http_endpoint", http_endpoint(id)}}));
        }
        for (int id = 1; id <= kNodes; ++id) {
            ASSERT_TRUE(wait_until([&] {
                const auto s = status(id);
                return s && s->at("leader_id").get<int>() == 1;
            }));
        }
    }

    MetadataClient client() {
        std::vector<std::string> endpoints;
        for (int id = 1; id <= kNodes; ++id) {
            endpoints.push_back(http_endpoint(id));
        }
        return MetadataClient(endpoints);
    }

    bool converged() {
        std::optional<std::uint64_t> applied;
        for (int id = 1; id <= kNodes; ++id) {
            const auto s = status(id);
            if (!s) {
                return false;
            }
            const auto value = s->at("last_applied").get<std::uint64_t>();
            if (applied && *applied != value) {
                return false;
            }
            applied = value;
        }
        return true;
    }

    // Kills every server, then checks each on-disk state machine directly.
    void expect_stores_agree(const std::string& object_id, int version) {
        kill_all();
        std::optional<std::uint64_t> applied;
        for (int id = 1; id <= kNodes; ++id) {
            MetadataStore store((data_dir(id) / "state").string());
            const auto loaded = store.get(object_id);
            ASSERT_TRUE(loaded.has_value()) << "node " << id;
            EXPECT_EQ(version, loaded->version) << "node " << id;
            if (applied) {
                EXPECT_EQ(*applied, store.last_applied()) << "node " << id;
            }
            applied = store.last_applied();
        }
    }

    int base_port_ = 0;
    fs::path root_;
    std::array<pid_t, kNodes> pids_{-1, -1, -1};
};

TEST_F(MetadataCrashTest, FollowerCrashBeforeApplyReplaysOnRestart) {
    form_cluster();
    const std::string id = "00000000-0000-0000-0000-00000000c001";

    ASSERT_TRUE(post(3, "/admin/fault", {{"point", "before_apply"}}));
    auto metadata = client();
    metadata.put(make_metadata(id));

    EXPECT_EQ(kFaultExitCode, wait_exit(3));
    spawn(3);
    ASSERT_TRUE(wait_until([&] { return converged(); }));

    expect_stores_agree(id, 1);
}

TEST_F(MetadataCrashTest, LeaderCrashAfterApplyKeepsSingleApply) {
    form_cluster();
    const std::string id = "00000000-0000-0000-0000-00000000c002";

    ASSERT_TRUE(post(1, "/admin/fault", {{"point", "after_apply"}}));
    auto metadata = client();
    // Leader dies before replying; the client retries with the same request_id.
    metadata.put(make_metadata(id));
    EXPECT_EQ(kFaultExitCode, wait_exit(1));
    EXPECT_EQ(1, metadata.get(id)->version);

    metadata.put(make_metadata(id));
    spawn(1);
    ASSERT_TRUE(wait_until([&] { return converged(); }));

    expect_stores_agree(id, 2);
}

}  // namespace
