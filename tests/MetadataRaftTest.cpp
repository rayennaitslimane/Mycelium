#include "MetadataServer.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

constexpr int kNodes = 3;

bool wait_until(const std::function<bool()>& pred,
                std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

ObjectMetadata make_metadata(const std::string& id) {
    ObjectMetadata metadata;
    metadata.id = id;
    metadata.size = 11;
    metadata.checksum = "sha256:raft";
    metadata.erasure = ErasureSpec{2, 1, 20};
    metadata.shard_locations = {"localhost:9001/a", "localhost:9002/b", "localhost:9003/c"};
    metadata.encrypted_dek = Bytes{0xDE, 0xAD};
    metadata.kek_id = "kek";
    return metadata;
}

MetadataCommand put_cmd(const std::string& id) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::Put;
    cmd.metadata = make_metadata(id);
    return cmd;
}

class MetadataRaftTest : public ::testing::Test {
protected:
    void SetUp() override {
        static int counter = 0;
        base_port_ = 20000 + static_cast<int>(getpid() % 400) * 50 + (counter++) * kNodes;
        root_ = fs::temp_directory_path() /
            ("metadata_raft_test_" + std::to_string(getpid()) + "_" + std::to_string(base_port_));
        fs::remove_all(root_);
    }

    void TearDown() override {
        for (auto& server : servers_) {
            server.reset();
        }
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    MetadataServerConfig config(int id) const {
        MetadataServerConfig cfg;
        cfg.id = id;
        cfg.raft_port = base_port_ + id - 1;
        cfg.raft_endpoint = "127.0.0.1:" + std::to_string(cfg.raft_port);
        cfg.http_endpoint = "127.0.0.1:" + std::to_string(cfg.raft_port + 10000);
        cfg.data_dir = (root_ / ("node" + std::to_string(id))).string();
        cfg.snapshot_distance = snapshot_distance_;
        return cfg;
    }

    MetadataServer& node(int id) { return *servers_[id - 1]; }

    void start_node(int id) {
        servers_[id - 1] = std::make_unique<MetadataServer>(config(id));
        servers_[id - 1]->start();
    }

    void stop_node(int id) { servers_[id - 1].reset(); }

    void form_cluster() {
        start_node(1);
        ASSERT_TRUE(wait_until([&] { return node(1).is_leader(); }));
        for (int id = 2; id <= kNodes; ++id) {
            start_node(id);
            const auto cfg = config(id);
            node(1).add_server(id, cfg.raft_endpoint, cfg.http_endpoint);
        }
        for (int id = 1; id <= kNodes; ++id) {
            ASSERT_TRUE(wait_until([&] { return node(id).leader_id() == 1; }));
        }
    }

    // Retries while a freshly elected leader has not yet committed its term entry.
    MetadataResult propose(int id, const MetadataCommand& cmd) {
        for (int attempt = 0;; ++attempt) {
            try {
                return node(id).propose(cmd);
            } catch (const NotLeaderError&) {
                throw;
            } catch (const std::runtime_error&) {
                if (attempt >= 20) {
                    throw;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    int find_leader() {
        for (int id = 1; id <= kNodes; ++id) {
            if (servers_[id - 1] && node(id).is_leader()) {
                return id;
            }
        }
        return -1;
    }

    bool has_snapshot(int id) const {
        const fs::path dir = root_ / ("node" + std::to_string(id)) / "snapshots";
        if (!fs::exists(dir)) {
            return false;
        }
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (fs::exists(entry.path() / "snapshot.meta")) {
                return true;
            }
        }
        return false;
    }

    int base_port_ = 0;
    int snapshot_distance_ = 1000;
    fs::path root_;
    std::array<std::unique_ptr<MetadataServer>, kNodes> servers_;
};

TEST_F(MetadataRaftTest, ElectsBootstrapLeaderAndJoinsMembers) {
    form_cluster();

    EXPECT_TRUE(node(1).is_leader());
    EXPECT_FALSE(node(2).is_leader());
    EXPECT_FALSE(node(3).is_leader());
    EXPECT_EQ(config(1).http_endpoint, node(2).leader_http_endpoint());
    EXPECT_EQ(config(1).http_endpoint, node(3).leader_http_endpoint());
}

TEST_F(MetadataRaftTest, ReplicatesCommittedWritesToAllMembers) {
    form_cluster();
    const std::string id = "00000000-0000-0000-0000-00000000a001";

    EXPECT_TRUE(propose(1, put_cmd(id)).ok);

    MetadataCommand reg;
    reg.op = MetadataOp::RegisterNode;
    reg.key = "localhost:9001";
    EXPECT_TRUE(propose(1, reg).ok);

    for (int n = 1; n <= kNodes; ++n) {
        ASSERT_TRUE(wait_until([&] {
            return node(n).local_store().get(id).has_value() &&
                   !node(n).local_store().list_nodes().empty();
        })) << "node " << n;
        EXPECT_EQ(1, node(n).local_store().get(id)->version);
    }

    ASSERT_TRUE(node(1).read().get(id).has_value());
}

TEST_F(MetadataRaftTest, FollowersRejectProposeAndRead) {
    form_cluster();

    try {
        node(2).propose(put_cmd("00000000-0000-0000-0000-00000000a002"));
        FAIL() << "expected NotLeaderError";
    } catch (const NotLeaderError& e) {
        EXPECT_EQ(1, e.leader_id);
    }
    EXPECT_THROW(node(3).read(), NotLeaderError);
    EXPECT_THROW(node(1).propose(put_cmd("not-a-uuid")), std::invalid_argument);
}

TEST_F(MetadataRaftTest, FailoverElectsNewLeaderAndKeepsCommittedData) {
    form_cluster();
    const std::string before = "00000000-0000-0000-0000-00000000a003";
    const std::string after = "00000000-0000-0000-0000-00000000a004";
    ASSERT_TRUE(propose(1, put_cmd(before)).ok);

    stop_node(1);

    int leader = -1;
    ASSERT_TRUE(wait_until([&] { return (leader = find_leader()) > 0; }));
    ASSERT_NE(1, leader);

    EXPECT_TRUE(propose(leader, put_cmd(after)).ok);
    ASSERT_TRUE(wait_until([&] {
        try {
            node(leader).read();
            return true;
        } catch (const NotLeaderError&) {
            return false;
        }
    }));
    EXPECT_TRUE(node(leader).read().get(before).has_value());
    EXPECT_TRUE(node(leader).read().get(after).has_value());
}

TEST_F(MetadataRaftTest, RestartReplaysWithoutDoubleApply) {
    form_cluster();
    const std::string id = "00000000-0000-0000-0000-00000000a005";
    ASSERT_TRUE(propose(1, put_cmd(id)).ok);
    ASSERT_TRUE(wait_until([&] { return node(3).local_store().get(id).has_value(); }));
    const std::uint64_t applied = node(3).last_applied();

    stop_node(3);
    start_node(3);

    ASSERT_TRUE(wait_until([&] { return node(3).leader_id() == 1; }));
    EXPECT_EQ(applied, node(3).last_applied());
    EXPECT_EQ(1, node(3).local_store().get(id)->version);

    ASSERT_TRUE(propose(1, put_cmd(id)).ok);
    ASSERT_TRUE(wait_until([&] { return node(3).local_store().get(id)->version == 2; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(2, node(3).local_store().get(id)->version);
}

TEST_F(MetadataRaftTest, ConditionalPutIsEvaluatedInLogOrder) {
    form_cluster();
    const std::string id = "00000000-0000-0000-0000-00000000a006";
    ASSERT_TRUE(propose(1, put_cmd(id)).ok);

    MetadataCommand cas;
    cas.op = MetadataOp::ConditionalPut;
    cas.metadata = make_metadata(id);
    cas.expected_version = 1;

    EXPECT_TRUE(propose(1, cas).ok);
    EXPECT_FALSE(propose(1, cas).ok) << "stale expected_version must lose";
    EXPECT_EQ(2, node(1).read().get(id)->version);
}

TEST_F(MetadataRaftTest, LaggingFollowerCatchesUpViaSnapshot) {
    snapshot_distance_ = 5;
    form_cluster();
    const std::string id = "00000000-0000-0000-0000-00000000a007";
    stop_node(3);

    for (int i = 0; i < 30; ++i) {
        ASSERT_TRUE(propose(1, put_cmd(id)).ok);
    }
    ASSERT_TRUE(wait_until([&] { return has_snapshot(1); }));

    start_node(3);
    ASSERT_TRUE(wait_until([&] {
        const auto loaded = node(3).local_store().get(id);
        return loaded && loaded->version == 30;
    }));
    EXPECT_TRUE(has_snapshot(3));
    ASSERT_TRUE(wait_until([&] { return node(3).last_applied() == node(1).last_applied(); }));
}

TEST_F(MetadataRaftTest, RestartsFromOwnSnapshot) {
    snapshot_distance_ = 5;
    form_cluster();
    const std::string id = "00000000-0000-0000-0000-00000000a008";
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(propose(1, put_cmd(id)).ok);
    }
    ASSERT_TRUE(wait_until([&] { return has_snapshot(2); }));
    ASSERT_TRUE(wait_until([&] { return node(2).last_applied() == node(1).last_applied(); }));

    stop_node(2);
    start_node(2);
    ASSERT_TRUE(wait_until([&] { return node(2).leader_id() == 1; }));
    EXPECT_EQ(20, node(2).local_store().get(id)->version);

    ASSERT_TRUE(propose(1, put_cmd(id)).ok);
    ASSERT_TRUE(wait_until([&] { return node(2).local_store().get(id)->version == 21; }));
}

}  // namespace
