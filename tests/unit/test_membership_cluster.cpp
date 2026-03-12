#include "swarmnet/membership.hpp"

#include <cstdint>
#include <iostream>
#include <vector>

namespace {

bool expect(bool value, const char* expr, int line) {
    if (!value) {
        std::cerr << "Assertion failed at line " << line << ": " << expr << '\n';
        return false;
    }
    return true;
}

#define EXPECT_TRUE(expr) \
    do { \
        if (!expect((expr), #expr, __LINE__)) { \
            return false; \
        } \
    } while (false)

bool test_100_node_cluster_converges() {
    swarmnet::membership::ClusterConfig cfg{};
    cfg.node_count = 100;
    cfg.bootstrap_count = 3;
    cfg.gossip_fanout = 5;
    cfg.main_ticks = 260;
    cfg.stabilization_ticks = 120;
    cfg.churn_period = 12;
    cfg.churn_batch = 30;  // 30% crash churn.
    cfg.churn_down_ticks = 5;
    cfg.heartbeat_timeout_ticks = 8;
    cfg.snapshot_interval_ticks = 16;
    cfg.partition_start_tick = 40;
    cfg.partition_end_tick = 120;
    cfg.partition_minority_count = 30;
    cfg.seed = 20260311ULL;
    const auto total_ticks = cfg.main_ticks + cfg.stabilization_ticks;

    swarmnet::membership::ClusterSimulator cluster(cfg);
    cluster.run();
    const auto result = cluster.result();
    const auto states = cluster.node_states();

    EXPECT_TRUE(result.converged);
    EXPECT_TRUE(result.online_nodes == 100U);
    EXPECT_TRUE(result.committed_view_epoch > 0U);
    EXPECT_TRUE(result.safe_mode_ticks > 0U);
    EXPECT_TRUE(result.crash_events > 0U);
    EXPECT_TRUE(result.rejoin_events > 0U);
    EXPECT_TRUE(result.snapshot_sync_events > 0U);
    EXPECT_TRUE(result.state_root_drift_events == 0U);
    EXPECT_TRUE(result.committed_state_root != 0U);
    EXPECT_TRUE(result.committed_members.size() == 100U);

    for (const auto& node : states) {
        EXPECT_TRUE(node.online);
        EXPECT_TRUE(!node.safe_mode);
        EXPECT_TRUE(!node.needs_rejoin);
        EXPECT_TRUE(node.committed_epoch == result.committed_view_epoch);
        EXPECT_TRUE(node.local_applied_tick == total_ticks);
        EXPECT_TRUE(node.local_state_root == result.committed_state_root);
        EXPECT_TRUE(node.committed_members == result.committed_members);
        EXPECT_TRUE(node.known_peers.size() >= cfg.bootstrap_count);
    }
    return true;
}

}  // namespace

int main() {
    if (!test_100_node_cluster_converges()) {
        return 1;
    }
    std::cout << "membership cluster tests passed\n";
    return 0;
}
