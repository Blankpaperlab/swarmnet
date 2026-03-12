#pragma once

#include <cstdint>
#include <vector>

namespace swarmnet::membership {

using NodeId = std::uint32_t;

struct ClusterConfig {
    std::uint32_t node_count = 100;
    std::uint32_t bootstrap_count = 3;
    std::uint32_t gossip_fanout = 4;
    std::uint64_t main_ticks = 200;
    std::uint64_t stabilization_ticks = 60;

    std::uint64_t churn_period = 24;
    std::uint32_t churn_batch = 4;
    std::uint64_t churn_down_ticks = 6;
    std::uint64_t heartbeat_timeout_ticks = 8;
    std::uint64_t snapshot_interval_ticks = 16;

    std::uint64_t partition_start_tick = 40;
    std::uint64_t partition_end_tick = 100;
    std::uint32_t partition_minority_count = 30;
    std::uint64_t seed = 20260311ULL;
};

struct NodeState {
    NodeId id = 0;
    bool online = false;
    bool safe_mode = false;
    bool needs_rejoin = false;
    std::uint64_t committed_epoch = 0;
    std::uint64_t local_applied_tick = 0;
    std::uint64_t local_state_counter = 0;
    std::uint64_t local_state_root = 0;
    std::vector<NodeId> committed_members;
    std::vector<NodeId> known_peers;
};

struct ClusterResult {
    bool converged = false;
    std::uint64_t committed_view_epoch = 0;
    std::vector<NodeId> committed_members;
    std::uint64_t committed_state_root = 0;
    std::uint32_t online_nodes = 0;
    std::uint32_t safe_mode_nodes = 0;
    std::uint64_t safe_mode_ticks = 0;
    std::uint64_t crash_events = 0;
    std::uint64_t rejoin_events = 0;
    std::uint64_t snapshot_sync_events = 0;
    std::uint64_t state_root_drift_events = 0;
};

class ClusterSimulator {
public:
    explicit ClusterSimulator(ClusterConfig config);
    ~ClusterSimulator();

    ClusterSimulator(const ClusterSimulator&) = delete;
    ClusterSimulator& operator=(const ClusterSimulator&) = delete;

    void run();
    [[nodiscard]] const ClusterResult& result() const noexcept;
    [[nodiscard]] std::vector<NodeState> node_states() const;

private:
    struct Impl;
    Impl* impl_;
};

}  // namespace swarmnet::membership
