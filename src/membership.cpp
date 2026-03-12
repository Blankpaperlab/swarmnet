#include "swarmnet/membership.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace swarmnet::membership {

namespace {

struct ViewCert {
    std::uint64_t epoch = 0;
    std::uint64_t effective_tick = 0;
    std::vector<NodeId> members;
    std::uint64_t proposal_hash = 0;
    std::vector<NodeId> signers;
};

struct TickCommit {
    std::uint64_t tick = 0;
    std::uint64_t delta = 0;
    std::uint64_t state_counter = 0;
    std::uint64_t state_root = 0;
};

struct NodeRuntime {
    NodeId id = 0;
    bool online = true;
    bool safe_mode = false;
    bool needs_rejoin = false;
    bool partition_blocked = false;
    std::uint64_t offline_until_tick = 0;
    std::uint64_t committed_epoch = 0;
    std::uint64_t local_applied_tick = 0;
    std::uint64_t local_state_counter = 0;
    std::uint64_t local_state_root = 0;
    std::vector<NodeId> committed_members;
    std::vector<NodeId> known_peers;
    std::vector<NodeId> bootstraps;
    std::vector<std::uint64_t> last_seen_tick;
    std::optional<ViewCert> pending_cert;
};

struct Proposal {
    std::uint64_t target_epoch = 0;
    std::uint64_t effective_tick = 0;
    NodeId proposer = 0;
    std::vector<NodeId> members;
    std::uint64_t hash = 0;
};

struct GossipMessage {
    NodeId from = 0;
    NodeId to = 0;
    std::vector<NodeId> peers;
    std::uint64_t committed_epoch = 0;
    std::vector<NodeId> committed_members;
    std::uint64_t state_tick = 0;
    std::uint64_t state_root = 0;
};

void add_unique_sorted(std::vector<NodeId>& values, NodeId value) {
    const auto it = std::lower_bound(values.begin(), values.end(), value);
    if (it == values.end() || *it != value) {
        values.insert(it, value);
    }
}

void merge_unique_sorted(std::vector<NodeId>& values, const std::vector<NodeId>& incoming) {
    for (const auto value : incoming) {
        add_unique_sorted(values, value);
    }
}

bool contains_sorted(const std::vector<NodeId>& values, NodeId value) {
    return std::binary_search(values.begin(), values.end(), value);
}

std::uint64_t mix64(std::uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31U);
}

std::uint64_t hash_view(std::uint64_t target_epoch,
                        std::uint64_t effective_tick,
                        const std::vector<NodeId>& members) {
    std::uint64_t h = 1469598103934665603ULL;
    h = mix64(h ^ target_epoch);
    h = mix64(h ^ effective_tick);
    for (const auto member : members) {
        h = mix64(h ^ static_cast<std::uint64_t>(member));
    }
    return h;
}

std::uint64_t hash_state_root(std::uint64_t tick, std::uint64_t state_counter) {
    std::uint64_t h = 1469598103934665603ULL;
    h = mix64(h ^ tick);
    h = mix64(h ^ state_counter);
    return h;
}

}  // namespace

struct ClusterSimulator::Impl {
    explicit Impl(ClusterConfig in_config) : config(std::move(in_config)) {
        if (config.node_count == 0U) {
            throw std::runtime_error("node_count must be > 0");
        }
        if (config.bootstrap_count == 0U || config.bootstrap_count > config.node_count) {
            throw std::runtime_error("bootstrap_count must be in [1, node_count]");
        }
        if (config.gossip_fanout == 0U) {
            throw std::runtime_error("gossip_fanout must be > 0");
        }
        if (config.partition_minority_count >= config.node_count) {
            throw std::runtime_error("partition_minority_count must be < node_count");
        }
        if (config.heartbeat_timeout_ticks == 0U) {
            throw std::runtime_error("heartbeat_timeout_ticks must be > 0");
        }
        if (config.snapshot_interval_ticks == 0U) {
            throw std::runtime_error("snapshot_interval_ticks must be > 0");
        }

        bootstrap_ids.reserve(config.bootstrap_count);
        for (NodeId i = 1; i <= config.bootstrap_count; ++i) {
            bootstrap_ids.push_back(i);
        }

        commits.push_back(TickCommit{
            .tick = 0,
            .delta = 0,
            .state_counter = 0,
            .state_root = hash_state_root(0, 0),
        });

        nodes.reserve(config.node_count);
        for (NodeId i = 1; i <= config.node_count; ++i) {
            NodeRuntime node{};
            node.id = i;
            node.committed_members = bootstrap_ids;
            node.bootstraps = bootstrap_ids;
            node.local_applied_tick = 0;
            node.local_state_counter = 0;
            node.local_state_root = commits.front().state_root;
            node.last_seen_tick.assign(static_cast<std::size_t>(config.node_count) + 1U, 0U);
            add_unique_sorted(node.known_peers, i);
            merge_unique_sorted(node.known_peers, bootstrap_ids);
            node.last_seen_tick[static_cast<std::size_t>(i)] = 1U;
            for (const auto bootstrap_id : bootstrap_ids) {
                node.last_seen_tick[static_cast<std::size_t>(bootstrap_id)] = 1U;
            }
            nodes.push_back(std::move(node));
        }
    }

    [[nodiscard]] bool partition_active(std::uint64_t tick) const noexcept {
        if (tick > config.main_ticks) {
            return false;
        }
        return tick >= config.partition_start_tick && tick < config.partition_end_tick;
    }

    [[nodiscard]] bool in_minority(NodeId id, std::uint64_t tick) const noexcept {
        if (!partition_active(tick)) {
            return false;
        }
        return id <= config.partition_minority_count;
    }

    [[nodiscard]] bool can_route(NodeId from, NodeId to, std::uint64_t tick) const noexcept {
        const auto& a = nodes[static_cast<std::size_t>(from - 1U)];
        const auto& b = nodes[static_cast<std::size_t>(to - 1U)];
        if (!a.online || !b.online) {
            return false;
        }
        if (!partition_active(tick)) {
            return true;
        }
        return in_minority(from, tick) == in_minority(to, tick);
    }

    [[nodiscard]] bool is_recently_seen(const NodeRuntime& node, NodeId peer, std::uint64_t tick) const noexcept {
        if (peer == node.id) {
            return true;
        }
        if (peer == 0U || peer > config.node_count) {
            return false;
        }

        const auto& candidate = nodes[static_cast<std::size_t>(peer - 1U)];
        if (!candidate.online) {
            return false;
        }

        const auto last_seen = node.last_seen_tick[static_cast<std::size_t>(peer)];
        if (last_seen == 0U || tick < last_seen) {
            return false;
        }
        return (tick - last_seen) <= config.heartbeat_timeout_ticks;
    }

    void refresh_self_heartbeats(std::uint64_t tick) {
        for (auto& node : nodes) {
            if (!node.online) {
                continue;
            }
            node.last_seen_tick[static_cast<std::size_t>(node.id)] = tick;
        }
    }

    void apply_churn(std::uint64_t tick) {
        for (auto& node : nodes) {
            if (!node.online && tick >= node.offline_until_tick) {
                node.online = true;
                node.offline_until_tick = 0;
                node.needs_rejoin = true;
                node.safe_mode = true;
                node.partition_blocked = false;
                node.pending_cert.reset();
                node.known_peers.clear();
                add_unique_sorted(node.known_peers, node.id);
                merge_unique_sorted(node.known_peers, bootstrap_ids);
                std::fill(node.last_seen_tick.begin(), node.last_seen_tick.end(), 0U);
                node.last_seen_tick[static_cast<std::size_t>(node.id)] = tick;
                for (const auto bootstrap_id : bootstrap_ids) {
                    node.last_seen_tick[static_cast<std::size_t>(bootstrap_id)] = tick;
                }
                result.rejoin_events += 1U;
            }
        }

        if (tick > config.main_ticks || config.churn_period == 0U || config.churn_batch == 0U) {
            return;
        }
        if ((tick % config.churn_period) != 0U) {
            return;
        }

        std::uint32_t down_count = 0;
        for (std::uint32_t k = 0; k < config.node_count && down_count < config.churn_batch; ++k) {
            const auto candidate_raw =
                mix64(config.seed ^ (tick * 131ULL) ^ (static_cast<std::uint64_t>(k) * 17ULL));
            const auto candidate = static_cast<NodeId>((candidate_raw % config.node_count) + 1U);
            auto& node = nodes[static_cast<std::size_t>(candidate - 1U)];
            if (contains_sorted(bootstrap_ids, candidate)) {
                continue;
            }
            if (!node.online) {
                continue;
            }
            node.online = false;
            node.safe_mode = true;
            node.needs_rejoin = true;
            node.partition_blocked = false;
            node.offline_until_tick = tick + config.churn_down_ticks;
            node.pending_cert.reset();
            result.crash_events += 1U;
            ++down_count;
        }
    }

    void update_safe_mode(std::uint64_t tick) {
        for (auto& node : nodes) {
            if (!node.online) {
                node.safe_mode = true;
                result.safe_mode_ticks += 1U;
                continue;
            }

            if (partition_active(tick) && in_minority(node.id, tick)) {
                node.partition_blocked = true;
                node.safe_mode = true;
                result.safe_mode_ticks += 1U;
                continue;
            }

            if (node.partition_blocked) {
                node.partition_blocked = false;
                node.needs_rejoin = true;
                result.rejoin_events += 1U;
            }

            node.safe_mode = node.needs_rejoin;
            if (node.safe_mode) {
                result.safe_mode_ticks += 1U;
            }
        }
    }

    void apply_pending_certs(std::uint64_t tick) {
        for (auto& node : nodes) {
            if (!node.online || node.safe_mode || !node.pending_cert.has_value()) {
                continue;
            }
            if (node.pending_cert->effective_tick > tick) {
                continue;
            }
            node.committed_epoch = node.pending_cert->epoch;
            node.committed_members = node.pending_cert->members;
            node.pending_cert.reset();
        }
    }

    [[nodiscard]] std::vector<NodeId> online_known_for(const NodeRuntime& node, std::uint64_t tick) const {
        std::vector<NodeId> out;
        out.reserve(node.known_peers.size());
        for (const auto peer : node.known_peers) {
            if (peer == 0U || peer > config.node_count) {
                continue;
            }
            const auto& candidate = nodes[static_cast<std::size_t>(peer - 1U)];
            if (!candidate.online) {
                continue;
            }
            if (peer == node.id || is_recently_seen(node, peer, tick) || contains_sorted(node.bootstraps, peer)) {
                out.push_back(peer);
            }
        }
        if (!contains_sorted(out, node.id)) {
            out.push_back(node.id);
        }
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        return out;
    }

    [[nodiscard]] std::vector<NodeId> pick_gossip_targets(const NodeRuntime& node, std::uint64_t tick) const {
        std::vector<NodeId> candidates;
        candidates.reserve(node.known_peers.size());
        for (const auto peer : node.known_peers) {
            if (peer == node.id || peer == 0U || peer > config.node_count) {
                continue;
            }
            if (!nodes[static_cast<std::size_t>(peer - 1U)].online) {
                continue;
            }
            candidates.push_back(peer);
        }

        if (candidates.empty()) {
            for (const auto bootstrap_id : node.bootstraps) {
                if (bootstrap_id == node.id || bootstrap_id == 0U || bootstrap_id > config.node_count) {
                    continue;
                }
                if (!nodes[static_cast<std::size_t>(bootstrap_id - 1U)].online) {
                    continue;
                }
                candidates.push_back(bootstrap_id);
            }
        }

        if (candidates.empty()) {
            return {};
        }
        std::sort(candidates.begin(), candidates.end());
        candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());

        const auto fanout = std::min<std::size_t>(config.gossip_fanout, candidates.size());
        const auto start = static_cast<std::size_t>(
            mix64(config.seed ^ (static_cast<std::uint64_t>(node.id) << 32U) ^ tick) % candidates.size());

        std::vector<NodeId> out;
        out.reserve(fanout);
        for (std::size_t i = 0; i < fanout; ++i) {
            out.push_back(candidates[(start + i) % candidates.size()]);
        }
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        (void)tick;
        return out;
    }

    void gossip_step(std::uint64_t tick) {
        std::vector<GossipMessage> messages;
        for (const auto& node : nodes) {
            if (!node.online) {
                continue;
            }
            const auto targets = pick_gossip_targets(node, tick);
            const auto advertised = online_known_for(node, tick);
            for (const auto target : targets) {
                if (!can_route(node.id, target, tick)) {
                    continue;
                }
                messages.push_back(GossipMessage{
                    .from = node.id,
                    .to = target,
                    .peers = advertised,
                    .committed_epoch = node.committed_epoch,
                    .committed_members = node.committed_members,
                    .state_tick = node.local_applied_tick,
                    .state_root = node.local_state_root,
                });
            }
        }

        for (const auto& message : messages) {
            auto& dst = nodes[static_cast<std::size_t>(message.to - 1U)];
            if (!dst.online) {
                continue;
            }
            add_unique_sorted(dst.known_peers, message.from);
            merge_unique_sorted(dst.known_peers, message.peers);
            dst.last_seen_tick[static_cast<std::size_t>(message.from)] = tick;
            for (const auto peer : message.peers) {
                if (peer == 0U || peer > config.node_count) {
                    continue;
                }
                const auto gossiped_seen = tick > 0U ? tick - 1U : 0U;
                auto& seen = dst.last_seen_tick[static_cast<std::size_t>(peer)];
                seen = std::max(seen, gossiped_seen);
            }

            if (message.committed_epoch > dst.committed_epoch) {
                dst.committed_epoch = message.committed_epoch;
                dst.committed_members = message.committed_members;
                dst.pending_cert.reset();
            }

            (void)message.state_tick;
            (void)message.state_root;
        }
    }

    void failure_detection_step(std::uint64_t tick) {
        for (auto& node : nodes) {
            if (!node.online) {
                continue;
            }

            std::vector<NodeId> filtered;
            filtered.reserve(node.known_peers.size() + node.bootstraps.size() + 1U);
            add_unique_sorted(filtered, node.id);

            for (const auto peer : node.known_peers) {
                if (peer == node.id || peer == 0U || peer > config.node_count) {
                    continue;
                }
                if (contains_sorted(node.bootstraps, peer)) {
                    if (nodes[static_cast<std::size_t>(peer - 1U)].online) {
                        add_unique_sorted(filtered, peer);
                    }
                    continue;
                }
                if (is_recently_seen(node, peer, tick)) {
                    add_unique_sorted(filtered, peer);
                }
            }

            for (const auto bootstrap_id : node.bootstraps) {
                if (bootstrap_id == 0U || bootstrap_id > config.node_count) {
                    continue;
                }
                if (nodes[static_cast<std::size_t>(bootstrap_id - 1U)].online) {
                    add_unique_sorted(filtered, bootstrap_id);
                }
            }

            node.known_peers = std::move(filtered);
        }
    }

    void cert_step(std::uint64_t tick) {
        std::uint64_t majority_epoch = 0;
        for (const auto& node : nodes) {
            if (!node.online || node.safe_mode) {
                continue;
            }
            majority_epoch = std::max(majority_epoch, node.committed_epoch);
        }

        std::vector<NodeId> eligible_voters;
        for (const auto& node : nodes) {
            if (!node.online || node.safe_mode) {
                continue;
            }
            if (node.committed_epoch != majority_epoch) {
                continue;
            }
            if (!contains_sorted(node.committed_members, node.id)) {
                continue;
            }
            eligible_voters.push_back(node.id);
        }
        if (eligible_voters.empty()) {
            return;
        }

        std::vector<Proposal> proposals;
        proposals.reserve(nodes.size());
        for (const auto& node : nodes) {
            if (!node.online || node.safe_mode) {
                continue;
            }
            if (node.committed_epoch != majority_epoch) {
                continue;
            }

            auto members = online_known_for(node, tick);
            if (members == node.committed_members) {
                continue;
            }

            const auto target_epoch = majority_epoch + 1U;
            const auto effective_tick = tick + 1U;
            const auto proposal_hash = hash_view(target_epoch, effective_tick, members);
            proposals.push_back(Proposal{
                .target_epoch = target_epoch,
                .effective_tick = effective_tick,
                .proposer = node.id,
                .members = std::move(members),
                .hash = proposal_hash,
            });
        }
        if (proposals.empty()) {
            return;
        }

        std::sort(proposals.begin(), proposals.end(), [](const Proposal& lhs, const Proposal& rhs) {
            if (lhs.hash != rhs.hash) {
                return lhs.hash < rhs.hash;
            }
            return lhs.proposer < rhs.proposer;
        });
        proposals.erase(std::unique(proposals.begin(),
                                    proposals.end(),
                                    [](const Proposal& lhs, const Proposal& rhs) {
                                        return lhs.hash == rhs.hash;
                                    }),
                        proposals.end());

        std::vector<std::uint64_t> proposal_hashes;
        proposal_hashes.reserve(proposals.size());
        for (const auto& proposal : proposals) {
            proposal_hashes.push_back(proposal.hash);
        }

        std::map<std::uint64_t, std::vector<NodeId>> votes_by_hash;
        for (const auto voter : eligible_voters) {
            const auto& voter_node = nodes[static_cast<std::size_t>(voter - 1U)];
            auto voter_members = online_known_for(voter_node, tick);
            auto vote_hash = hash_view(majority_epoch + 1U, tick + 1U, voter_members);
            if (!std::binary_search(proposal_hashes.begin(), proposal_hashes.end(), vote_hash)) {
                vote_hash = proposals.front().hash;
            }
            votes_by_hash[vote_hash].push_back(voter);
        }

        const auto quorum = static_cast<std::size_t>((eligible_voters.size() / 2U) + 1U);
        std::size_t winning_votes = 0U;
        std::optional<Proposal> winning{};
        for (const auto& proposal : proposals) {
            const auto votes_it = votes_by_hash.find(proposal.hash);
            if (votes_it == votes_by_hash.end()) {
                continue;
            }
            const auto vote_count = votes_it->second.size();
            if (vote_count < quorum) {
                continue;
            }
            if (!winning.has_value() || vote_count > winning_votes ||
                (vote_count == winning_votes && proposal.hash < winning->hash)) {
                winning = proposal;
                winning_votes = vote_count;
            }
        }
        if (!winning.has_value()) {
            return;
        }

        const auto votes_it = votes_by_hash.find(winning->hash);
        if (votes_it == votes_by_hash.end()) {
            return;
        }

        ViewCert cert{
            .epoch = winning->target_epoch,
            .effective_tick = winning->effective_tick,
            .members = winning->members,
            .proposal_hash = winning->hash,
            .signers = votes_it->second,
        };
        std::sort(cert.signers.begin(), cert.signers.end());
        cert.signers.erase(std::unique(cert.signers.begin(), cert.signers.end()), cert.signers.end());

        for (auto& node : nodes) {
            if (!node.online || node.safe_mode) {
                continue;
            }
            if (node.committed_epoch != majority_epoch) {
                continue;
            }
            node.pending_cert = cert;
        }
    }

    void commit_state_tick(std::uint64_t tick) {
        std::size_t active_count = 0U;
        for (const auto& node : nodes) {
            if (node.online && !node.safe_mode) {
                ++active_count;
            }
        }

        const auto delta = static_cast<std::uint64_t>(active_count);
        const auto next_state_counter = commits.back().state_counter + delta;
        const auto next_state_root = hash_state_root(tick, next_state_counter);
        commits.push_back(TickCommit{
            .tick = tick,
            .delta = delta,
            .state_counter = next_state_counter,
            .state_root = next_state_root,
        });

        for (auto& node : nodes) {
            if (!node.online || node.safe_mode) {
                continue;
            }
            node.local_state_counter += delta;
            node.local_applied_tick = tick;
            node.local_state_root = next_state_root;
        }
    }

    [[nodiscard]] std::optional<NodeId> pick_snapshot_donor(const NodeRuntime& node, std::uint64_t tick) const {
        std::optional<NodeId> best{};
        auto consider = [&](NodeId peer) {
            if (peer == node.id || peer == 0U || peer > config.node_count) {
                return;
            }
            const auto& candidate = nodes[static_cast<std::size_t>(peer - 1U)];
            if (!candidate.online || candidate.safe_mode) {
                return;
            }
            if (!can_route(peer, node.id, tick)) {
                return;
            }
            if (candidate.local_applied_tick != tick) {
                return;
            }
            if (!best.has_value() || peer < *best) {
                best = peer;
            }
        };

        for (const auto peer : node.known_peers) {
            consider(peer);
        }
        if (best.has_value()) {
            return best;
        }

        for (const auto bootstrap_id : bootstrap_ids) {
            consider(bootstrap_id);
        }
        if (best.has_value()) {
            return best;
        }

        for (NodeId peer = 1; peer <= config.node_count; ++peer) {
            consider(peer);
        }
        return best;
    }

    void rejoin_sync_step(std::uint64_t tick) {
        for (auto& node : nodes) {
            if (!node.online || !node.needs_rejoin) {
                continue;
            }
            if (partition_active(tick) && in_minority(node.id, tick)) {
                continue;
            }

            const auto donor = pick_snapshot_donor(node, tick);
            if (!donor.has_value()) {
                continue;
            }

            const auto snapshot_tick = tick - (tick % config.snapshot_interval_ticks);
            const auto snapshot_index = static_cast<std::size_t>(snapshot_tick);
            if (snapshot_index >= commits.size()) {
                continue;
            }

            const auto& snapshot = commits[snapshot_index];
            node.local_applied_tick = snapshot.tick;
            node.local_state_counter = snapshot.state_counter;
            node.local_state_root = snapshot.state_root;

            for (std::uint64_t t = snapshot.tick + 1U; t <= tick; ++t) {
                const auto& commit = commits[static_cast<std::size_t>(t)];
                node.local_applied_tick = commit.tick;
                node.local_state_counter += commit.delta;
                node.local_state_root = commit.state_root;
            }

            const auto& donor_state = nodes[static_cast<std::size_t>(*donor - 1U)];
            node.committed_epoch = donor_state.committed_epoch;
            node.committed_members = donor_state.committed_members;
            add_unique_sorted(node.known_peers, *donor);
            node.last_seen_tick[static_cast<std::size_t>(*donor)] = tick;
            node.pending_cert.reset();
            node.needs_rejoin = false;
            node.safe_mode = false;
            result.snapshot_sync_events += 1U;
        }
    }

    void detect_state_root_drift(std::uint64_t tick) {
        const auto index = static_cast<std::size_t>(tick);
        if (index >= commits.size()) {
            return;
        }

        const auto committed_root = commits[index].state_root;
        for (auto& node : nodes) {
            if (!node.online || node.safe_mode) {
                continue;
            }
            if (node.local_applied_tick == tick && node.local_state_root == committed_root) {
                continue;
            }
            result.state_root_drift_events += 1U;
            node.needs_rejoin = true;
            node.safe_mode = true;
        }
    }

    void finalize_result() {
        result.online_nodes = 0;
        result.safe_mode_nodes = 0;

        std::optional<std::uint64_t> ref_epoch;
        std::optional<std::vector<NodeId>> ref_members;
        std::optional<std::uint64_t> ref_state_root;
        result.converged = true;
        result.committed_state_root = commits.empty() ? 0U : commits.back().state_root;

        for (const auto& node : nodes) {
            if (!node.online) {
                continue;
            }
            ++result.online_nodes;
            if (node.safe_mode) {
                ++result.safe_mode_nodes;
                result.converged = false;
            }

            if (!ref_epoch.has_value()) {
                ref_epoch = node.committed_epoch;
                ref_members = node.committed_members;
                ref_state_root = node.local_state_root;
                continue;
            }

            if (node.committed_epoch != *ref_epoch || node.committed_members != *ref_members ||
                node.local_state_root != *ref_state_root) {
                result.converged = false;
            }
        }

        if (ref_epoch.has_value()) {
            result.committed_view_epoch = *ref_epoch;
            result.committed_members = *ref_members;
            result.committed_state_root = ref_state_root.value_or(result.committed_state_root);
        } else {
            result.committed_view_epoch = 0;
            result.committed_members.clear();
            result.committed_state_root = 0;
            result.converged = false;
        }

        if (result.state_root_drift_events > 0U) {
            result.converged = false;
        }
    }

    std::vector<NodeState> export_node_states() const {
        std::vector<NodeState> out;
        out.reserve(nodes.size());
        for (const auto& node : nodes) {
            out.push_back(NodeState{
                .id = node.id,
                .online = node.online,
                .safe_mode = node.safe_mode,
                .needs_rejoin = node.needs_rejoin,
                .committed_epoch = node.committed_epoch,
                .local_applied_tick = node.local_applied_tick,
                .local_state_counter = node.local_state_counter,
                .local_state_root = node.local_state_root,
                .committed_members = node.committed_members,
                .known_peers = node.known_peers,
            });
        }
        return out;
    }

    ClusterConfig config;
    std::vector<NodeId> bootstrap_ids;
    std::vector<NodeRuntime> nodes;
    std::vector<TickCommit> commits;
    ClusterResult result{};
};

ClusterSimulator::ClusterSimulator(ClusterConfig config) : impl_(new Impl(std::move(config))) {}

ClusterSimulator::~ClusterSimulator() {
    delete impl_;
    impl_ = nullptr;
}

void ClusterSimulator::run() {
    if (impl_ == nullptr) {
        throw std::runtime_error("ClusterSimulator is not initialized");
    }

    const auto total_ticks = impl_->config.main_ticks + impl_->config.stabilization_ticks;
    for (std::uint64_t tick = 1; tick <= total_ticks; ++tick) {
        impl_->apply_churn(tick);
        impl_->refresh_self_heartbeats(tick);
        impl_->update_safe_mode(tick);
        impl_->apply_pending_certs(tick);
        impl_->gossip_step(tick);
        impl_->failure_detection_step(tick);
        impl_->cert_step(tick);
        impl_->commit_state_tick(tick);
        impl_->rejoin_sync_step(tick);
        impl_->detect_state_root_drift(tick);
    }
    impl_->finalize_result();
}

const ClusterResult& ClusterSimulator::result() const noexcept {
    return impl_->result;
}

std::vector<NodeState> ClusterSimulator::node_states() const {
    return impl_->export_node_states();
}

}  // namespace swarmnet::membership
