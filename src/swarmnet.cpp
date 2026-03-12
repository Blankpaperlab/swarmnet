#include "swarmnet/swarmnet.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace swarmnet {

struct Agent::Impl {
    Config cfg;
    std::vector<std::string> bootstrap;
    std::vector<SwarmNet::AgentFn> callbacks;
    core::DeterministicKernel kernel;
    std::mutex mu;
    std::atomic<bool> stop_requested{false};
    std::atomic<std::uint64_t> next_origin_seq{1};
    std::atomic<std::uint64_t> next_external_seq{1};
    core::NodeId local_node_id{0, 1};
    std::unique_ptr<replay::Journal> journal;

    explicit Impl(Config in_cfg) : cfg(std::move(in_cfg)), kernel(cfg.tick_us, cfg.input_horizon_ticks) {
        kernel.configure_memory(cfg.arena_page_bytes, cfg.max_arena_bytes, cfg.max_pending_events);
        if (cfg.replay_mode != replay::Mode::off) {
            journal = std::make_unique<replay::Journal>(cfg.replay_mode, cfg.replay_path);
        }
    }
};

struct SwarmNet::Impl {
    explicit Impl(Config cfg) : agent(std::move(cfg)) {}
    Agent::Impl agent;
};

Agent::Agent(Impl* impl) noexcept : impl_(impl) {}

void Agent::broadcast(std::string_view topic, std::string_view payload) {
    if (topic.empty()) {
        return;
    }
    set_shared_state(std::string("broadcast/") + std::string(topic), std::string(payload));
}

void Agent::send_to(std::string_view peer_id, std::string_view payload) {
    if (peer_id.empty()) {
        return;
    }
    set_shared_state(std::string("direct/") + std::string(peer_id), std::string(payload));
}

void Agent::set_shared_state(std::string key, std::string value) {
    std::scoped_lock lock(impl_->mu);
    core::Event event;
    event.tick = impl_->kernel.next_ingest_tick();
    event.node_id = impl_->local_node_id;
    event.origin_seq = impl_->next_origin_seq.fetch_add(1U, std::memory_order_relaxed);
    event.type = core::EventType::upsert;
    event.key = std::move(key);
    event.value = std::move(value);
    (void)impl_->kernel.try_enqueue(std::move(event));
}

std::optional<SharedStateView> Agent::get_shared_state_view(std::string_view key) const {
    const auto snapshot = impl_->kernel.latest_snapshot();
    const auto value = snapshot->view_of(key);
    if (!value.has_value()) {
        return std::nullopt;
    }
    return SharedStateView{
        .snapshot = snapshot,
        .value = *value,
    };
}

std::optional<std::string> Agent::get_shared_state(std::string_view key) const {
    const auto value = get_shared_state_view(key);
    if (!value.has_value()) {
        return std::nullopt;
    }
    return std::string(value->value.data(), value->value.size());
}

std::uint64_t Agent::tick() const noexcept {
    return impl_->kernel.current_tick();
}

SwarmNet::SwarmNet(Config cfg) : impl_(new Impl(std::move(cfg))) {}

SwarmNet::~SwarmNet() {
    delete impl_;
}

SwarmNet::SwarmNet(SwarmNet&& other) noexcept : impl_(other.impl_) {
    other.impl_ = nullptr;
}

SwarmNet& SwarmNet::operator=(SwarmNet&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    delete impl_;
    impl_ = other.impl_;
    other.impl_ = nullptr;
    return *this;
}

void SwarmNet::add_bootstrap(std::string endpoint) {
    if (endpoint.empty()) {
        return;
    }
    std::scoped_lock lock(impl_->agent.mu);
    impl_->agent.bootstrap.push_back(std::move(endpoint));
}

void SwarmNet::spawn(AgentFn fn) {
    if (!fn) {
        return;
    }
    std::scoped_lock lock(impl_->agent.mu);
    impl_->agent.callbacks.push_back(std::move(fn));
}

void SwarmNet::run_ticks(std::uint64_t tick_count) {
    impl_->agent.stop_requested.store(false, std::memory_order_release);
    Agent agent(&impl_->agent);

    if (impl_->agent.cfg.replay_mode == replay::Mode::playback) {
        const auto unlimited = std::numeric_limits<std::uint64_t>::max();
        std::uint64_t remaining = tick_count == 0U ? unlimited : tick_count;

        while (!impl_->agent.stop_requested.load(std::memory_order_acquire) && remaining > 0U) {
            replay::TickRecord replay_tick{};
            if (impl_->agent.journal == nullptr || !impl_->agent.journal->read_next_tick(replay_tick)) {
                break;
            }

            core::CommitResult commit{};
            {
                std::scoped_lock lock(impl_->agent.mu);
                for (const auto& event : replay_tick.events) {
                    impl_->agent.kernel.enqueue(event);
                }
                commit = impl_->agent.kernel.commit_next_tick_with_events();
            }

            if (commit.snapshot->tick != replay_tick.tick) {
                throw std::runtime_error("replay tick mismatch");
            }
            if (impl_->agent.cfg.strict_replay && commit.snapshot->state_root != replay_tick.state_root) {
                throw std::runtime_error("replay state-root mismatch");
            }

            if (remaining != unlimited) {
                --remaining;
            }
        }
        return;
    }

    for (std::uint64_t i = 0; i < tick_count; ++i) {
        if (impl_->agent.stop_requested.load(std::memory_order_acquire)) {
            break;
        }

        std::vector<AgentFn> callbacks_copy;
        {
            std::scoped_lock lock(impl_->agent.mu);
            callbacks_copy = impl_->agent.callbacks;
        }

        for (auto& fn : callbacks_copy) {
            fn(agent);
        }

        core::CommitResult commit{};
        {
            std::scoped_lock lock(impl_->agent.mu);
            commit = impl_->agent.kernel.commit_next_tick_with_events();
        }

        if (impl_->agent.cfg.replay_mode == replay::Mode::record && impl_->agent.journal != nullptr) {
            impl_->agent.journal->append_tick(replay::TickRecord{
                .tick = commit.snapshot->tick,
                .state_root = commit.snapshot->state_root,
                .events = std::move(commit.committed_events),
            });
        }
    }
}

void SwarmNet::run_for(std::chrono::milliseconds duration) {
    if (duration <= std::chrono::milliseconds::zero()) {
        return;
    }

    const auto tick_us = std::max(impl_->agent.cfg.tick_us, 1U);
    const auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    std::uint64_t ticks = static_cast<std::uint64_t>(total_us / static_cast<long long>(tick_us));
    if (ticks == 0U) {
        ticks = 1U;
    }
    run_ticks(ticks);
}

void SwarmNet::stop() noexcept {
    impl_->agent.stop_requested.store(true, std::memory_order_release);
}

std::uint64_t SwarmNet::latest_state_root() const {
    return impl_->agent.kernel.latest_snapshot()->state_root;
}

RuntimeStats SwarmNet::runtime_stats() const {
    const auto snapshot = impl_->agent.kernel.latest_snapshot();
    const auto mem = impl_->agent.kernel.memory_stats();
    return RuntimeStats{
        .tick = snapshot->tick,
        .state_root = snapshot->state_root,
        .arena_bytes = mem.arena_bytes,
        .arena_limit = mem.arena_limit,
        .arena_rejections = mem.arena_rejections,
        .pending_events = mem.pending_events,
        .pending_limit = mem.pending_limit,
        .dropped_events = mem.dropped_events,
    };
}

std::uint64_t SwarmNet::next_external_sequence() const {
    return impl_->agent.next_external_seq.fetch_add(1U, std::memory_order_relaxed);
}

std::optional<std::string> SwarmNet::try_replay_external(std::uint64_t sequence,
                                                         std::string_view name) const {
    if (impl_->agent.cfg.replay_mode != replay::Mode::playback) {
        return std::nullopt;
    }
    if (impl_->agent.journal == nullptr) {
        throw std::runtime_error("playback mode enabled but journal is missing");
    }

    const auto value = impl_->agent.journal->read_external(sequence, name);
    if (!value.has_value() && impl_->agent.cfg.strict_replay) {
        throw std::runtime_error("missing replay external value");
    }
    return value;
}

void SwarmNet::record_external_value(std::uint64_t sequence, std::string_view name, std::string value) {
    if (impl_->agent.cfg.replay_mode != replay::Mode::record || impl_->agent.journal == nullptr) {
        return;
    }
    impl_->agent.journal->append_external(replay::ExternalRecord{
        .sequence = sequence,
        .name = std::string(name),
        .value = std::move(value),
    });
}

}  // namespace swarmnet
