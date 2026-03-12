#include "support/determinism.hpp"
#include "swarmnet/swarmnet.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

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

bool test_rng_is_reproducible() {
    swarmnet::test::DeterministicRng a(12345);
    swarmnet::test::DeterministicRng b(12345);
    for (int i = 0; i < 256; ++i) {
        EXPECT_TRUE(a.next_u64() == b.next_u64());
    }
    return true;
}

bool test_fake_clock() {
    swarmnet::test::FakeClock clock(10);
    EXPECT_TRUE(clock.now() == 10);
    clock.advance(5);
    EXPECT_TRUE(clock.now() == 15);
    return true;
}

bool test_trace_recorder_digest() {
    swarmnet::test::TraceRecorder rec1;
    swarmnet::test::TraceRecorder rec2;

    for (std::uint64_t t = 1; t <= 100; ++t) {
        rec1.record(t, "event_" + std::to_string(t));
        rec2.record(t, "event_" + std::to_string(t));
    }

    EXPECT_TRUE(rec1.size() == 100);
    EXPECT_TRUE(rec2.size() == 100);
    EXPECT_TRUE(rec1.digest() == rec2.digest());
    return true;
}

bool test_swarmnet_minimal_smoke() {
    using namespace std::chrono_literals;

    swarmnet::Config cfg;
    cfg.swarm_id = "week1-smoke";
    cfg.tick_us = 250;

    swarmnet::SwarmNet swarm(cfg);
    std::atomic<std::uint64_t> ticks{0};

    swarm.spawn([&ticks](swarmnet::Agent& agent) {
        agent.broadcast("hb", "1");
        agent.send_to("peer-a", "hello");
        agent.set_shared_state("alive", "yes");
        (void)agent.get_shared_state("alive");
        ticks.fetch_add(1, std::memory_order_relaxed);
    });

    swarm.run_for(3ms);
    EXPECT_TRUE(ticks.load(std::memory_order_relaxed) > 0);
    return true;
}

bool test_zero_copy_view_and_backpressure_stats() {
    swarmnet::Config cfg;
    cfg.swarm_id = "week7-zero-copy";
    cfg.tick_us = 1;
    cfg.max_pending_events = 1;
    cfg.max_arena_bytes = 2U * 1024U * 1024U;

    swarmnet::SwarmNet swarm(cfg);
    std::atomic<bool> saw_view{false};
    std::atomic<bool> stable_non_empty{false};

    swarm.spawn([&](swarmnet::Agent& agent) {
        agent.set_shared_state("k", "value_a");
        agent.set_shared_state("k2", "value_b");
        const auto view = agent.get_shared_state_view("k");
        if (view.has_value()) {
            saw_view.store(true, std::memory_order_relaxed);
            if (!view->value.empty() && view->snapshot != nullptr) {
                stable_non_empty.store(true, std::memory_order_relaxed);
            }
        }
    });

    swarm.run_ticks(8);
    const auto stats = swarm.runtime_stats();
    EXPECT_TRUE(saw_view.load(std::memory_order_relaxed));
    EXPECT_TRUE(stable_non_empty.load(std::memory_order_relaxed));
    EXPECT_TRUE(stats.dropped_events > 0U);
    EXPECT_TRUE(stats.pending_limit == 1U);
    return true;
}

}  // namespace

int main() {
    if (!test_rng_is_reproducible()) {
        return 1;
    }
    if (!test_fake_clock()) {
        return 1;
    }
    if (!test_trace_recorder_digest()) {
        return 1;
    }
    if (!test_swarmnet_minimal_smoke()) {
        return 1;
    }
    if (!test_zero_copy_view_and_backpressure_stats()) {
        return 1;
    }

    std::cout << "determinism harness passed\n";
    return 0;
}
