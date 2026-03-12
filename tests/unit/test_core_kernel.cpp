#include "support/determinism.hpp"
#include "swarmnet/core.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
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

std::uint32_t read_u32_le(const std::vector<std::uint8_t>& in, std::size_t offset) {
    return static_cast<std::uint32_t>(static_cast<std::uint32_t>(in[offset]) |
                                      (static_cast<std::uint32_t>(in[offset + 1U]) << 8U) |
                                      (static_cast<std::uint32_t>(in[offset + 2U]) << 16U) |
                                      (static_cast<std::uint32_t>(in[offset + 3U]) << 24U));
}

std::vector<swarmnet::core::Event> make_property_events() {
    using swarmnet::core::Event;
    using swarmnet::core::EventType;
    using swarmnet::core::NodeId;

    return {
        Event{.tick = 10, .node_id = NodeId{0, 1}, .origin_seq = 3, .type = EventType::upsert, .key = "k1", .value = "a"},
        Event{.tick = 10, .node_id = NodeId{0, 1}, .origin_seq = 1, .type = EventType::upsert, .key = "k2", .value = "b"},
        Event{.tick = 10, .node_id = NodeId{0, 2}, .origin_seq = 2, .type = EventType::upsert, .key = "k3", .value = "c"},
        Event{.tick = 10, .node_id = NodeId{0, 2}, .origin_seq = 4, .type = EventType::erase,  .key = "k2", .value = ""},
        Event{.tick = 10, .node_id = NodeId{0, 3}, .origin_seq = 1, .type = EventType::upsert, .key = "k4", .value = "d"},
        Event{.tick = 10, .node_id = NodeId{0, 3}, .origin_seq = 2, .type = EventType::upsert, .key = "k1", .value = "z"},
    };
}

std::uint64_t apply_once(const std::vector<swarmnet::core::Event>& events) {
    swarmnet::core::StateStore store;
    const auto snapshot = store.apply(10, events);
    return snapshot->state_root;
}

bool test_tick_scheduler() {
    swarmnet::core::TickScheduler scheduler(250, 3);
    EXPECT_TRUE(scheduler.current_tick() == 0);
    EXPECT_TRUE(scheduler.next_ingest_tick() == 3);
    EXPECT_TRUE(scheduler.advance() == 1);
    EXPECT_TRUE(scheduler.current_tick() == 1);
    EXPECT_TRUE(scheduler.next_ingest_tick() == 4);
    EXPECT_TRUE(scheduler.tick_period_us() == 250);
    EXPECT_TRUE(scheduler.horizon_ticks() == 3);
    return true;
}

bool test_state_store_immutable() {
    using swarmnet::core::Event;
    using swarmnet::core::EventType;
    using swarmnet::core::NodeId;

    swarmnet::core::StateStore store;
    const auto snapshot0 = store.current();
    EXPECT_TRUE(snapshot0->tick == 0);

    const auto snapshot1 = store.apply(1, {Event{
                                            .tick = 1,
                                            .node_id = NodeId{0, 1},
                                            .origin_seq = 1,
                                            .type = EventType::upsert,
                                            .key = "alpha",
                                            .value = "one",
                                        }});
    const auto snapshot2 = store.apply(2, {Event{
                                            .tick = 2,
                                            .node_id = NodeId{0, 1},
                                            .origin_seq = 2,
                                            .type = EventType::upsert,
                                            .key = "alpha",
                                            .value = "two",
                                        }});

    EXPECT_TRUE(snapshot1->tick == 1);
    EXPECT_TRUE(snapshot2->tick == 2);
    EXPECT_TRUE(snapshot1->copy_of("alpha").value_or("") == "one");
    EXPECT_TRUE(snapshot2->copy_of("alpha").value_or("") == "two");
    EXPECT_TRUE(snapshot1->state_root != snapshot2->state_root);

    const auto v1 = snapshot1->view_of("alpha");
    EXPECT_TRUE(v1.has_value());
    EXPECT_TRUE(v1->data() == snapshot1->kv->at("alpha")->view().data());
    return true;
}

bool test_canonical_sort_is_stable() {
    using swarmnet::core::Event;
    using swarmnet::core::EventType;
    using swarmnet::core::NodeId;

    std::vector<Event> events{
        Event{.tick = 2, .node_id = NodeId{1, 0}, .origin_seq = 2, .type = EventType::upsert, .key = "x", .value = "2"},
        Event{.tick = 1, .node_id = NodeId{1, 0}, .origin_seq = 2, .type = EventType::upsert, .key = "x", .value = "1"},
        Event{.tick = 1, .node_id = NodeId{1, 0}, .origin_seq = 1, .type = EventType::upsert, .key = "x", .value = "0"},
    };

    const auto ordered = swarmnet::core::canonicalize_events(events);
    EXPECT_TRUE(ordered.size() == 3);
    EXPECT_TRUE(ordered[0].tick == 1);
    EXPECT_TRUE(ordered[0].origin_seq == 1);
    EXPECT_TRUE(ordered[1].tick == 1);
    EXPECT_TRUE(ordered[1].origin_seq == 2);
    EXPECT_TRUE(ordered[2].tick == 2);
    return true;
}

bool test_serialization_contract() {
    using swarmnet::core::Event;
    using swarmnet::core::EventType;
    using swarmnet::core::NodeId;

    const Event in{
        .tick = 42,
        .node_id = NodeId{0x0102030405060708ULL, 0x090A0B0C0D0E0F10ULL},
        .origin_seq = 77,
        .type = EventType::upsert,
        .key = "alpha",
        .value = "beta",
    };

    const auto frame = swarmnet::core::serialize_event(in);
    EXPECT_TRUE(frame.size() > swarmnet::core::kHeaderSize);

    swarmnet::core::SerializationHeader header{};
    EXPECT_TRUE(swarmnet::core::read_header(frame, header));
    EXPECT_TRUE(header.magic == swarmnet::core::kFrameMagic);
    EXPECT_TRUE(header.schema_version == swarmnet::core::kSchemaVersion);
    EXPECT_TRUE(header.payload_type == swarmnet::core::kPayloadTypeEvent);
    EXPECT_TRUE(static_cast<std::size_t>(header.payload_size) == frame.size() - swarmnet::core::kHeaderSize);
    EXPECT_TRUE(read_u32_le(frame, 0) == swarmnet::core::kFrameMagic);
    EXPECT_TRUE(frame[4] == 1U && frame[5] == 0U);
    EXPECT_TRUE(frame[6] == 1U && frame[7] == 0U);

    swarmnet::core::Event out{};
    EXPECT_TRUE(swarmnet::core::deserialize_event(frame, out));
    EXPECT_TRUE(out == in);

    auto corrupted = frame;
    corrupted[swarmnet::core::kHeaderSize + 1U] ^= 0x80U;
    EXPECT_TRUE(!swarmnet::core::deserialize_event(corrupted, out));
    return true;
}

bool test_property_permuted_input_same_root() {
    auto events = make_property_events();
    swarmnet::test::DeterministicRng rng(20260311);

    std::uint64_t first_root = 0;
    constexpr int rounds = 300;
    for (int round = 0; round < rounds; ++round) {
        for (std::size_t i = events.size(); i > 1U; --i) {
            const auto j = static_cast<std::size_t>(rng.next_u64() % static_cast<std::uint64_t>(i));
            std::swap(events[i - 1U], events[j]);
        }

        const auto root = apply_once(events);
        if (round == 0) {
            first_root = root;
        } else {
            EXPECT_TRUE(root == first_root);
        }
    }
    return true;
}

bool test_100k_ticks_stable() {
    using swarmnet::core::DeterministicKernel;
    using swarmnet::core::Event;
    using swarmnet::core::EventType;
    using swarmnet::core::NodeId;

    const auto run = []() {
        DeterministicKernel kernel(1, 1);
        std::uint64_t trace_digest = 1469598103934665603ULL;
        for (std::uint64_t tick = 1; tick <= 100000ULL; ++tick) {
            kernel.enqueue(Event{
                .tick = tick,
                .node_id = NodeId{0, 1},
                .origin_seq = tick,
                .type = EventType::upsert,
                .key = "counter",
                .value = std::to_string(tick),
            });
            const auto snapshot = kernel.commit_next_tick();
            trace_digest ^= snapshot->state_root;
            trace_digest *= 1099511628211ULL;
        }
        return std::pair{kernel.latest_snapshot()->state_root, trace_digest};
    };

    const auto first = run();
    const auto second = run();
    EXPECT_TRUE(first.first == second.first);
    EXPECT_TRUE(first.second == second.second);
    return true;
}

bool test_backpressure_and_arena_rejection() {
    using swarmnet::core::DeterministicKernel;
    using swarmnet::core::Event;
    using swarmnet::core::EventType;
    using swarmnet::core::NodeId;

    DeterministicKernel kernel(1, 1);
    kernel.configure_memory(128U, 256U, 2U);

    const auto mk = [](std::uint64_t seq, std::string value) {
        return Event{
            .tick = 1,
            .node_id = NodeId{0, 1},
            .origin_seq = seq,
            .type = EventType::upsert,
            .key = "k" + std::to_string(seq),
            .value = std::move(value),
        };
    };

    EXPECT_TRUE(kernel.try_enqueue(mk(1, "a")));
    EXPECT_TRUE(kernel.try_enqueue(mk(2, "b")));
    EXPECT_TRUE(!kernel.try_enqueue(mk(3, "c")));

    auto stats = kernel.memory_stats();
    EXPECT_TRUE(stats.pending_events == 2U);
    EXPECT_TRUE(stats.pending_limit == 2U);
    EXPECT_TRUE(stats.dropped_events == 1U);

    (void)kernel.commit_next_tick();

    const auto large = std::string(1024U, 'z');
    EXPECT_TRUE(kernel.try_enqueue(mk(4, large)));
    (void)kernel.commit_next_tick();

    stats = kernel.memory_stats();
    EXPECT_TRUE(stats.arena_rejections > 0U);
    return true;
}

}  // namespace

int main() {
    if (!test_tick_scheduler()) {
        return 1;
    }
    if (!test_state_store_immutable()) {
        return 1;
    }
    if (!test_canonical_sort_is_stable()) {
        return 1;
    }
    if (!test_serialization_contract()) {
        return 1;
    }
    if (!test_property_permuted_input_same_root()) {
        return 1;
    }
    if (!test_100k_ticks_stable()) {
        return 1;
    }
    if (!test_backpressure_and_arena_rejection()) {
        return 1;
    }

    std::cout << "core kernel tests passed\n";
    return 0;
}
