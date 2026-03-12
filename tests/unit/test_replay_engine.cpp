#include "swarmnet/swarmnet.hpp"

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>
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

void install_agents(swarmnet::SwarmNet& swarm, std::uint32_t agents) {
    for (std::uint32_t id = 0; id < agents; ++id) {
        swarm.spawn([id, agents, &swarm](swarmnet::Agent& self) {
            const auto tick = self.tick();
            const auto next = static_cast<std::uint32_t>((id + 1U) % agents);
            self.broadcast("hb/" + std::to_string(id), std::to_string(tick));
            self.send_to("peer/" + std::to_string(next), std::to_string(tick ^ id));

            const auto ext = swarm.record_external("sensor/" + std::to_string(id), [tick, id]() {
                return static_cast<std::uint64_t>((tick * 1099511628211ULL) ^
                                                  (static_cast<std::uint64_t>(id) * 1469598103934665603ULL));
            });
            self.set_shared_state("ext/" + std::to_string(id), std::to_string(ext));
        });
    }
}

bool test_record_then_playback_same_root() {
    const auto replay_path = std::filesystem::path("swarmnet_replay_engine_test.bin");
    std::error_code ec;
    std::filesystem::remove(replay_path, ec);

    constexpr std::uint32_t agents = 120;
    constexpr std::uint64_t ticks = 300;

    swarmnet::Config record_cfg;
    record_cfg.swarm_id = "week3-replay";
    record_cfg.tick_us = 1;
    record_cfg.input_horizon_ticks = 1;
    record_cfg.replay_mode = swarmnet::replay::Mode::record;
    record_cfg.replay_path = replay_path.string();

    swarmnet::SwarmNet record_swarm(record_cfg);
    install_agents(record_swarm, agents);
    const auto global_recorded = record_swarm.record_external("global_seed", []() { return 424242ULL; });
    EXPECT_TRUE(global_recorded == 424242ULL);
    record_swarm.run_ticks(ticks);
    const auto recorded_root = record_swarm.latest_state_root();

    swarmnet::Config playback_cfg = record_cfg;
    playback_cfg.replay_mode = swarmnet::replay::Mode::playback;
    playback_cfg.strict_replay = true;
    swarmnet::SwarmNet playback_swarm(playback_cfg);

    bool fallback_called = false;
    const auto global_playback = playback_swarm.record_external("global_seed", [&fallback_called]() {
        fallback_called = true;
        return 1ULL;
    });
    EXPECT_TRUE(!fallback_called);
    EXPECT_TRUE(global_playback == global_recorded);

    playback_swarm.run_ticks(0);
    const auto replayed_root = playback_swarm.latest_state_root();
    EXPECT_TRUE(replayed_root == recorded_root);

    std::filesystem::remove(replay_path, ec);
    return true;
}

}  // namespace

int main() {
    try {
        if (!test_record_then_playback_same_root()) {
            return 1;
        }
        std::cout << "replay engine tests passed\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Unexpected exception: " << ex.what() << '\n';
        return 1;
    }
}
