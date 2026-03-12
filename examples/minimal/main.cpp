#include "swarmnet/swarmnet.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>

int main() {
    using namespace std::chrono_literals;

    swarmnet::Config cfg;
    cfg.swarm_id = "minimal-example";
    cfg.tick_us = 500;

    swarmnet::SwarmNet swarm(cfg);
    swarm.add_bootstrap("127.0.0.1:9000");

    std::atomic<std::uint64_t> count{0};
    swarm.spawn([&count](swarmnet::Agent& self) {
        self.broadcast("heartbeat", "alive");
        self.set_shared_state("status", "running");
        count.fetch_add(1, std::memory_order_relaxed);
    });

    swarm.run_for(2ms);
    std::cout << "ticks=" << count.load(std::memory_order_relaxed) << '\n';
    return 0;
}
