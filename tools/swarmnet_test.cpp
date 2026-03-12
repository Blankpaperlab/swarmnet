#include "swarmnet/swarmnet.hpp"
#include "swarmnet/membership.hpp"
#include "swarmnet/transport.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <psapi.h>
#else
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

enum class RunMode {
    single = 0,
    multi = 1,
    worker = 2,
    cluster = 3,
    bench = 4,
};

struct Options {
    RunMode mode = RunMode::single;
    std::uint32_t agents = 500;
    std::uint64_t ticks = 200;
    std::uint32_t processes = 10;
    std::uint32_t workers = 0;
    std::uint32_t worker_id = 0;
    std::uint16_t base_port = 39000;
    std::string result_dir = ".swarmnet-multiprocess";
    std::uint32_t bootstrap = 3;
    std::uint32_t gossip_fanout = 4;
    std::uint64_t stabilization_ticks = 60;
    std::uint64_t churn_period = 24;
    std::uint32_t churn_batch = 4;
    std::uint64_t churn_down_ticks = 6;
    std::uint64_t heartbeat_timeout_ticks = 8;
    std::uint64_t snapshot_interval_ticks = 16;
    std::uint64_t partition_start = 40;
    std::uint64_t partition_end = 100;
    std::uint32_t partition_minority = 30;
    std::size_t arena_page_kb = 64;
    std::size_t arena_max_mb = 256;
    std::size_t max_pending_events = 65536;
    std::size_t payload_bytes = 48;
    std::uint64_t target_cpu_percent = 40;
    std::size_t target_mem_mb = 3072;
    std::uint64_t bench_sample_ticks = 20;
    std::string replay_out;
    std::string replay_in;
    bool quiet = false;
};

bool starts_with(std::string_view text, std::string_view prefix) {
    return text.size() >= prefix.size() && text.substr(0, prefix.size()) == prefix;
}

std::uint64_t parse_u64(std::string_view value, const char* name) {
    const std::string text(value);
    char* end = nullptr;
    const auto parsed = std::strtoull(text.c_str(), &end, 10);
    if (end == nullptr || *end != '\0') {
        throw std::runtime_error(std::string("invalid numeric value for ") + name);
    }
    return static_cast<std::uint64_t>(parsed);
}

RunMode parse_mode(std::string_view text) {
    if (text == "single") {
        return RunMode::single;
    }
    if (text == "multi") {
        return RunMode::multi;
    }
    if (text == "worker") {
        return RunMode::worker;
    }
    if (text == "cluster") {
        return RunMode::cluster;
    }
    if (text == "bench") {
        return RunMode::bench;
    }
    throw std::runtime_error("invalid mode: " + std::string(text));
}

Options parse_args(int argc, char** argv) {
    Options options{};
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        if (starts_with(arg, "--mode=")) {
            options.mode = parse_mode(arg.substr(7));
            continue;
        }
        if (starts_with(arg, "--agents=")) {
            const auto value = parse_u64(arg.substr(9), "--agents");
            if (value == 0U || value > 100000U) {
                throw std::runtime_error("--agents must be in [1, 100000]");
            }
            options.agents = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--ticks=")) {
            options.ticks = parse_u64(arg.substr(8), "--ticks");
            continue;
        }
        if (starts_with(arg, "--processes=")) {
            const auto value = parse_u64(arg.substr(12), "--processes");
            if (value == 0U || value > 1024U) {
                throw std::runtime_error("--processes must be in [1, 1024]");
            }
            options.processes = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--workers=")) {
            const auto value = parse_u64(arg.substr(10), "--workers");
            if (value == 0U || value > 1024U) {
                throw std::runtime_error("--workers must be in [1, 1024]");
            }
            options.workers = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--worker-id=")) {
            const auto value = parse_u64(arg.substr(12), "--worker-id");
            if (value > 1024U) {
                throw std::runtime_error("--worker-id must be <= 1024");
            }
            options.worker_id = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--base-port=")) {
            const auto value = parse_u64(arg.substr(12), "--base-port");
            if (value < 1024U || value > 65000U) {
                throw std::runtime_error("--base-port must be in [1024, 65000]");
            }
            options.base_port = static_cast<std::uint16_t>(value);
            continue;
        }
        if (starts_with(arg, "--bootstrap=")) {
            const auto value = parse_u64(arg.substr(12), "--bootstrap");
            if (value == 0U || value > 100000U) {
                throw std::runtime_error("--bootstrap must be in [1, 100000]");
            }
            options.bootstrap = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--fanout=")) {
            const auto value = parse_u64(arg.substr(9), "--fanout");
            if (value == 0U || value > 1024U) {
                throw std::runtime_error("--fanout must be in [1, 1024]");
            }
            options.gossip_fanout = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--stabilization-ticks=")) {
            options.stabilization_ticks = parse_u64(arg.substr(22), "--stabilization-ticks");
            continue;
        }
        if (starts_with(arg, "--churn-period=")) {
            options.churn_period = parse_u64(arg.substr(15), "--churn-period");
            continue;
        }
        if (starts_with(arg, "--churn-batch=")) {
            const auto value = parse_u64(arg.substr(14), "--churn-batch");
            if (value > 100000U) {
                throw std::runtime_error("--churn-batch too large");
            }
            options.churn_batch = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--churn-down-ticks=")) {
            options.churn_down_ticks = parse_u64(arg.substr(19), "--churn-down-ticks");
            continue;
        }
        if (starts_with(arg, "--heartbeat-timeout=")) {
            options.heartbeat_timeout_ticks = parse_u64(arg.substr(20), "--heartbeat-timeout");
            continue;
        }
        if (starts_with(arg, "--snapshot-interval=")) {
            options.snapshot_interval_ticks = parse_u64(arg.substr(20), "--snapshot-interval");
            continue;
        }
        if (starts_with(arg, "--arena-page-kb=")) {
            const auto value = parse_u64(arg.substr(16), "--arena-page-kb");
            if (value == 0U || value > 1024U * 1024U) {
                throw std::runtime_error("--arena-page-kb must be in [1, 1048576]");
            }
            options.arena_page_kb = static_cast<std::size_t>(value);
            continue;
        }
        if (starts_with(arg, "--arena-max-mb=")) {
            const auto value = parse_u64(arg.substr(15), "--arena-max-mb");
            if (value == 0U || value > 1024U * 1024U) {
                throw std::runtime_error("--arena-max-mb must be in [1, 1048576]");
            }
            options.arena_max_mb = static_cast<std::size_t>(value);
            continue;
        }
        if (starts_with(arg, "--max-pending-events=")) {
            const auto value = parse_u64(arg.substr(21), "--max-pending-events");
            if (value == 0U || value > 100000000U) {
                throw std::runtime_error("--max-pending-events must be in [1, 100000000]");
            }
            options.max_pending_events = static_cast<std::size_t>(value);
            continue;
        }
        if (starts_with(arg, "--payload-bytes=")) {
            const auto value = parse_u64(arg.substr(16), "--payload-bytes");
            if (value == 0U || value > 65536U) {
                throw std::runtime_error("--payload-bytes must be in [1, 65536]");
            }
            options.payload_bytes = static_cast<std::size_t>(value);
            continue;
        }
        if (starts_with(arg, "--target-cpu=")) {
            const auto value = parse_u64(arg.substr(13), "--target-cpu");
            if (value == 0U || value > 100U) {
                throw std::runtime_error("--target-cpu must be in [1, 100]");
            }
            options.target_cpu_percent = value;
            continue;
        }
        if (starts_with(arg, "--target-mem-mb=")) {
            const auto value = parse_u64(arg.substr(16), "--target-mem-mb");
            if (value == 0U || value > 1024U * 1024U) {
                throw std::runtime_error("--target-mem-mb must be in [1, 1048576]");
            }
            options.target_mem_mb = static_cast<std::size_t>(value);
            continue;
        }
        if (starts_with(arg, "--bench-sample-ticks=")) {
            const auto value = parse_u64(arg.substr(21), "--bench-sample-ticks");
            if (value == 0U || value > 1000000U) {
                throw std::runtime_error("--bench-sample-ticks must be in [1, 1000000]");
            }
            options.bench_sample_ticks = value;
            continue;
        }
        if (starts_with(arg, "--partition-start=")) {
            options.partition_start = parse_u64(arg.substr(18), "--partition-start");
            continue;
        }
        if (starts_with(arg, "--partition-end=")) {
            options.partition_end = parse_u64(arg.substr(16), "--partition-end");
            continue;
        }
        if (starts_with(arg, "--partition-minority=")) {
            const auto value = parse_u64(arg.substr(21), "--partition-minority");
            if (value > 100000U) {
                throw std::runtime_error("--partition-minority too large");
            }
            options.partition_minority = static_cast<std::uint32_t>(value);
            continue;
        }
        if (starts_with(arg, "--result-dir=")) {
            options.result_dir = std::string(arg.substr(13));
            continue;
        }
        if (starts_with(arg, "--replay-out=")) {
            options.replay_out = std::string(arg.substr(13));
            continue;
        }
        if (starts_with(arg, "--replay-in=")) {
            options.replay_in = std::string(arg.substr(12));
            continue;
        }
        if (arg == "--quiet") {
            options.quiet = true;
            continue;
        }
        if (arg == "--help") {
            std::cout
                << "Usage:\n"
                << "  swarmnet_test [--mode=single] [--agents=N] [--ticks=N] [--replay-out=PATH] [--replay-in=PATH]\n"
                << "  swarmnet_test --mode=multi [--agents=N] [--ticks=N] [--processes=N] [--base-port=P]\n"
                << "  swarmnet_test --mode=worker --workers=N --worker-id=ID [--agents=N] [--ticks=N]\n"
                << "  swarmnet_test --mode=cluster [--agents=100] [--bootstrap=3] [--fanout=4] [--ticks=220]\n"
                << "    [--heartbeat-timeout=8] [--snapshot-interval=16] [--churn-batch=30]\n"
                << "  swarmnet_test --mode=bench [--agents=1000] [--ticks=300] [--payload-bytes=48]\n"
                << "    [--arena-max-mb=256] [--target-cpu=40] [--target-mem-mb=3072]\n";
            std::exit(0);
        }
        throw std::runtime_error("unknown argument: " + std::string(arg));
    }

    if (!options.replay_out.empty() && !options.replay_in.empty()) {
        throw std::runtime_error("use either --replay-out or --replay-in, not both");
    }
    return options;
}

struct AgentPartition {
    std::uint32_t start_index = 0;
    std::uint32_t count = 0;
};

AgentPartition partition_for_worker(std::uint32_t total_agents,
                                    std::uint32_t workers,
                                    std::uint32_t worker_id) {
    const auto base = total_agents / workers;
    const auto rem = total_agents % workers;
    const auto count = base + (worker_id < rem ? 1U : 0U);
    const auto start = (worker_id * base) + std::min(worker_id, rem);
    return AgentPartition{.start_index = start, .count = count};
}

std::uint64_t hash_mix(std::uint64_t state, std::uint64_t value) {
    constexpr std::uint64_t kPrime = 1099511628211ULL;
    state ^= value;
    state *= kPrime;
    return state;
}

std::uint64_t process_rss_bytes() {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS counters{};
    const auto ok = GetProcessMemoryInfo(GetCurrentProcess(),
                                         &counters,
                                         sizeof(counters));
    if (ok == 0) {
        return 0U;
    }
    return static_cast<std::uint64_t>(counters.WorkingSetSize);
#else
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return 0U;
    }
#if defined(__APPLE__)
    return static_cast<std::uint64_t>(usage.ru_maxrss);
#else
    return static_cast<std::uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
#endif
}

std::vector<std::uint8_t> make_payload(std::uint64_t tick, std::uint32_t agent_id) {
    std::vector<std::uint8_t> out;
    out.reserve(12U);
    for (std::uint32_t shift = 0; shift < 64U; shift += 8U) {
        out.push_back(static_cast<std::uint8_t>((tick >> shift) & 0xFFU));
    }
    for (std::uint32_t shift = 0; shift < 32U; shift += 8U) {
        out.push_back(static_cast<std::uint8_t>((agent_id >> shift) & 0xFFU));
    }
    return out;
}

struct WorkerStats {
    bool ok = false;
    std::uint64_t digest = 1469598103934665603ULL;
    std::uint64_t reliable_sent = 0;
    std::uint64_t reliable_received = 0;
    std::uint64_t unreliable_received = 0;
    std::uint32_t handshake_peers = 0;
    std::string backend;
    std::string error;
};

std::filesystem::path worker_result_path(const std::string& result_dir, std::uint32_t worker_id) {
    return std::filesystem::path(result_dir) / ("worker_" + std::to_string(worker_id) + ".txt");
}

void write_worker_result(const std::filesystem::path& path, const WorkerStats& stats) {
    std::ofstream out(path, std::ios::trunc);
    if (!out.is_open()) {
        throw std::runtime_error("failed to write worker result file: " + path.string());
    }
    out << "status=" << (stats.ok ? "ok" : "error") << '\n';
    out << "backend=" << stats.backend << '\n';
    out << "handshake_peers=" << stats.handshake_peers << '\n';
    out << "reliable_sent=" << stats.reliable_sent << '\n';
    out << "reliable_received=" << stats.reliable_received << '\n';
    out << "unreliable_received=" << stats.unreliable_received << '\n';
    out << "digest=" << stats.digest << '\n';
    out << "error=" << stats.error << '\n';
}

std::map<std::string, std::string> read_key_values(const std::filesystem::path& path) {
    std::ifstream in(path);
    if (!in.is_open()) {
        throw std::runtime_error("missing worker result file: " + path.string());
    }

    std::map<std::string, std::string> kv;
    std::string line;
    while (std::getline(in, line)) {
        const auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        kv[line.substr(0, pos)] = line.substr(pos + 1);
    }
    return kv;
}

std::string make_state_payload(std::uint64_t tick, std::uint32_t agent_id, std::size_t payload_bytes) {
    std::string payload = std::to_string(tick) + ":" + std::to_string(agent_id);
    if (payload.size() < payload_bytes) {
        payload.append(payload_bytes - payload.size(), 'x');
    } else if (payload.size() > payload_bytes) {
        payload.resize(payload_bytes);
    }
    return payload;
}

void install_agents(swarmnet::SwarmNet& swarm, std::uint32_t agents, std::size_t payload_bytes) {
    for (std::uint32_t id = 0; id < agents; ++id) {
        swarm.spawn([id, agents, payload_bytes, &swarm](swarmnet::Agent& self) {
            const auto tick = self.tick();
            const auto next = static_cast<std::uint32_t>((id + 1U) % agents);
            const auto payload = make_state_payload(tick, id, payload_bytes);

            self.broadcast("hb/" + std::to_string(id), payload);
            self.send_to("agent/" + std::to_string(next), payload);
            self.set_shared_state("state/" + std::to_string(id), payload);
            (void)self.get_shared_state_view("state/" + std::to_string(id));

            const auto ext = swarm.record_external("sensor/" + std::to_string(id), [tick, id]() {
                return static_cast<std::uint64_t>((tick * 1315423911ULL) ^
                                                  (static_cast<std::uint64_t>(id) * 2654435761ULL));
            });
            self.set_shared_state("ext/" + std::to_string(id), std::to_string(ext));
        });
    }
}

int run_single_mode(const Options& opts) {
    swarmnet::Config cfg;
    cfg.swarm_id = "swarmnet-local-sim";
    cfg.tick_us = 1;
    cfg.input_horizon_ticks = 1;
    cfg.arena_page_bytes = opts.arena_page_kb * 1024U;
    cfg.max_arena_bytes = opts.arena_max_mb * 1024U * 1024U;
    cfg.max_pending_events = opts.max_pending_events;
    if (!opts.replay_out.empty()) {
        cfg.replay_mode = swarmnet::replay::Mode::record;
        cfg.replay_path = opts.replay_out;
    } else if (!opts.replay_in.empty()) {
        cfg.replay_mode = swarmnet::replay::Mode::playback;
        cfg.replay_path = opts.replay_in;
    }

    swarmnet::SwarmNet swarm(cfg);
    install_agents(swarm, opts.agents, opts.payload_bytes);
    swarm.run_ticks(opts.ticks);

    if (!opts.quiet) {
        const auto stats = swarm.runtime_stats();
        std::cout << "agents=" << opts.agents
                  << " ticks=" << opts.ticks
                  << " state_root=" << swarm.latest_state_root()
                  << " arena_mb=" << (stats.arena_bytes / (1024U * 1024U))
                  << " dropped_events=" << stats.dropped_events
                  << '\n';
    }
    return 0;
}

int run_worker_mode(const Options& opts) {
    if (opts.workers == 0U) {
        throw std::runtime_error("worker mode requires --workers");
    }
    if (opts.worker_id >= opts.workers) {
        throw std::runtime_error("--worker-id must be < --workers");
    }

    WorkerStats stats{};
    const auto part = partition_for_worker(opts.agents, opts.workers, opts.worker_id);
    const auto node_id = static_cast<std::uint64_t>(opts.worker_id + 1U);

    auto backend = swarmnet::transport::create_default_backend();
    stats.backend = backend->backend_name();

    swarmnet::transport::Endpoint local_ep{
        .host = "127.0.0.1",
        .port = static_cast<std::uint16_t>(opts.base_port + opts.worker_id),
    };
    std::string error;
    if (!backend->bind(local_ep, error)) {
        stats.error = "bind failed: " + error;
        write_worker_result(worker_result_path(opts.result_dir, opts.worker_id), stats);
        return 1;
    }

    std::unordered_map<std::string, swarmnet::transport::Endpoint> peers;
    std::unordered_map<std::string, bool> hello_acked;
    std::unordered_map<std::string, bool> done_by_peer;
    for (std::uint32_t i = 0; i < opts.workers; ++i) {
        if (i == opts.worker_id) {
            continue;
        }
        swarmnet::transport::Endpoint ep{
            .host = "127.0.0.1",
            .port = static_cast<std::uint16_t>(opts.base_port + i),
        };
        const auto key = swarmnet::transport::endpoint_to_string(ep);
        peers[key] = ep;
        hello_acked[key] = false;
        done_by_peer[key] = false;
    }

    const auto handshake_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (std::chrono::steady_clock::now() < handshake_deadline) {
        bool all_acked = true;
        for (const auto& [key, peer] : peers) {
            if (hello_acked[key]) {
                continue;
            }
            all_acked = false;
            (void)backend->send_hello(peer, node_id, error);
        }
        if (all_acked) {
            break;
        }

        backend->poll(std::chrono::milliseconds(8),
                      [&backend, &hello_acked, &stats, node_id](const swarmnet::transport::Packet& pkt) {
                          std::string unused;
                          if (pkt.kind == swarmnet::transport::PacketKind::hello) {
                              (void)backend->send_hello_ack(pkt.from, node_id, unused);
                              return;
                          }
                          if (pkt.kind == swarmnet::transport::PacketKind::hello_ack) {
                              const auto key = swarmnet::transport::endpoint_to_string(pkt.from);
                              auto it = hello_acked.find(key);
                              if (it != hello_acked.end()) {
                                  it->second = true;
                              }
                              return;
                          }
                          if (pkt.kind == swarmnet::transport::PacketKind::reliable) {
                              stats.reliable_received += 1U;
                              stats.digest = hash_mix(stats.digest, pkt.sequence);
                              stats.digest = hash_mix(stats.digest, pkt.stream_id);
                              return;
                          }
                          if (pkt.kind == swarmnet::transport::PacketKind::unreliable) {
                              stats.unreliable_received += 1U;
                              stats.digest = hash_mix(stats.digest,
                                                      static_cast<std::uint64_t>(pkt.payload.size()));
                          }
                      });
        backend->maintenance();
    }

    for (const auto& [key, acked] : hello_acked) {
        (void)key;
        if (!acked) {
            stats.error = "handshake did not converge";
            write_worker_result(worker_result_path(opts.result_dir, opts.worker_id), stats);
            return 1;
        }
        stats.handshake_peers += 1U;
    }

    auto handle_packet = [&backend, &stats, node_id, &done_by_peer](const swarmnet::transport::Packet& pkt) {
        std::string unused;
        if (pkt.kind == swarmnet::transport::PacketKind::hello) {
            (void)backend->send_hello_ack(pkt.from, node_id, unused);
            return;
        }
        if (pkt.kind == swarmnet::transport::PacketKind::reliable) {
            stats.reliable_received += 1U;
            stats.digest = hash_mix(stats.digest, pkt.sequence);
            stats.digest = hash_mix(stats.digest, pkt.stream_id);
            return;
        }
        if (pkt.kind == swarmnet::transport::PacketKind::unreliable) {
            if (pkt.payload.size() >= 5U &&
                pkt.payload[0] == static_cast<std::uint8_t>('D') &&
                pkt.payload[1] == static_cast<std::uint8_t>('O') &&
                pkt.payload[2] == static_cast<std::uint8_t>('N') &&
                pkt.payload[3] == static_cast<std::uint8_t>('E') &&
                pkt.payload[4] == static_cast<std::uint8_t>(':')) {
                const auto key = swarmnet::transport::endpoint_to_string(pkt.from);
                auto it = done_by_peer.find(key);
                if (it != done_by_peer.end()) {
                    it->second = true;
                }
                return;
            }
            stats.unreliable_received += 1U;
            stats.digest = hash_mix(stats.digest, static_cast<std::uint64_t>(pkt.payload.size()));
        }
    };

    for (std::uint64_t tick = 1; tick <= opts.ticks; ++tick) {
        for (std::uint32_t local_index = 0; local_index < part.count; ++local_index) {
            const auto global_agent = part.start_index + local_index;
            const auto target_worker =
                static_cast<std::uint32_t>((global_agent + static_cast<std::uint32_t>(tick) + 1U) % opts.workers);
            if (target_worker == opts.worker_id) {
                continue;
            }

            swarmnet::transport::Endpoint target_ep{
                .host = "127.0.0.1",
                .port = static_cast<std::uint16_t>(opts.base_port + target_worker),
            };

            const auto payload = make_payload(tick, global_agent);
            if (((global_agent + static_cast<std::uint32_t>(tick)) % 5U) == 0U) {
                (void)backend->send_unreliable(target_ep, payload, error);
            }

            if (((global_agent + static_cast<std::uint32_t>(tick)) % 3U) == 0U) {
                std::uint64_t seq = 0;
                const auto stream_id = static_cast<std::uint32_t>(global_agent % 8U);
                if (backend->send_reliable(target_ep, stream_id, payload, seq, error)) {
                    stats.reliable_sent += 1U;
                    stats.digest = hash_mix(stats.digest, seq);
                }
            }
        }

        backend->poll(std::chrono::milliseconds(2), handle_packet);
        backend->maintenance();
    }

    const auto done_payload_text = std::string("DONE:") + std::to_string(opts.worker_id);
    std::vector<std::uint8_t> done_payload(done_payload_text.begin(), done_payload_text.end());

    auto all_peers_done = [&done_by_peer]() {
        for (const auto& [key, done] : done_by_peer) {
            (void)key;
            if (!done) {
                return false;
            }
        }
        return true;
    };

    auto last_done_broadcast = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    const auto finalize_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (std::chrono::steady_clock::now() < finalize_deadline) {
        const auto now = std::chrono::steady_clock::now();
        if (now - last_done_broadcast >= std::chrono::milliseconds(100)) {
            for (const auto& [key, peer] : peers) {
                (void)key;
                (void)backend->send_unreliable(peer, done_payload, error);
            }
            last_done_broadcast = now;
        }

        backend->poll(std::chrono::milliseconds(6), handle_packet);
        backend->maintenance();

        if (all_peers_done() && backend->pending_reliable_count() == 0U) {
            break;
        }
    }

    if (!all_peers_done()) {
        stats.error = "mesh finalization timeout (missing DONE markers)";
        write_worker_result(worker_result_path(opts.result_dir, opts.worker_id), stats);
        return 1;
    }
    if (backend->pending_reliable_count() != 0U) {
        stats.error = "reliable delivery did not drain after finalization";
        write_worker_result(worker_result_path(opts.result_dir, opts.worker_id), stats);
        return 1;
    }

    stats.ok = true;
    write_worker_result(worker_result_path(opts.result_dir, opts.worker_id), stats);
    return 0;
}

struct ChildProcess {
    std::uint32_t worker_id = 0;
    std::filesystem::path result_file;
#if defined(_WIN32)
    HANDLE handle = nullptr;
#else
    pid_t pid = 0;
#endif
};

std::string quote_arg(const std::string& text) {
    if (text.find_first_of(" \t\"") == std::string::npos) {
        return text;
    }
    std::string out = "\"";
    for (const char ch : text) {
        if (ch == '"') {
            out += "\\\"";
        } else {
            out.push_back(ch);
        }
    }
    out.push_back('"');
    return out;
}

bool spawn_worker_process(const std::string& exe_path,
                          const std::vector<std::string>& args,
                          ChildProcess& out_child,
                          std::string& error) {
#if defined(_WIN32)
    std::string cmd = quote_arg(exe_path);
    for (const auto& arg : args) {
        cmd += " " + quote_arg(arg);
    }

    STARTUPINFOA si{};
    si.cb = static_cast<DWORD>(sizeof(si));
    PROCESS_INFORMATION pi{};
    std::vector<char> mutable_cmd(cmd.begin(), cmd.end());
    mutable_cmd.push_back('\0');

    if (CreateProcessA(nullptr,
                       mutable_cmd.data(),
                       nullptr,
                       nullptr,
                       FALSE,
                       CREATE_NO_WINDOW,
                       nullptr,
                       nullptr,
                       &si,
                       &pi) == 0) {
        error = "CreateProcessA failed";
        return false;
    }

    CloseHandle(pi.hThread);
    out_child.handle = pi.hProcess;
    return true;
#else
    pid_t pid = fork();
    if (pid < 0) {
        error = "fork() failed";
        return false;
    }
    if (pid == 0) {
        std::vector<std::string> argv_text;
        argv_text.reserve(args.size() + 1U);
        argv_text.push_back(exe_path);
        argv_text.insert(argv_text.end(), args.begin(), args.end());

        std::vector<char*> argv_exec;
        argv_exec.reserve(argv_text.size() + 1U);
        for (auto& item : argv_text) {
            argv_exec.push_back(item.data());
        }
        argv_exec.push_back(nullptr);
        execv(exe_path.c_str(), argv_exec.data());
        std::exit(127);
    }
    out_child.pid = pid;
    return true;
#endif
}

int wait_worker_process(ChildProcess& child, std::string& error) {
#if defined(_WIN32)
    const DWORD wait_rc = WaitForSingleObject(child.handle, INFINITE);
    if (wait_rc != WAIT_OBJECT_0) {
        error = "WaitForSingleObject failed";
        CloseHandle(child.handle);
        child.handle = nullptr;
        return 1;
    }

    DWORD exit_code = 1;
    if (GetExitCodeProcess(child.handle, &exit_code) == 0) {
        error = "GetExitCodeProcess failed";
        CloseHandle(child.handle);
        child.handle = nullptr;
        return 1;
    }
    CloseHandle(child.handle);
    child.handle = nullptr;
    return static_cast<int>(exit_code);
#else
    int status = 0;
    if (waitpid(child.pid, &status, 0) < 0) {
        error = "waitpid failed";
        return 1;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    error = "worker terminated unexpectedly";
    return 1;
#endif
}

int run_multi_mode(const Options& opts, const std::string& argv0) {
    const auto workers = opts.workers == 0U ? opts.processes : opts.workers;
    if (workers < 2U) {
        throw std::runtime_error("multi mode requires at least 2 workers");
    }

    const auto dir = std::filesystem::path(opts.result_dir);
    std::filesystem::create_directories(dir);
    for (std::uint32_t i = 0; i < workers; ++i) {
        std::error_code ec;
        std::filesystem::remove(worker_result_path(opts.result_dir, i), ec);
    }

    const auto exe_path = std::filesystem::absolute(argv0).string();
    std::vector<ChildProcess> children;
    children.reserve(workers);

    for (std::uint32_t i = 0; i < workers; ++i) {
        ChildProcess child{};
        child.worker_id = i;
        child.result_file = worker_result_path(opts.result_dir, i);

        std::vector<std::string> args{
            "--mode=worker",
            "--agents=" + std::to_string(opts.agents),
            "--ticks=" + std::to_string(opts.ticks),
            "--workers=" + std::to_string(workers),
            "--worker-id=" + std::to_string(i),
            "--base-port=" + std::to_string(opts.base_port),
            "--result-dir=" + opts.result_dir,
            "--quiet",
        };

        std::string error;
        if (!spawn_worker_process(exe_path, args, child, error)) {
            throw std::runtime_error("failed to spawn worker " + std::to_string(i) + ": " + error);
        }
        children.push_back(child);
    }

    for (auto& child : children) {
        std::string error;
        const int rc = wait_worker_process(child, error);
        if (rc != 0) {
            throw std::runtime_error("worker " + std::to_string(child.worker_id) +
                                     " failed with code " + std::to_string(rc) + " (" + error + ")");
        }
    }

    std::uint64_t combined_digest = 1469598103934665603ULL;
    std::uint64_t total_reliable_sent = 0;
    std::uint64_t total_reliable_received = 0;
    std::uint64_t total_unreliable_received = 0;
    for (std::uint32_t i = 0; i < workers; ++i) {
        const auto kv = read_key_values(worker_result_path(opts.result_dir, i));
        const auto status_it = kv.find("status");
        if (status_it == kv.end() || status_it->second != "ok") {
            const auto err_it = kv.find("error");
            throw std::runtime_error("worker " + std::to_string(i) +
                                     " reported error: " +
                                     (err_it == kv.end() ? std::string("unknown") : err_it->second));
        }

        const auto digest = std::stoull(kv.at("digest"));
        const auto reliable_sent = std::stoull(kv.at("reliable_sent"));
        const auto reliable_received = std::stoull(kv.at("reliable_received"));
        const auto unreliable_received = std::stoull(kv.at("unreliable_received"));

        combined_digest = hash_mix(combined_digest, digest);
        total_reliable_sent += reliable_sent;
        total_reliable_received += reliable_received;
        total_unreliable_received += unreliable_received;
    }

    if (!opts.quiet) {
        std::cout << "mode=multi"
                  << " agents=" << opts.agents
                  << " workers=" << workers
                  << " ticks=" << opts.ticks
                  << " combined_root=" << combined_digest
                  << " reliable_sent=" << total_reliable_sent
                  << " reliable_received=" << total_reliable_received
                  << " unreliable_received=" << total_unreliable_received
                  << '\n';
    }
    return 0;
}

int run_cluster_mode(const Options& opts) {
    swarmnet::membership::ClusterConfig cfg{};
    cfg.node_count = opts.agents;
    cfg.bootstrap_count = opts.bootstrap;
    cfg.gossip_fanout = opts.gossip_fanout;
    cfg.main_ticks = opts.ticks;
    cfg.stabilization_ticks = opts.stabilization_ticks;
    cfg.churn_period = opts.churn_period;
    cfg.churn_batch = opts.churn_batch;
    cfg.churn_down_ticks = opts.churn_down_ticks;
    cfg.heartbeat_timeout_ticks = opts.heartbeat_timeout_ticks;
    cfg.snapshot_interval_ticks = opts.snapshot_interval_ticks;
    cfg.partition_start_tick = opts.partition_start;
    cfg.partition_end_tick = opts.partition_end;
    cfg.partition_minority_count = opts.partition_minority;

    swarmnet::membership::ClusterSimulator cluster(cfg);
    cluster.run();
    const auto result = cluster.result();

    if (!opts.quiet) {
        std::cout << "mode=cluster"
                  << " nodes=" << opts.agents
                  << " bootstrap=" << opts.bootstrap
                  << " fanout=" << opts.gossip_fanout
                  << " converged=" << (result.converged ? 1 : 0)
                  << " committed_view_epoch=" << result.committed_view_epoch
                  << " state_root=" << result.committed_state_root
                  << " online_nodes=" << result.online_nodes
                  << " safe_mode_nodes=" << result.safe_mode_nodes
                  << " safe_mode_ticks=" << result.safe_mode_ticks
                  << " crash_events=" << result.crash_events
                  << " rejoin_events=" << result.rejoin_events
                  << " snapshot_sync_events=" << result.snapshot_sync_events
                  << " drift_events=" << result.state_root_drift_events
                  << '\n';
    }

    return (result.converged && result.state_root_drift_events == 0U) ? 0 : 1;
}

int run_bench_mode(const Options& opts) {
    swarmnet::Config cfg;
    cfg.swarm_id = "swarmnet-week7-bench";
    cfg.tick_us = 1;
    cfg.input_horizon_ticks = 1;
    cfg.arena_page_bytes = opts.arena_page_kb * 1024U;
    cfg.max_arena_bytes = opts.arena_max_mb * 1024U * 1024U;
    cfg.max_pending_events = opts.max_pending_events;

    swarmnet::SwarmNet swarm(cfg);
    install_agents(swarm, opts.agents, opts.payload_bytes);

    const auto wall_start = std::chrono::steady_clock::now();
    const auto cpu_start = std::clock();
    std::uint64_t peak_rss = process_rss_bytes();

    std::uint64_t remaining = opts.ticks;
    while (remaining > 0U) {
        const auto step = std::min<std::uint64_t>(remaining, opts.bench_sample_ticks);
        swarm.run_ticks(step);
        remaining -= step;
        peak_rss = std::max(peak_rss, process_rss_bytes());
    }

    const auto cpu_end = std::clock();
    const auto wall_end = std::chrono::steady_clock::now();
    const auto stats = swarm.runtime_stats();

    const auto wall_seconds = std::chrono::duration<double>(wall_end - wall_start).count();
    const auto cpu_seconds =
        static_cast<double>(cpu_end - cpu_start) / static_cast<double>(CLOCKS_PER_SEC);
    const auto logical_cores = std::max(1U, std::thread::hardware_concurrency());
    const auto cpu_percent = wall_seconds > 0.0
                                 ? (cpu_seconds / wall_seconds) *
                                       (100.0 / static_cast<double>(logical_cores))
                                 : 0.0;

    const auto arena_mb = stats.arena_bytes / (1024U * 1024U);
    const auto rss_mb = peak_rss == 0U ? 0U : peak_rss / (1024U * 1024U);
    const auto measured_mem_mb = rss_mb == 0U ? arena_mb : std::max(arena_mb, rss_mb);

    const auto cpu_ok = cpu_percent <= static_cast<double>(opts.target_cpu_percent);
    const auto mem_ok = measured_mem_mb <= opts.target_mem_mb;
    const auto pass = cpu_ok && mem_ok;

    if (!opts.quiet) {
        std::cout << std::fixed << std::setprecision(2)
                  << "mode=bench"
                  << " agents=" << opts.agents
                  << " ticks=" << opts.ticks
                  << " wall_s=" << wall_seconds
                  << " cpu_pct=" << cpu_percent
                  << " rss_mb=" << rss_mb
                  << " arena_mb=" << arena_mb
                  << " dropped_events=" << stats.dropped_events
                  << " arena_rejections=" << stats.arena_rejections
                  << " target_cpu_pct=" << opts.target_cpu_percent
                  << " target_mem_mb=" << opts.target_mem_mb
                  << " pass=" << (pass ? 1 : 0)
                  << " state_root=" << stats.state_root
                  << '\n';
    }

    return pass ? 0 : 2;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const auto opts = parse_args(argc, argv);
        if (opts.mode == RunMode::single) {
            return run_single_mode(opts);
        }
        if (opts.mode == RunMode::worker) {
            return run_worker_mode(opts);
        }
        if (opts.mode == RunMode::cluster) {
            return run_cluster_mode(opts);
        }
        if (opts.mode == RunMode::bench) {
            return run_bench_mode(opts);
        }
        return run_multi_mode(opts, argv[0]);
    } catch (const std::exception& ex) {
        std::cerr << "swarmnet_test error: " << ex.what() << '\n';
        return 1;
    }
}
