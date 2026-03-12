#pragma once

#include <chrono>
#include <charconv>
#include <concepts>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

#include "swarmnet/core.hpp"
#include "swarmnet/replay.hpp"

namespace swarmnet {

struct Config {
    std::string swarm_id = "default";
    std::uint32_t tick_us = 1000;
    std::uint32_t input_horizon_ticks = 1;
    std::size_t arena_page_bytes = 64U * 1024U;
    std::size_t max_arena_bytes = 256U * 1024U * 1024U;
    std::size_t max_pending_events = 65536U;
    replay::Mode replay_mode = replay::Mode::off;
    std::string replay_path;
    bool strict_replay = true;
};

struct SharedStateView {
    std::shared_ptr<const core::StateSnapshot> snapshot;
    std::string_view value;
};

struct RuntimeStats {
    std::uint64_t tick = 0;
    std::uint64_t state_root = 0;
    std::size_t arena_bytes = 0;
    std::size_t arena_limit = 0;
    std::uint64_t arena_rejections = 0;
    std::size_t pending_events = 0;
    std::size_t pending_limit = 0;
    std::uint64_t dropped_events = 0;
};

namespace detail {

template <typename T>
std::string encode_external_value(const T& value) {
    if constexpr (std::same_as<T, std::string>) {
        return value;
    } else if constexpr (std::is_same_v<T, const char*>) {
        return std::string(value);
    } else if constexpr (std::is_arithmetic_v<T>) {
        return std::to_string(value);
    } else {
        std::ostringstream oss;
        if (!(oss << value)) {
            throw std::runtime_error("record_external: value type is not serializable");
        }
        return oss.str();
    }
}

template <typename T>
T decode_external_value(const std::string& encoded) {
    if constexpr (std::same_as<T, std::string>) {
        return encoded;
    } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
        long long parsed = 0;
        const auto* begin = encoded.data();
        const auto* end = encoded.data() + encoded.size();
        const auto result = std::from_chars(begin, end, parsed);
        if (result.ec != std::errc() || result.ptr != end) {
            throw std::runtime_error("record_external: failed to decode signed integral value");
        }
        if (parsed < static_cast<long long>(std::numeric_limits<T>::min()) ||
            parsed > static_cast<long long>(std::numeric_limits<T>::max())) {
            throw std::runtime_error("record_external: signed integral value out of range");
        }
        return static_cast<T>(parsed);
    } else if constexpr (std::is_integral_v<T> && !std::is_signed_v<T>) {
        unsigned long long parsed = 0;
        const auto* begin = encoded.data();
        const auto* end = encoded.data() + encoded.size();
        const auto result = std::from_chars(begin, end, parsed);
        if (result.ec != std::errc() || result.ptr != end) {
            throw std::runtime_error("record_external: failed to decode unsigned integral value");
        }
        if (parsed > static_cast<unsigned long long>(std::numeric_limits<T>::max())) {
            throw std::runtime_error("record_external: unsigned integral value out of range");
        }
        return static_cast<T>(parsed);
    } else if constexpr (std::is_floating_point_v<T>) {
        std::istringstream iss(encoded);
        long double parsed = 0.0;
        iss >> parsed;
        if (!iss || !iss.eof()) {
            throw std::runtime_error("record_external: failed to decode floating-point value");
        }
        return static_cast<T>(parsed);
    } else {
        std::istringstream iss(encoded);
        T out{};
        if (!(iss >> out) || !iss.eof()) {
            throw std::runtime_error("record_external: failed to decode custom value");
        }
        return out;
    }
}

}  // namespace detail

class Agent {
public:
    void broadcast(std::string_view topic, std::string_view payload);
    void send_to(std::string_view peer_id, std::string_view payload);
    void set_shared_state(std::string key, std::string value);
    [[nodiscard]] std::optional<SharedStateView> get_shared_state_view(std::string_view key) const;
    [[nodiscard]] std::optional<std::string> get_shared_state(std::string_view key) const;
    [[nodiscard]] std::uint64_t tick() const noexcept;

private:
    struct Impl;
    explicit Agent(Impl* impl) noexcept;
    Impl* impl_;
    friend class SwarmNet;
};

class SwarmNet {
public:
    using AgentFn = std::function<void(Agent&)>;

    explicit SwarmNet(Config cfg);
    ~SwarmNet();

    SwarmNet(const SwarmNet&) = delete;
    SwarmNet& operator=(const SwarmNet&) = delete;
    SwarmNet(SwarmNet&&) noexcept;
    SwarmNet& operator=(SwarmNet&&) noexcept;

    void add_bootstrap(std::string endpoint);
    void spawn(AgentFn fn);
    void run_ticks(std::uint64_t tick_count);
    void run_for(std::chrono::milliseconds duration);
    void stop() noexcept;
    [[nodiscard]] std::uint64_t latest_state_root() const;
    [[nodiscard]] RuntimeStats runtime_stats() const;

    template <class Fn>
    auto record_external(std::string_view name, Fn&& fn) -> decltype(fn()) {
        using ReturnT = decltype(fn());
        const auto sequence = next_external_sequence();
        const auto replay_value = try_replay_external(sequence, name);

        if constexpr (std::is_void_v<ReturnT>) {
            if (replay_value.has_value()) {
                return;
            }
            fn();
            record_external_value(sequence, name, "");
            return;
        } else {
            if (replay_value.has_value()) {
                return detail::decode_external_value<ReturnT>(*replay_value);
            }

            ReturnT result = fn();
            record_external_value(sequence, name, detail::encode_external_value(result));
            return result;
        }
    }

private:
    [[nodiscard]] std::uint64_t next_external_sequence() const;
    [[nodiscard]] std::optional<std::string> try_replay_external(std::uint64_t sequence,
                                                                 std::string_view name) const;
    void record_external_value(std::uint64_t sequence, std::string_view name, std::string value);

    struct Impl;
    Impl* impl_;
};

}  // namespace swarmnet
