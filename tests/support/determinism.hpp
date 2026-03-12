#pragma once

#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace swarmnet::test {

inline std::uint64_t splitmix64(std::uint64_t& x) noexcept {
    std::uint64_t z = (x += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27U)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31U);
}

class DeterministicRng {
public:
    explicit DeterministicRng(std::uint64_t seed) noexcept : state_(seed) {}
    [[nodiscard]] std::uint64_t next_u64() noexcept { return splitmix64(state_); }

private:
    std::uint64_t state_;
};

inline std::uint64_t seed_from_env(const char* key, std::uint64_t fallback) noexcept {
    const char* value = std::getenv(key);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    char* end = nullptr;
    const auto parsed = std::strtoull(value, &end, 10);
    if (end == value || *end != '\0') {
        return fallback;
    }
    return static_cast<std::uint64_t>(parsed);
}

class FakeClock {
public:
    explicit FakeClock(std::uint64_t start = 0) noexcept : tick_(start) {}
    [[nodiscard]] std::uint64_t now() const noexcept { return tick_; }
    void advance(std::uint64_t delta = 1) noexcept { tick_ += delta; }

private:
    std::uint64_t tick_;
};

struct TraceEntry {
    std::uint64_t tick = 0;
    std::string event;
};

class TraceRecorder {
public:
    void record(std::uint64_t tick, std::string event) {
        entries_.push_back(TraceEntry{tick, std::move(event)});
    }

    [[nodiscard]] std::string digest() const {
        std::uint64_t h = 1469598103934665603ULL;
        for (const auto& e : entries_) {
            h ^= e.tick;
            h *= 1099511628211ULL;
            for (const auto ch : e.event) {
                h ^= static_cast<std::uint64_t>(static_cast<unsigned char>(ch));
                h *= 1099511628211ULL;
            }
        }
        std::ostringstream out;
        out << std::hex << std::setw(16) << std::setfill('0') << h;
        return out.str();
    }

    [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }

private:
    std::vector<TraceEntry> entries_;
};

}  // namespace swarmnet::test
