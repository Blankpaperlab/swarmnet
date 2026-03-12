#pragma once

#include <atomic>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace swarmnet::core {

using Tick = std::uint64_t;
using EventId = std::uint64_t;

struct NodeId {
    std::uint64_t hi = 0;
    std::uint64_t lo = 0;

    auto operator<=>(const NodeId&) const = default;
};

enum class EventType : std::uint16_t {
    upsert = 1,
    erase = 2,
};

struct Event {
    Tick tick = 0;
    NodeId node_id{};
    EventId origin_seq = 0;
    EventType type = EventType::upsert;
    std::string key;
    std::string value;

    bool operator==(const Event&) const = default;
};

struct ImmutableBlob {
    std::shared_ptr<const std::vector<std::uint8_t>> owner;
    std::uint32_t offset = 0;
    std::uint32_t size = 0;

    [[nodiscard]] std::string_view view() const noexcept;
    [[nodiscard]] std::string str() const;
};

using SharedStateMap = std::map<std::string, std::shared_ptr<const ImmutableBlob>, std::less<>>;

struct StateSnapshot {
    Tick tick = 0;
    std::uint64_t state_root = 0;
    std::shared_ptr<const SharedStateMap> kv;

    [[nodiscard]] std::optional<std::string_view> view_of(std::string_view key) const noexcept;
    [[nodiscard]] std::optional<std::string> copy_of(std::string_view key) const;
};

struct CommitResult {
    std::shared_ptr<const StateSnapshot> snapshot;
    std::vector<Event> committed_events;
};

struct SerializationHeader {
    std::uint32_t magic = 0;
    std::uint16_t schema_version = 0;
    std::uint16_t payload_type = 0;
    std::uint32_t payload_size = 0;
    std::uint32_t payload_crc32c = 0;
};

inline constexpr std::uint32_t kFrameMagic = 0x53574E31U;     // SWN1
inline constexpr std::uint16_t kSchemaVersion = 1U;
inline constexpr std::uint16_t kPayloadTypeEvent = 1U;
inline constexpr std::size_t kHeaderSize = 16U;

struct MemoryStats {
    std::size_t arena_bytes = 0;
    std::size_t arena_limit = 0;
    std::uint64_t arena_rejections = 0;
    std::size_t pending_events = 0;
    std::size_t pending_limit = 0;
    std::uint64_t dropped_events = 0;
};

class TickScheduler {
public:
    explicit TickScheduler(std::uint32_t tick_us = 1000, std::uint32_t horizon_ticks = 1);

    [[nodiscard]] Tick current_tick() const noexcept;
    [[nodiscard]] Tick next_ingest_tick() const noexcept;
    [[nodiscard]] Tick advance() noexcept;
    [[nodiscard]] std::uint32_t tick_period_us() const noexcept;
    [[nodiscard]] std::uint32_t horizon_ticks() const noexcept;

private:
    std::atomic<Tick> current_tick_{0};
    std::uint32_t tick_period_us_ = 1000;
    std::uint32_t horizon_ticks_ = 1;
};

class StateStore {
public:
    StateStore();
    ~StateStore();

    void configure_memory(std::size_t arena_page_bytes, std::size_t arena_max_bytes);
    [[nodiscard]] std::shared_ptr<const StateSnapshot> current() const;
    [[nodiscard]] std::shared_ptr<const StateSnapshot> apply(Tick tick, const std::vector<Event>& events);
    [[nodiscard]] MemoryStats memory_stats() const noexcept;

private:
    struct BlobArena;
    std::unique_ptr<BlobArena> arena_;
    std::atomic<std::uint64_t> arena_rejections_{0};
    std::atomic<std::shared_ptr<const StateSnapshot>> current_snapshot_;
};

class DeterministicKernel {
public:
    explicit DeterministicKernel(std::uint32_t tick_us = 1000, std::uint32_t horizon_ticks = 1);

    void enqueue(Event event);
    [[nodiscard]] bool try_enqueue(Event event);
    void configure_memory(std::size_t arena_page_bytes,
                          std::size_t arena_max_bytes,
                          std::size_t max_pending_events);
    [[nodiscard]] CommitResult commit_next_tick_with_events();
    [[nodiscard]] std::shared_ptr<const StateSnapshot> commit_next_tick();
    [[nodiscard]] std::shared_ptr<const StateSnapshot> latest_snapshot() const;
    [[nodiscard]] Tick current_tick() const noexcept;
    [[nodiscard]] Tick next_ingest_tick() const noexcept;
    [[nodiscard]] MemoryStats memory_stats() const;

private:
    TickScheduler scheduler_;
    StateStore state_store_;
    mutable std::mutex mutex_;
    std::vector<Event> pending_;
    std::size_t max_pending_events_ = 65536U;
    std::atomic<std::uint64_t> dropped_events_{0};
};

[[nodiscard]] std::vector<Event> canonicalize_events(std::vector<Event> events);
[[nodiscard]] std::uint64_t payload_hash64(const Event& event) noexcept;
[[nodiscard]] std::uint64_t compute_state_root(const SharedStateMap& state) noexcept;
[[nodiscard]] std::uint32_t crc32c(const std::uint8_t* data, std::size_t size) noexcept;
[[nodiscard]] std::vector<std::uint8_t> serialize_event(const Event& event);
[[nodiscard]] bool deserialize_event(const std::vector<std::uint8_t>& frame, Event& out_event);
[[nodiscard]] bool read_header(const std::vector<std::uint8_t>& frame, SerializationHeader& out_header);

}  // namespace swarmnet::core
