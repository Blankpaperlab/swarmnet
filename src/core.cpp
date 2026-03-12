#include "swarmnet/core.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <mutex>
#include <new>
#include <utility>

namespace swarmnet::core {

namespace {

constexpr std::uint64_t kFnvOffset64 = 1469598103934665603ULL;
constexpr std::uint64_t kFnvPrime64 = 1099511628211ULL;
constexpr std::uint32_t kCrc32cPolynomial = 0x82F63B78U;
constexpr std::size_t kDefaultArenaPageBytes = 64U * 1024U;
constexpr std::size_t kDefaultArenaMaxBytes = 256U * 1024U * 1024U;

void hash_bytes(std::uint64_t& state, const std::uint8_t* data, std::size_t len) noexcept {
    for (std::size_t i = 0; i < len; ++i) {
        state ^= static_cast<std::uint64_t>(data[i]);
        state *= kFnvPrime64;
    }
}

void hash_u32(std::uint64_t& state, std::uint32_t value) noexcept {
    std::array<std::uint8_t, 4> bytes{
        static_cast<std::uint8_t>(value & 0xFFU),
        static_cast<std::uint8_t>((value >> 8U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 16U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 24U) & 0xFFU),
    };
    hash_bytes(state, bytes.data(), bytes.size());
}

void hash_u64(std::uint64_t& state, std::uint64_t value) noexcept {
    std::array<std::uint8_t, 8> bytes{
        static_cast<std::uint8_t>(value & 0xFFU),
        static_cast<std::uint8_t>((value >> 8U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 16U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 24U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 32U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 40U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 48U) & 0xFFU),
        static_cast<std::uint8_t>((value >> 56U) & 0xFFU),
    };
    hash_bytes(state, bytes.data(), bytes.size());
}

void append_u16_le(std::vector<std::uint8_t>& out, std::uint16_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
}

void append_u32_le(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
}

void append_u64_le(std::vector<std::uint8_t>& out, std::uint64_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 32U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 40U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 48U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 56U) & 0xFFU));
}

bool read_u16_le(const std::vector<std::uint8_t>& in, std::size_t& offset, std::uint16_t& out) {
    if (offset + 2U > in.size()) {
        return false;
    }
    out = static_cast<std::uint16_t>(static_cast<std::uint16_t>(in[offset]) |
                                     (static_cast<std::uint16_t>(in[offset + 1U]) << 8U));
    offset += 2U;
    return true;
}

bool read_u32_le(const std::vector<std::uint8_t>& in, std::size_t& offset, std::uint32_t& out) {
    if (offset + 4U > in.size()) {
        return false;
    }
    out = static_cast<std::uint32_t>(static_cast<std::uint32_t>(in[offset]) |
                                     (static_cast<std::uint32_t>(in[offset + 1U]) << 8U) |
                                     (static_cast<std::uint32_t>(in[offset + 2U]) << 16U) |
                                     (static_cast<std::uint32_t>(in[offset + 3U]) << 24U));
    offset += 4U;
    return true;
}

bool read_u64_le(const std::vector<std::uint8_t>& in, std::size_t& offset, std::uint64_t& out) {
    if (offset + 8U > in.size()) {
        return false;
    }
    out = static_cast<std::uint64_t>(static_cast<std::uint64_t>(in[offset]) |
                                     (static_cast<std::uint64_t>(in[offset + 1U]) << 8U) |
                                     (static_cast<std::uint64_t>(in[offset + 2U]) << 16U) |
                                     (static_cast<std::uint64_t>(in[offset + 3U]) << 24U) |
                                     (static_cast<std::uint64_t>(in[offset + 4U]) << 32U) |
                                     (static_cast<std::uint64_t>(in[offset + 5U]) << 40U) |
                                     (static_cast<std::uint64_t>(in[offset + 6U]) << 48U) |
                                     (static_cast<std::uint64_t>(in[offset + 7U]) << 56U));
    offset += 8U;
    return true;
}

}  // namespace

std::string_view ImmutableBlob::view() const noexcept {
    if (owner == nullptr || size == 0U) {
        return {};
    }
    if (offset >= owner->size()) {
        return {};
    }
    const auto available = owner->size() - static_cast<std::size_t>(offset);
    const auto count = std::min<std::size_t>(available, static_cast<std::size_t>(size));
    const auto* base = reinterpret_cast<const char*>(owner->data());
    return std::string_view(base + offset, count);
}

std::string ImmutableBlob::str() const {
    const auto in = view();
    return std::string(in.data(), in.size());
}

std::optional<std::string_view> StateSnapshot::view_of(std::string_view key) const noexcept {
    if (kv == nullptr) {
        return std::nullopt;
    }
    const auto it = kv->find(key);
    if (it == kv->end() || it->second == nullptr) {
        return std::nullopt;
    }
    return it->second->view();
}

std::optional<std::string> StateSnapshot::copy_of(std::string_view key) const {
    const auto value = view_of(key);
    if (!value.has_value()) {
        return std::nullopt;
    }
    return std::string(value->data(), value->size());
}

struct StateStore::BlobArena {
    struct Page {
        std::shared_ptr<std::vector<std::uint8_t>> bytes;
        std::size_t used = 0;
    };

    void configure(std::size_t page_bytes_in, std::size_t max_bytes_in) {
        std::scoped_lock lock(mu);
        page_bytes = std::max<std::size_t>(page_bytes_in, 1024U);
        max_bytes = std::max<std::size_t>(max_bytes_in, page_bytes);
    }

    [[nodiscard]] std::size_t bytes_used() const noexcept {
        std::scoped_lock lock(mu);
        return allocated_bytes;
    }

    [[nodiscard]] std::size_t bytes_limit() const noexcept {
        std::scoped_lock lock(mu);
        return max_bytes;
    }

    [[nodiscard]] std::shared_ptr<const ImmutableBlob> allocate(std::string_view value) {
        if (value.empty()) {
            static const auto kEmptyOwner = std::make_shared<const std::vector<std::uint8_t>>();
            return std::make_shared<const ImmutableBlob>(ImmutableBlob{
                .owner = kEmptyOwner,
                .offset = 0,
                .size = 0,
            });
        }

        const auto needed = value.size();
        std::scoped_lock lock(mu);
        if (needed > max_bytes) {
            return nullptr;
        }

        Page* page = nullptr;
        if (!pages.empty()) {
            auto& tail = pages.back();
            if (tail.bytes != nullptr && tail.used + needed <= tail.bytes->size()) {
                page = &tail;
            }
        }
        if (page == nullptr) {
            const auto alloc_size = std::max<std::size_t>(page_bytes, needed);
            if (allocated_bytes + alloc_size > max_bytes) {
                return nullptr;
            }

            auto bytes = std::make_shared<std::vector<std::uint8_t>>(alloc_size);
            pages.push_back(Page{
                .bytes = std::move(bytes),
                .used = 0,
            });
            allocated_bytes += alloc_size;
            page = &pages.back();
        }

        const auto begin = page->used;
        std::memcpy(page->bytes->data() + begin,
                    value.data(),
                    needed);
        page->used += needed;

        return std::make_shared<const ImmutableBlob>(ImmutableBlob{
            .owner = std::const_pointer_cast<const std::vector<std::uint8_t>>(page->bytes),
            .offset = static_cast<std::uint32_t>(begin),
            .size = static_cast<std::uint32_t>(needed),
        });
    }

    std::size_t page_bytes = kDefaultArenaPageBytes;
    std::size_t max_bytes = kDefaultArenaMaxBytes;
    std::size_t allocated_bytes = 0;
    std::vector<Page> pages;
    mutable std::mutex mu;
};

TickScheduler::TickScheduler(std::uint32_t tick_us, std::uint32_t horizon_ticks)
    : tick_period_us_(std::max(tick_us, 1U)), horizon_ticks_(std::max(horizon_ticks, 1U)) {}

Tick TickScheduler::current_tick() const noexcept {
    return current_tick_.load(std::memory_order_acquire);
}

Tick TickScheduler::next_ingest_tick() const noexcept {
    return current_tick() + static_cast<Tick>(horizon_ticks_);
}

Tick TickScheduler::advance() noexcept {
    return current_tick_.fetch_add(1U, std::memory_order_acq_rel) + 1U;
}

std::uint32_t TickScheduler::tick_period_us() const noexcept {
    return tick_period_us_;
}

std::uint32_t TickScheduler::horizon_ticks() const noexcept {
    return horizon_ticks_;
}

StateStore::StateStore() {
    arena_ = std::make_unique<BlobArena>();
    const auto initial_map = std::make_shared<const SharedStateMap>();
    const auto initial_snapshot = std::make_shared<const StateSnapshot>(StateSnapshot{
        .tick = 0,
        .state_root = compute_state_root(*initial_map),
        .kv = initial_map,
    });
    current_snapshot_.store(initial_snapshot, std::memory_order_release);
}

StateStore::~StateStore() = default;

void StateStore::configure_memory(std::size_t arena_page_bytes, std::size_t arena_max_bytes) {
    if (arena_ == nullptr) {
        arena_ = std::make_unique<BlobArena>();
    }
    arena_->configure(arena_page_bytes, arena_max_bytes);
}

std::shared_ptr<const StateSnapshot> StateStore::current() const {
    return current_snapshot_.load(std::memory_order_acquire);
}

std::shared_ptr<const StateSnapshot> StateStore::apply(Tick tick, const std::vector<Event>& events) {
    const auto previous = current();
    auto next_map = std::make_shared<SharedStateMap>(*previous->kv);
    const auto canonical = canonicalize_events(events);

    for (const auto& event : canonical) {
        if (event.tick != tick) {
            continue;
        }
        if (event.type == EventType::erase) {
            next_map->erase(event.key);
            continue;
        }

        auto value_blob = arena_ != nullptr ? arena_->allocate(event.value) : nullptr;
        if (value_blob == nullptr) {
            arena_rejections_.fetch_add(1U, std::memory_order_relaxed);
            continue;
        }
        next_map->insert_or_assign(event.key, std::move(value_blob));
    }

    auto next_snapshot = std::make_shared<const StateSnapshot>(StateSnapshot{
        .tick = tick,
        .state_root = compute_state_root(*next_map),
        .kv = std::const_pointer_cast<const SharedStateMap>(next_map),
    });
    current_snapshot_.store(next_snapshot, std::memory_order_release);
    return next_snapshot;
}

MemoryStats StateStore::memory_stats() const noexcept {
    MemoryStats stats{};
    stats.arena_rejections = arena_rejections_.load(std::memory_order_relaxed);
    if (arena_ != nullptr) {
        stats.arena_bytes = arena_->bytes_used();
        stats.arena_limit = arena_->bytes_limit();
    }
    return stats;
}

DeterministicKernel::DeterministicKernel(std::uint32_t tick_us, std::uint32_t horizon_ticks)
    : scheduler_(tick_us, horizon_ticks) {
    state_store_.configure_memory(kDefaultArenaPageBytes, kDefaultArenaMaxBytes);
}

void DeterministicKernel::enqueue(Event event) {
    (void)try_enqueue(std::move(event));
}

bool DeterministicKernel::try_enqueue(Event event) {
    std::scoped_lock lock(mutex_);
    if (pending_.size() >= max_pending_events_) {
        dropped_events_.fetch_add(1U, std::memory_order_relaxed);
        return false;
    }
    pending_.push_back(std::move(event));
    return true;
}

void DeterministicKernel::configure_memory(std::size_t arena_page_bytes,
                                           std::size_t arena_max_bytes,
                                           std::size_t max_pending_events) {
    std::scoped_lock lock(mutex_);
    max_pending_events_ = std::max<std::size_t>(max_pending_events, 1U);
    state_store_.configure_memory(arena_page_bytes, arena_max_bytes);
}

CommitResult DeterministicKernel::commit_next_tick_with_events() {
    std::scoped_lock lock(mutex_);
    const Tick commit_tick = scheduler_.advance();
    std::vector<Event> to_commit;
    to_commit.reserve(pending_.size());

    auto next = std::vector<Event>{};
    next.reserve(pending_.size());
    for (auto& event : pending_) {
        if (event.tick <= commit_tick) {
            event.tick = commit_tick;
            to_commit.push_back(std::move(event));
        } else {
            next.push_back(std::move(event));
        }
    }
    pending_ = std::move(next);

    auto snapshot = state_store_.apply(commit_tick, to_commit);
    return CommitResult{
        .snapshot = std::move(snapshot),
        .committed_events = std::move(to_commit),
    };
}

std::shared_ptr<const StateSnapshot> DeterministicKernel::commit_next_tick() {
    return commit_next_tick_with_events().snapshot;
}

std::shared_ptr<const StateSnapshot> DeterministicKernel::latest_snapshot() const {
    return state_store_.current();
}

Tick DeterministicKernel::current_tick() const noexcept {
    return scheduler_.current_tick();
}

Tick DeterministicKernel::next_ingest_tick() const noexcept {
    return scheduler_.next_ingest_tick();
}

MemoryStats DeterministicKernel::memory_stats() const {
    MemoryStats stats = state_store_.memory_stats();
    std::scoped_lock lock(mutex_);
    stats.pending_events = pending_.size();
    stats.pending_limit = max_pending_events_;
    stats.dropped_events = dropped_events_.load(std::memory_order_relaxed);
    return stats;
}

std::vector<Event> canonicalize_events(std::vector<Event> events) {
    std::stable_sort(events.begin(), events.end(), [](const Event& lhs, const Event& rhs) {
        if (lhs.tick != rhs.tick) {
            return lhs.tick < rhs.tick;
        }
        if (lhs.node_id != rhs.node_id) {
            return lhs.node_id < rhs.node_id;
        }
        if (lhs.origin_seq != rhs.origin_seq) {
            return lhs.origin_seq < rhs.origin_seq;
        }
        if (lhs.type != rhs.type) {
            return static_cast<std::uint16_t>(lhs.type) < static_cast<std::uint16_t>(rhs.type);
        }

        const auto lhs_hash = payload_hash64(lhs);
        const auto rhs_hash = payload_hash64(rhs);
        if (lhs_hash != rhs_hash) {
            return lhs_hash < rhs_hash;
        }
        if (lhs.key != rhs.key) {
            return lhs.key < rhs.key;
        }
        return lhs.value < rhs.value;
    });
    return events;
}

std::uint64_t payload_hash64(const Event& event) noexcept {
    std::uint64_t hash = kFnvOffset64;
    hash_u64(hash, event.tick);
    hash_u64(hash, event.node_id.hi);
    hash_u64(hash, event.node_id.lo);
    hash_u64(hash, event.origin_seq);
    hash_u32(hash, static_cast<std::uint16_t>(event.type));
    hash_u32(hash, static_cast<std::uint32_t>(event.key.size()));
    hash_bytes(hash,
               reinterpret_cast<const std::uint8_t*>(event.key.data()),
               static_cast<std::size_t>(event.key.size()));
    hash_u32(hash, static_cast<std::uint32_t>(event.value.size()));
    hash_bytes(hash,
               reinterpret_cast<const std::uint8_t*>(event.value.data()),
               static_cast<std::size_t>(event.value.size()));
    return hash;
}

std::uint64_t compute_state_root(const SharedStateMap& state) noexcept {
    std::uint64_t hash = kFnvOffset64;
    for (const auto& [key, blob] : state) {
        hash_u32(hash, static_cast<std::uint32_t>(key.size()));
        hash_bytes(hash,
                   reinterpret_cast<const std::uint8_t*>(key.data()),
                   static_cast<std::size_t>(key.size()));
        const auto value = blob != nullptr ? blob->view() : std::string_view{};
        hash_u32(hash, static_cast<std::uint32_t>(value.size()));
        hash_bytes(hash,
                   reinterpret_cast<const std::uint8_t*>(value.data()),
                   static_cast<std::size_t>(value.size()));
    }
    return hash;
}

std::uint32_t crc32c(const std::uint8_t* data, std::size_t size) noexcept {
    std::uint32_t crc = 0xFFFFFFFFU;
    for (std::size_t i = 0; i < size; ++i) {
        crc ^= static_cast<std::uint32_t>(data[i]);
        for (int bit = 0; bit < 8; ++bit) {
            const std::uint32_t mask = (crc & 1U) == 0U ? 0U : 0xFFFFFFFFU;
            crc = (crc >> 1U) ^ (kCrc32cPolynomial & mask);
        }
    }
    return ~crc;
}

std::vector<std::uint8_t> serialize_event(const Event& event) {
    if (event.key.size() > std::numeric_limits<std::uint32_t>::max() ||
        event.value.size() > std::numeric_limits<std::uint32_t>::max()) {
        return {};
    }

    std::vector<std::uint8_t> payload;
    payload.reserve(8U + 8U + 8U + 8U + 2U + 2U + 4U + 4U + event.key.size() + event.value.size());
    append_u64_le(payload, event.tick);
    append_u64_le(payload, event.node_id.hi);
    append_u64_le(payload, event.node_id.lo);
    append_u64_le(payload, event.origin_seq);
    append_u16_le(payload, static_cast<std::uint16_t>(event.type));
    append_u16_le(payload, 0U);
    append_u32_le(payload, static_cast<std::uint32_t>(event.key.size()));
    append_u32_le(payload, static_cast<std::uint32_t>(event.value.size()));
    payload.insert(payload.end(), event.key.begin(), event.key.end());
    payload.insert(payload.end(), event.value.begin(), event.value.end());

    const auto payload_crc =
        crc32c(payload.data(), static_cast<std::size_t>(payload.size()));

    std::vector<std::uint8_t> frame;
    frame.reserve(kHeaderSize + payload.size());
    append_u32_le(frame, kFrameMagic);
    append_u16_le(frame, kSchemaVersion);
    append_u16_le(frame, kPayloadTypeEvent);
    append_u32_le(frame, static_cast<std::uint32_t>(payload.size()));
    append_u32_le(frame, payload_crc);
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

bool read_header(const std::vector<std::uint8_t>& frame, SerializationHeader& out_header) {
    if (frame.size() < kHeaderSize) {
        return false;
    }

    std::size_t offset = 0;
    if (!read_u32_le(frame, offset, out_header.magic)) {
        return false;
    }
    if (!read_u16_le(frame, offset, out_header.schema_version)) {
        return false;
    }
    if (!read_u16_le(frame, offset, out_header.payload_type)) {
        return false;
    }
    if (!read_u32_le(frame, offset, out_header.payload_size)) {
        return false;
    }
    if (!read_u32_le(frame, offset, out_header.payload_crc32c)) {
        return false;
    }
    return true;
}

bool deserialize_event(const std::vector<std::uint8_t>& frame, Event& out_event) {
    SerializationHeader header{};
    if (!read_header(frame, header)) {
        return false;
    }
    if (header.magic != kFrameMagic || header.schema_version != kSchemaVersion ||
        header.payload_type != kPayloadTypeEvent) {
        return false;
    }

    const auto payload_size = static_cast<std::size_t>(header.payload_size);
    if (frame.size() != kHeaderSize + payload_size) {
        return false;
    }

    const std::uint8_t* payload_ptr = frame.data() + kHeaderSize;
    const auto actual_crc = crc32c(payload_ptr, payload_size);
    if (actual_crc != header.payload_crc32c) {
        return false;
    }

    std::size_t offset = kHeaderSize;
    std::uint16_t event_type = 0;
    std::uint16_t reserved = 0;
    std::uint32_t key_len = 0;
    std::uint32_t value_len = 0;

    if (!read_u64_le(frame, offset, out_event.tick)) {
        return false;
    }
    if (!read_u64_le(frame, offset, out_event.node_id.hi)) {
        return false;
    }
    if (!read_u64_le(frame, offset, out_event.node_id.lo)) {
        return false;
    }
    if (!read_u64_le(frame, offset, out_event.origin_seq)) {
        return false;
    }
    if (!read_u16_le(frame, offset, event_type)) {
        return false;
    }
    if (!read_u16_le(frame, offset, reserved)) {
        return false;
    }
    if (reserved != 0U) {
        return false;
    }
    if (!read_u32_le(frame, offset, key_len)) {
        return false;
    }
    if (!read_u32_le(frame, offset, value_len)) {
        return false;
    }

    const auto remaining = frame.size() - offset;
    const auto expected_remaining = static_cast<std::size_t>(key_len) + static_cast<std::size_t>(value_len);
    if (remaining != expected_remaining) {
        return false;
    }

    out_event.type = static_cast<EventType>(event_type);
    if (out_event.type != EventType::upsert && out_event.type != EventType::erase) {
        return false;
    }

    out_event.key.assign(reinterpret_cast<const char*>(frame.data() + offset), key_len);
    offset += static_cast<std::size_t>(key_len);
    out_event.value.assign(reinterpret_cast<const char*>(frame.data() + offset), value_len);
    return true;
}

}  // namespace swarmnet::core
