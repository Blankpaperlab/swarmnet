#include "swarmnet/replay.hpp"

#include <array>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace swarmnet::replay {

namespace {

constexpr std::uint32_t kReplayMagic = 0x31504C52U;  // RLP1
constexpr std::uint16_t kReplaySchemaVersion = 1U;
constexpr std::uint16_t kRecordTypeTick = 1U;
constexpr std::uint16_t kRecordTypeExternal = 2U;
constexpr std::size_t kFileHeaderSize = 8U;
constexpr std::size_t kRecordHeaderSize = 12U;

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

void require(bool value, const char* message) {
    if (!value) {
        throw std::runtime_error(message);
    }
}

std::vector<std::uint8_t> encode_tick_payload(const TickRecord& record) {
    if (record.events.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("too many events in tick record");
    }

    std::vector<std::uint8_t> out;
    out.reserve(8U + 8U + 4U + (record.events.size() * 48U));
    append_u64_le(out, record.tick);
    append_u64_le(out, record.state_root);
    append_u32_le(out, static_cast<std::uint32_t>(record.events.size()));
    for (const auto& event : record.events) {
        const auto frame = core::serialize_event(event);
        if (frame.size() > std::numeric_limits<std::uint32_t>::max()) {
            throw std::runtime_error("event frame too large");
        }
        append_u32_le(out, static_cast<std::uint32_t>(frame.size()));
        out.insert(out.end(), frame.begin(), frame.end());
    }
    return out;
}

std::vector<std::uint8_t> encode_external_payload(const ExternalRecord& record) {
    if (record.name.size() > std::numeric_limits<std::uint32_t>::max() ||
        record.value.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("external payload too large");
    }

    std::vector<std::uint8_t> out;
    out.reserve(8U + 4U + 4U + record.name.size() + record.value.size());
    append_u64_le(out, record.sequence);
    append_u32_le(out, static_cast<std::uint32_t>(record.name.size()));
    append_u32_le(out, static_cast<std::uint32_t>(record.value.size()));
    out.insert(out.end(), record.name.begin(), record.name.end());
    out.insert(out.end(), record.value.begin(), record.value.end());
    return out;
}

bool decode_tick_payload(const std::vector<std::uint8_t>& payload, TickRecord& out) {
    std::size_t offset = 0;
    std::uint32_t event_count = 0;
    if (!read_u64_le(payload, offset, out.tick)) {
        return false;
    }
    if (!read_u64_le(payload, offset, out.state_root)) {
        return false;
    }
    if (!read_u32_le(payload, offset, event_count)) {
        return false;
    }

    out.events.clear();
    out.events.reserve(event_count);
    for (std::uint32_t i = 0; i < event_count; ++i) {
        std::uint32_t frame_size = 0;
        if (!read_u32_le(payload, offset, frame_size)) {
            return false;
        }
        const auto size = static_cast<std::size_t>(frame_size);
        if (offset + size > payload.size()) {
            return false;
        }
        std::vector<std::uint8_t> frame;
        frame.insert(frame.end(), payload.begin() + static_cast<std::ptrdiff_t>(offset),
                     payload.begin() + static_cast<std::ptrdiff_t>(offset + size));
        core::Event event{};
        if (!core::deserialize_event(frame, event)) {
            return false;
        }
        out.events.push_back(std::move(event));
        offset += size;
    }

    return offset == payload.size();
}

bool decode_external_payload(const std::vector<std::uint8_t>& payload, ExternalRecord& out) {
    std::size_t offset = 0;
    std::uint32_t name_len = 0;
    std::uint32_t value_len = 0;
    if (!read_u64_le(payload, offset, out.sequence)) {
        return false;
    }
    if (!read_u32_le(payload, offset, name_len)) {
        return false;
    }
    if (!read_u32_le(payload, offset, value_len)) {
        return false;
    }

    const auto remaining = payload.size() - offset;
    const auto expected = static_cast<std::size_t>(name_len) + static_cast<std::size_t>(value_len);
    if (remaining != expected) {
        return false;
    }

    out.name.assign(reinterpret_cast<const char*>(payload.data() + offset), name_len);
    offset += static_cast<std::size_t>(name_len);
    out.value.assign(reinterpret_cast<const char*>(payload.data() + offset), value_len);
    return true;
}

}  // namespace

struct Journal::Impl {
    std::ofstream writer;
};

Journal::Journal(Mode mode, std::string path) : mode_(mode), path_(std::move(path)) {
    if (mode_ == Mode::off) {
        return;
    }
    if (path_.empty()) {
        throw std::runtime_error("replay mode requires replay_path");
    }

    if (mode_ == Mode::record) {
        impl_ = new Impl();
        impl_->writer.open(path_, std::ios::binary | std::ios::trunc);
        if (!impl_->writer.is_open()) {
            throw std::runtime_error("failed to open replay journal for writing");
        }
        write_file_header();
        return;
    }

    parse_file();
}

Journal::~Journal() {
    flush();
    delete impl_;
}

Mode Journal::mode() const noexcept {
    return mode_;
}

bool Journal::enabled() const noexcept {
    return mode_ != Mode::off;
}

void Journal::append_tick(const TickRecord& record) {
    if (mode_ != Mode::record) {
        return;
    }
    require(impl_ != nullptr, "journal writer is not initialized");

    auto payload = encode_tick_payload(record);
    if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("tick payload too large");
    }

    std::vector<std::uint8_t> header;
    header.reserve(kRecordHeaderSize);
    append_u16_le(header, kRecordTypeTick);
    append_u16_le(header, 0U);
    append_u32_le(header, static_cast<std::uint32_t>(payload.size()));
    append_u32_le(header, core::crc32c(payload.data(), payload.size()));

    impl_->writer.write(reinterpret_cast<const char*>(header.data()),
                        static_cast<std::streamsize>(header.size()));
    impl_->writer.write(reinterpret_cast<const char*>(payload.data()),
                        static_cast<std::streamsize>(payload.size()));
    if (!impl_->writer.good()) {
        throw std::runtime_error("failed to write tick record");
    }
}

void Journal::append_external(const ExternalRecord& record) {
    if (mode_ != Mode::record) {
        return;
    }
    require(impl_ != nullptr, "journal writer is not initialized");

    auto payload = encode_external_payload(record);
    if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("external payload too large");
    }

    std::vector<std::uint8_t> header;
    header.reserve(kRecordHeaderSize);
    append_u16_le(header, kRecordTypeExternal);
    append_u16_le(header, 0U);
    append_u32_le(header, static_cast<std::uint32_t>(payload.size()));
    append_u32_le(header, core::crc32c(payload.data(), payload.size()));

    impl_->writer.write(reinterpret_cast<const char*>(header.data()),
                        static_cast<std::streamsize>(header.size()));
    impl_->writer.write(reinterpret_cast<const char*>(payload.data()),
                        static_cast<std::streamsize>(payload.size()));
    if (!impl_->writer.good()) {
        throw std::runtime_error("failed to write external record");
    }
}

bool Journal::read_next_tick(TickRecord& out) {
    if (mode_ != Mode::playback) {
        return false;
    }
    if (next_tick_index_ >= tick_records_.size()) {
        return false;
    }
    out = tick_records_[next_tick_index_];
    ++next_tick_index_;
    return true;
}

std::optional<std::string> Journal::read_external(std::uint64_t sequence, std::string_view name) const {
    if (mode_ != Mode::playback) {
        return std::nullopt;
    }
    const auto it = external_by_sequence_.find(sequence);
    if (it == external_by_sequence_.end()) {
        return std::nullopt;
    }
    if (it->second.name != name) {
        return std::nullopt;
    }
    return it->second.value;
}

std::size_t Journal::tick_count() const noexcept {
    return tick_records_.size();
}

std::size_t Journal::tick_index() const noexcept {
    return next_tick_index_;
}

void Journal::flush() {
    if (mode_ == Mode::record && impl_ != nullptr && impl_->writer.is_open()) {
        impl_->writer.flush();
    }
}

void Journal::write_file_header() {
    require(mode_ == Mode::record, "write_file_header requires record mode");
    require(impl_ != nullptr, "journal writer is not initialized");

    std::vector<std::uint8_t> header;
    header.reserve(kFileHeaderSize);
    append_u32_le(header, kReplayMagic);
    append_u16_le(header, kReplaySchemaVersion);
    append_u16_le(header, 0U);
    impl_->writer.write(reinterpret_cast<const char*>(header.data()),
                        static_cast<std::streamsize>(header.size()));
    if (!impl_->writer.good()) {
        throw std::runtime_error("failed to write replay header");
    }
}

void Journal::parse_file() {
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open replay journal for playback");
    }

    std::vector<std::uint8_t> file_header(kFileHeaderSize);
    in.read(reinterpret_cast<char*>(file_header.data()), static_cast<std::streamsize>(file_header.size()));
    if (in.gcount() != static_cast<std::streamsize>(file_header.size())) {
        throw std::runtime_error("replay file header is truncated");
    }

    std::size_t header_offset = 0;
    std::uint32_t magic = 0;
    std::uint16_t schema = 0;
    std::uint16_t reserved = 0;
    require(read_u32_le(file_header, header_offset, magic), "failed to parse replay header magic");
    require(read_u16_le(file_header, header_offset, schema), "failed to parse replay header schema");
    require(read_u16_le(file_header, header_offset, reserved), "failed to parse replay header reserved");
    require(magic == kReplayMagic, "invalid replay magic");
    require(schema == kReplaySchemaVersion, "unsupported replay schema version");
    require(reserved == 0U, "invalid replay reserved bits");

    while (true) {
        std::vector<std::uint8_t> rec_header(kRecordHeaderSize);
        in.read(reinterpret_cast<char*>(rec_header.data()), static_cast<std::streamsize>(rec_header.size()));
        const auto read = in.gcount();
        if (read == 0) {
            break;
        }
        if (read != static_cast<std::streamsize>(rec_header.size())) {
            throw std::runtime_error("replay record header truncated");
        }

        std::size_t offset = 0;
        std::uint16_t rec_type = 0;
        std::uint16_t rec_reserved = 0;
        std::uint32_t rec_size = 0;
        std::uint32_t rec_crc = 0;
        require(read_u16_le(rec_header, offset, rec_type), "failed to parse record type");
        require(read_u16_le(rec_header, offset, rec_reserved), "failed to parse record reserved");
        require(read_u32_le(rec_header, offset, rec_size), "failed to parse record size");
        require(read_u32_le(rec_header, offset, rec_crc), "failed to parse record crc");
        require(rec_reserved == 0U, "record reserved must be zero");

        std::vector<std::uint8_t> payload(static_cast<std::size_t>(rec_size));
        in.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
        if (in.gcount() != static_cast<std::streamsize>(payload.size())) {
            throw std::runtime_error("replay payload truncated");
        }
        const auto actual_crc = core::crc32c(payload.data(), payload.size());
        require(actual_crc == rec_crc, "replay payload crc mismatch");

        if (rec_type == kRecordTypeTick) {
            TickRecord record{};
            require(decode_tick_payload(payload, record), "failed to decode tick record");
            tick_records_.push_back(std::move(record));
            continue;
        }

        if (rec_type == kRecordTypeExternal) {
            ExternalRecord record{};
            require(decode_external_payload(payload, record), "failed to decode external record");
            external_by_sequence_[record.sequence] = std::move(record);
            continue;
        }

        throw std::runtime_error("unknown replay record type");
    }
}

}  // namespace swarmnet::replay
