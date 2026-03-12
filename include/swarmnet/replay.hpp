#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "swarmnet/core.hpp"

namespace swarmnet::replay {

enum class Mode : std::uint8_t {
    off = 0,
    record = 1,
    playback = 2,
};

struct TickRecord {
    core::Tick tick = 0;
    std::uint64_t state_root = 0;
    std::vector<core::Event> events;
};

struct ExternalRecord {
    std::uint64_t sequence = 0;
    std::string name;
    std::string value;
};

class Journal {
public:
    Journal(Mode mode, std::string path);
    ~Journal();

    Journal(const Journal&) = delete;
    Journal& operator=(const Journal&) = delete;
    Journal(Journal&&) = delete;
    Journal& operator=(Journal&&) = delete;

    [[nodiscard]] Mode mode() const noexcept;
    [[nodiscard]] bool enabled() const noexcept;

    void append_tick(const TickRecord& record);
    void append_external(const ExternalRecord& record);

    [[nodiscard]] bool read_next_tick(TickRecord& out);
    [[nodiscard]] std::optional<std::string> read_external(std::uint64_t sequence,
                                                           std::string_view name) const;
    [[nodiscard]] std::size_t tick_count() const noexcept;
    [[nodiscard]] std::size_t tick_index() const noexcept;

    void flush();

private:
    void write_file_header();
    void parse_file();

    Mode mode_{Mode::off};
    std::string path_;

    std::vector<TickRecord> tick_records_;
    std::unordered_map<std::uint64_t, ExternalRecord> external_by_sequence_;
    std::size_t next_tick_index_ = 0;

    struct Impl;
    Impl* impl_ = nullptr;
};

}  // namespace swarmnet::replay
