#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace swarmnet::transport {

struct Endpoint {
    std::string host;
    std::uint16_t port = 0;

    bool operator==(const Endpoint&) const = default;
};

enum class PacketKind : std::uint16_t {
    hello = 1,
    hello_ack = 2,
    unreliable = 3,
    reliable = 4,
    ack = 5,
};

struct Packet {
    PacketKind kind = PacketKind::unreliable;
    Endpoint from{};
    std::uint64_t node_id = 0;
    std::uint32_t stream_id = 0;
    std::uint64_t sequence = 0;
    std::vector<std::uint8_t> payload;
};

using PacketHandler = std::function<void(const Packet&)>;

class ITransportBackend {
public:
    virtual ~ITransportBackend() = default;

    [[nodiscard]] virtual std::string backend_name() const = 0;

    virtual bool bind(const Endpoint& local, std::string& error) = 0;
    virtual bool send_hello(const Endpoint& to, std::uint64_t node_id, std::string& error) = 0;
    virtual bool send_hello_ack(const Endpoint& to, std::uint64_t node_id, std::string& error) = 0;
    virtual bool send_unreliable(const Endpoint& to,
                                 std::span<const std::uint8_t> payload,
                                 std::string& error) = 0;
    virtual bool send_reliable(const Endpoint& to,
                               std::uint32_t stream_id,
                               std::span<const std::uint8_t> payload,
                               std::uint64_t& out_sequence,
                               std::string& error) = 0;

    virtual void poll(std::chrono::milliseconds budget, const PacketHandler& handler) = 0;
    virtual void maintenance() = 0;
    [[nodiscard]] virtual std::size_t pending_reliable_count() const = 0;
};

[[nodiscard]] bool parse_endpoint(std::string_view text, Endpoint& out);
[[nodiscard]] std::string endpoint_to_string(const Endpoint& ep);

[[nodiscard]] std::unique_ptr<ITransportBackend> create_default_backend();
[[nodiscard]] std::unique_ptr<ITransportBackend> create_backend(std::string_view backend_name);

}  // namespace swarmnet::transport
