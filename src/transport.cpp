#include "swarmnet/transport.hpp"

#include "swarmnet/core.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace swarmnet::transport {

namespace {

constexpr std::uint32_t kMagic = 0x31544E53U;  // SNT1
constexpr std::uint16_t kVersion = 1U;
constexpr std::size_t kHeaderSize = 36U;
constexpr auto kRetransmitDelay = std::chrono::milliseconds(20);

#if defined(_WIN32)
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

std::string last_socket_error() {
#if defined(_WIN32)
    return "winsock error " + std::to_string(WSAGetLastError());
#else
    return std::string("socket error ") + std::strerror(errno);
#endif
}

bool socket_would_block() {
#if defined(_WIN32)
    const auto err = WSAGetLastError();
    return err == WSAEWOULDBLOCK;
#else
    return errno == EWOULDBLOCK || errno == EAGAIN;
#endif
}

void close_socket(SocketHandle sock) {
    if (sock == kInvalidSocket) {
        return;
    }
#if defined(_WIN32)
    closesocket(sock);
#else
    close(sock);
#endif
}

bool set_nonblocking(SocketHandle sock, std::string& error) {
#if defined(_WIN32)
    u_long mode = 1;
    const long cmd = static_cast<long>(FIONBIO);
    if (ioctlsocket(sock, cmd, &mode) != 0) {
        error = "ioctlsocket(FIONBIO) failed: " + last_socket_error();
        return false;
    }
#else
    const auto flags = fcntl(sock, F_GETFL, 0);
    if (flags < 0) {
        error = "fcntl(F_GETFL) failed: " + last_socket_error();
        return false;
    }
    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
        error = "fcntl(F_SETFL) failed: " + last_socket_error();
        return false;
    }
#endif
    return true;
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

std::string key_for_stream(const Endpoint& ep, std::uint32_t stream_id) {
    return endpoint_to_string(ep) + "|" + std::to_string(stream_id);
}

std::string key_for_pending(const Endpoint& ep, std::uint32_t stream_id, std::uint64_t sequence) {
    return endpoint_to_string(ep) + "|" + std::to_string(stream_id) + "|" + std::to_string(sequence);
}

bool make_sockaddr(const Endpoint& endpoint, sockaddr_in& out, std::string& error) {
    std::memset(&out, 0, sizeof(out));
    out.sin_family = AF_INET;
    out.sin_port = htons(endpoint.port);

    const std::string host = (endpoint.host == "localhost") ? "127.0.0.1" : endpoint.host;
    if (host == "*" || host == "0.0.0.0") {
        out.sin_addr.s_addr = htonl(INADDR_ANY);
        return true;
    }

    if (inet_pton(AF_INET, host.c_str(), &out.sin_addr) != 1) {
        error = "invalid IPv4 address: " + endpoint.host;
        return false;
    }
    return true;
}

Endpoint endpoint_from_sockaddr(const sockaddr_in& in) {
    char buffer[INET_ADDRSTRLEN] = {0};
    const char* text = inet_ntop(AF_INET, &in.sin_addr, buffer, static_cast<socklen_t>(sizeof(buffer)));
    Endpoint out{};
    out.host = text == nullptr ? "0.0.0.0" : std::string(text);
    out.port = ntohs(in.sin_port);
    return out;
}

struct PendingReliable {
    Endpoint to{};
    std::uint32_t stream_id = 0;
    std::uint64_t sequence = 0;
    std::vector<std::uint8_t> frame;
    std::chrono::steady_clock::time_point last_send{};
};

struct ReceiveOrderState {
    std::uint64_t expected_sequence = 1U;
    std::map<std::uint64_t, Packet> buffered;
};

class DatagramTransportBackend : public ITransportBackend {
public:
    explicit DatagramTransportBackend(std::string backend_name) : backend_name_(std::move(backend_name)) {
#if defined(_WIN32)
        static bool wsa_initialized = false;
        if (!wsa_initialized) {
            WSADATA wsa{};
            if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
                throw std::runtime_error("WSAStartup failed");
            }
            wsa_initialized = true;
        }
#endif
    }

    ~DatagramTransportBackend() override {
        close_socket(socket_);
    }

    [[nodiscard]] std::string backend_name() const override {
        return backend_name_;
    }

    bool bind(const Endpoint& local, std::string& error) override {
        close_socket(socket_);
        socket_ = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (socket_ == kInvalidSocket) {
            error = "socket() failed: " + last_socket_error();
            return false;
        }

        int reuse = 1;
#if defined(_WIN32)
        if (setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse)) != 0) {
#else
        if (setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
#endif
            error = "setsockopt(SO_REUSEADDR) failed: " + last_socket_error();
            close_socket(socket_);
            socket_ = kInvalidSocket;
            return false;
        }

        if (!set_nonblocking(socket_, error)) {
            close_socket(socket_);
            socket_ = kInvalidSocket;
            return false;
        }

        sockaddr_in addr{};
        if (!make_sockaddr(local, addr, error)) {
            close_socket(socket_);
            socket_ = kInvalidSocket;
            return false;
        }

        if (::bind(socket_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
            error = "bind() failed: " + last_socket_error();
            close_socket(socket_);
            socket_ = kInvalidSocket;
            return false;
        }
        local_endpoint_ = local;
        return true;
    }

    bool send_hello(const Endpoint& to, std::uint64_t node_id, std::string& error) override {
        return send_frame(to, PacketKind::hello, 0U, 0U, node_id, {}, false, error);
    }

    bool send_hello_ack(const Endpoint& to, std::uint64_t node_id, std::string& error) override {
        return send_frame(to, PacketKind::hello_ack, 0U, 0U, node_id, {}, false, error);
    }

    bool send_unreliable(const Endpoint& to,
                         std::span<const std::uint8_t> payload,
                         std::string& error) override {
        return send_frame(to, PacketKind::unreliable, 0U, 0U, 0U, payload, false, error);
    }

    bool send_reliable(const Endpoint& to,
                       std::uint32_t stream_id,
                       std::span<const std::uint8_t> payload,
                       std::uint64_t& out_sequence,
                       std::string& error) override {
        const auto stream_key = key_for_stream(to, stream_id);
        const auto it = next_sequence_by_stream_.find(stream_key);
        std::uint64_t next = 1U;
        if (it != next_sequence_by_stream_.end()) {
            next = it->second;
        }
        if (!send_frame(to, PacketKind::reliable, stream_id, next, 0U, payload, true, error)) {
            out_sequence = 0U;
            return false;
        }
        next_sequence_by_stream_[stream_key] = next + 1U;
        out_sequence = next;
        return true;
    }

    void poll(std::chrono::milliseconds budget, const PacketHandler& handler) override {
        if (socket_ == kInvalidSocket) {
            return;
        }
        const auto deadline = std::chrono::steady_clock::now() + budget;

        while (std::chrono::steady_clock::now() < deadline) {
            bool had_packet = false;

            while (true) {
                std::array<std::uint8_t, 65536> buffer{};
                sockaddr_in from_addr{};
#if defined(_WIN32)
                int from_len = static_cast<int>(sizeof(from_addr));
                const int read =
                    recvfrom(socket_,
                             reinterpret_cast<char*>(buffer.data()),
                             static_cast<int>(buffer.size()),
                             0,
                             reinterpret_cast<sockaddr*>(&from_addr),
                             &from_len);
#else
                socklen_t from_len = static_cast<socklen_t>(sizeof(from_addr));
                const ssize_t read =
                    recvfrom(socket_,
                             buffer.data(),
                             buffer.size(),
                             0,
                             reinterpret_cast<sockaddr*>(&from_addr),
                             &from_len);
#endif

                if (read < 0) {
                    if (socket_would_block()) {
                        break;
                    }
                    break;
                }
                if (read == 0) {
                    break;
                }

                had_packet = true;
                const auto packet_bytes = static_cast<std::size_t>(read);
                std::vector<std::uint8_t> frame;
                frame.insert(frame.end(), buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(packet_bytes));
                Packet packet{};
                packet.from = endpoint_from_sockaddr(from_addr);

                if (!decode_frame(frame, packet)) {
                    continue;
                }

                if (packet.kind == PacketKind::ack) {
                    const auto key = key_for_pending(packet.from, packet.stream_id, packet.sequence);
                    pending_.erase(std::remove_if(pending_.begin(),
                                                  pending_.end(),
                                                  [&key](const PendingReliable& item) {
                                                      return key_for_pending(item.to, item.stream_id, item.sequence) == key;
                                                  }),
                                   pending_.end());
                    continue;
                }

                if (packet.kind == PacketKind::reliable) {
                    std::string unused_error;
                    (void)send_ack(packet.from, packet.stream_id, packet.sequence, unused_error);

                    const auto stream_key = key_for_stream(packet.from, packet.stream_id);
                    auto& rx_state = receive_state_by_stream_[stream_key];

                    if (packet.sequence < rx_state.expected_sequence) {
                        continue;
                    }

                    if (packet.sequence > rx_state.expected_sequence) {
                        (void)rx_state.buffered.emplace(packet.sequence, std::move(packet));
                        continue;
                    }

                    handler(packet);
                    rx_state.expected_sequence += 1U;
                    while (true) {
                        const auto it = rx_state.buffered.find(rx_state.expected_sequence);
                        if (it == rx_state.buffered.end()) {
                            break;
                        }
                        handler(it->second);
                        rx_state.buffered.erase(it);
                        rx_state.expected_sequence += 1U;
                    }
                    continue;
                }

                handler(packet);
            }

            maintenance();
            if (!had_packet) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    void maintenance() override {
        if (socket_ == kInvalidSocket) {
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        for (auto& item : pending_) {
            if (now - item.last_send < kRetransmitDelay) {
                continue;
            }
            std::string error;
            if (!send_raw(item.to, item.frame, error)) {
                continue;
            }
            item.last_send = now;
        }
    }

    [[nodiscard]] std::size_t pending_reliable_count() const override {
        return pending_.size();
    }

private:
    bool send_ack(const Endpoint& to,
                  std::uint32_t stream_id,
                  std::uint64_t sequence,
                  std::string& error) {
        return send_frame(to, PacketKind::ack, stream_id, sequence, 0U, {}, false, error);
    }

    bool send_frame(const Endpoint& to,
                    PacketKind kind,
                    std::uint32_t stream_id,
                    std::uint64_t sequence,
                    std::uint64_t node_id,
                    std::span<const std::uint8_t> payload,
                    bool track_reliable,
                    std::string& error) {
        if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
            error = "payload too large";
            return false;
        }

        std::vector<std::uint8_t> frame;
        frame.reserve(kHeaderSize + payload.size());
        append_u32_le(frame, kMagic);
        append_u16_le(frame, kVersion);
        append_u16_le(frame, static_cast<std::uint16_t>(kind));
        append_u32_le(frame, stream_id);
        append_u64_le(frame, sequence);
        append_u64_le(frame, node_id);
        append_u32_le(frame, static_cast<std::uint32_t>(payload.size()));
        append_u32_le(frame, core::crc32c(payload.data(), payload.size()));
        frame.insert(frame.end(), payload.begin(), payload.end());

        if (!send_raw(to, frame, error)) {
            return false;
        }

        if (track_reliable) {
            pending_.push_back(PendingReliable{
                .to = to,
                .stream_id = stream_id,
                .sequence = sequence,
                .frame = frame,
                .last_send = std::chrono::steady_clock::now(),
            });
        }
        return true;
    }

    bool send_raw(const Endpoint& to, const std::vector<std::uint8_t>& frame, std::string& error) {
        if (socket_ == kInvalidSocket) {
            error = "socket is not bound";
            return false;
        }

        sockaddr_in to_addr{};
        if (!make_sockaddr(to, to_addr, error)) {
            return false;
        }

#if defined(_WIN32)
        const int sent = sendto(socket_,
                                reinterpret_cast<const char*>(frame.data()),
                                static_cast<int>(frame.size()),
                                0,
                                reinterpret_cast<const sockaddr*>(&to_addr),
                                static_cast<int>(sizeof(to_addr)));
#else
        const ssize_t sent = sendto(socket_,
                                    frame.data(),
                                    frame.size(),
                                    0,
                                    reinterpret_cast<const sockaddr*>(&to_addr),
                                    static_cast<socklen_t>(sizeof(to_addr)));
#endif

        if (sent < 0) {
            error = "sendto() failed: " + last_socket_error();
            return false;
        }

        if (static_cast<std::size_t>(sent) != frame.size()) {
            error = "sendto() short write";
            return false;
        }
        return true;
    }

    bool decode_frame(const std::vector<std::uint8_t>& frame, Packet& out) const {
        if (frame.size() < kHeaderSize) {
            return false;
        }

        std::size_t offset = 0;
        std::uint32_t magic = 0;
        std::uint16_t version = 0;
        std::uint16_t kind = 0;
        std::uint32_t stream_id = 0;
        std::uint64_t sequence = 0;
        std::uint64_t node_id = 0;
        std::uint32_t payload_size = 0;
        std::uint32_t payload_crc = 0;

        if (!read_u32_le(frame, offset, magic) || !read_u16_le(frame, offset, version) ||
            !read_u16_le(frame, offset, kind) || !read_u32_le(frame, offset, stream_id) ||
            !read_u64_le(frame, offset, sequence) || !read_u64_le(frame, offset, node_id) ||
            !read_u32_le(frame, offset, payload_size) || !read_u32_le(frame, offset, payload_crc)) {
            return false;
        }

        if (magic != kMagic || version != kVersion) {
            return false;
        }
        const auto expected_size = kHeaderSize + static_cast<std::size_t>(payload_size);
        if (frame.size() != expected_size) {
            return false;
        }

        const auto* payload_ptr = frame.data() + static_cast<std::ptrdiff_t>(kHeaderSize);
        const auto computed_crc = core::crc32c(payload_ptr, static_cast<std::size_t>(payload_size));
        if (computed_crc != payload_crc) {
            return false;
        }

        out.kind = static_cast<PacketKind>(kind);
        out.stream_id = stream_id;
        out.sequence = sequence;
        out.node_id = node_id;
        out.payload.assign(payload_ptr, payload_ptr + static_cast<std::ptrdiff_t>(payload_size));
        return true;
    }

    std::string backend_name_;
    SocketHandle socket_ = kInvalidSocket;
    Endpoint local_endpoint_{};
    std::unordered_map<std::string, std::uint64_t> next_sequence_by_stream_;
    std::unordered_map<std::string, ReceiveOrderState> receive_state_by_stream_;
    std::vector<PendingReliable> pending_;
};

class PollTransportBackend final : public DatagramTransportBackend {
public:
    PollTransportBackend() : DatagramTransportBackend("poll") {}
};

class EpollTransportBackend final : public DatagramTransportBackend {
public:
    EpollTransportBackend() : DatagramTransportBackend("epoll") {}
};

class KqueueTransportBackend final : public DatagramTransportBackend {
public:
    KqueueTransportBackend() : DatagramTransportBackend("kqueue") {}
};

class IocpTransportBackend final : public DatagramTransportBackend {
public:
    IocpTransportBackend() : DatagramTransportBackend("iocp") {}
};

}  // namespace

bool parse_endpoint(std::string_view text, Endpoint& out) {
    const auto pos = text.rfind(':');
    if (pos == std::string_view::npos || pos == 0U || pos + 1U >= text.size()) {
        return false;
    }

    const auto host = text.substr(0, pos);
    const auto port_text = text.substr(pos + 1U);
    std::uint64_t parsed = 0;
    for (const char ch : port_text) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        parsed = (parsed * 10U) + static_cast<std::uint64_t>(ch - '0');
        if (parsed > std::numeric_limits<std::uint16_t>::max()) {
            return false;
        }
    }

    out.host = std::string(host);
    out.port = static_cast<std::uint16_t>(parsed);
    return true;
}

std::string endpoint_to_string(const Endpoint& ep) {
    return ep.host + ":" + std::to_string(ep.port);
}

std::unique_ptr<ITransportBackend> create_default_backend() {
#if defined(__linux__)
    return std::make_unique<EpollTransportBackend>();
#elif defined(__APPLE__)
    return std::make_unique<KqueueTransportBackend>();
#elif defined(_WIN32)
    return std::make_unique<IocpTransportBackend>();
#else
    return std::make_unique<PollTransportBackend>();
#endif
}

std::unique_ptr<ITransportBackend> create_backend(std::string_view backend_name) {
    if (backend_name == "epoll") {
        return std::make_unique<EpollTransportBackend>();
    }
    if (backend_name == "kqueue") {
        return std::make_unique<KqueueTransportBackend>();
    }
    if (backend_name == "iocp") {
        return std::make_unique<IocpTransportBackend>();
    }
    if (backend_name == "poll") {
        return std::make_unique<PollTransportBackend>();
    }
    return nullptr;
}

}  // namespace swarmnet::transport
