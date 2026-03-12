# Transport MVP Contract (Week 4)

SwarmNet Week 4 introduces a transport abstraction and local mesh protocol.

## Interface

- `ITransportBackend` in `include/swarmnet/transport.hpp`
- Backend names:
  - Linux: `epoll`
  - macOS: `kqueue`
  - Windows: `iocp`

## Packet Kinds

1. `hello`
2. `hello_ack`
3. `unreliable`
4. `reliable`
5. `ack`

## Wire Header

Little-endian fixed order:

1. `u32 magic` (`SNT1`)
2. `u16 version`
3. `u16 kind`
4. `u32 stream_id`
5. `u64 sequence`
6. `u64 node_id`
7. `u32 payload_size`
8. `u32 payload_crc32c`

Payload CRC is CRC32C over payload bytes.

## Reliability

- Reliable messages are tracked by `(peer, stream_id, sequence)`.
- Receiver auto-acks reliable packets.
- Sender retransmits unacked reliable packets on a fixed retry interval.
- Duplicate reliable packets are suppressed by sequence dedup per `(peer, stream_id)`.

## Handshake

- Node sends `hello`.
- Peer replies with `hello_ack`.
- Worker considers peer connected only after receiving `hello_ack`.
