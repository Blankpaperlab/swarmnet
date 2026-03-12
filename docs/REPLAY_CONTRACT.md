# Replay Journal Contract (v1)

This file defines the deterministic replay journal format used in Week 3.

## File Header (8 bytes, little-endian)

1. `u32 magic`: `0x31504C52` (`RLP1`)
2. `u16 schema_version`: `1`
3. `u16 reserved`: `0`

## Record Header (12 bytes, little-endian)

1. `u16 record_type`
2. `u16 reserved` (`0`)
3. `u32 payload_size`
4. `u32 payload_crc32c`

Record types:

- `1`: `TickRecord`
- `2`: `ExternalRecord`

## TickRecord Payload

1. `u64 tick`
2. `u64 state_root`
3. `u32 event_count`
4. repeated `event_count` times:
   - `u32 frame_size`
   - `frame_size` bytes of `Event` frame from `serialize_event(...)`

## ExternalRecord Payload

1. `u64 sequence`
2. `u32 name_len`
3. `u32 value_len`
4. `name_len` bytes
5. `value_len` bytes

## Validation Rules

- Header magic and schema must match.
- Record reserved fields must be zero.
- Payload length must match.
- CRC32C must match payload bytes.
- Tick events must deserialize through the Week 2 serialization contract.
