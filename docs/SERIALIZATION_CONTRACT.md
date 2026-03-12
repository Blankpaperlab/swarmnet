# Serialization Contract (v1)

This document locks the Week 2 deterministic wire contract for `Event` frames.

## Endianness

- All integral fields are encoded as little-endian.
- No platform-native byte order is allowed.

## Schema

- `magic`: `0x53574E31` (`SWN1`)
- `schema_version`: `1`
- `payload_type`: `1` (`Event`)

## Frame Layout

Header (`16` bytes total, fixed order):

1. `u32 magic`
2. `u16 schema_version`
3. `u16 payload_type`
4. `u32 payload_size`
5. `u32 payload_crc32c`

Payload (`Event`, fixed order):

1. `u64 tick`
2. `u64 node_id_hi`
3. `u64 node_id_lo`
4. `u64 origin_seq`
5. `u16 event_type` (`1=upsert`, `2=erase`)
6. `u16 reserved` (`0`)
7. `u32 key_len`
8. `u32 value_len`
9. `key_len` bytes (raw key bytes)
10. `value_len` bytes (raw value bytes)

## Integrity

- `payload_crc32c` is CRC32C (Castagnoli) over payload bytes only.
- A frame is invalid if:
  - header is malformed
  - `magic`/`schema_version`/`payload_type` mismatch
  - `payload_size` does not match frame length
  - CRC mismatch
  - reserved field is non-zero

## Determinism Notes

- The contract is field-order stable and architecture-independent.
- Tests in `tests/unit/test_core_kernel.cpp` enforce header values, little-endian encoding, and CRC validation.
