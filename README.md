# SwarmNet v0.1 (Solo Founder Track)

SwarmNet is a pure C++23 middleware library for deterministic multi-agent systems.

## Week 7 Status

- Repository skeleton in place
- Deterministic core kernel + serialization contract implemented
- Replay journal (record/playback) and playback injector implemented
- `record_external(...)` API integrated
- Transport MVP (`ITransportBackend`) with platform backend selection
- Unreliable + reliable streams and basic hello/hello_ack handshake
- Multi-process local mesh simulation mode in `swarmnet_test`
- Bootstrap + gossip + lightweight deterministic membership commit (`view_epoch`)
- Majority-commit-only partition behavior (minority safe mode)
- Heartbeat timeout failure detection
- Snapshot + delta catch-up on rejoin
- Crash/restart and rejoin tracking with drift detection (`drift_events`)
- Arena-backed immutable state blobs
- Zero-copy shared-state read API (`Agent::get_shared_state_view`)
- Memory-pressure backpressure (`max_arena_bytes`, `max_pending_events`) + runtime drop/rejection stats
- Benchmark mode and Week 7 target gate in `swarmnet_test --mode=bench`

## Quick Start

```bash
cmake --preset dev-debug
cmake --build --preset dev-debug
ctest --preset dev-debug
```

Print benchmark baseline:

```bash
cmake --build --preset dev-debug --target benchmark-reference
```

Generate and verify amalgamated dist files:

```bash
cmake --build --preset dev-debug --target amalgamate
ctest --preset dev-debug -R amalgamation_smoke
```

CI note: fast/debug and release compatibility matrices run GCC, Clang, and MSVC with `amalgamation_smoke` enabled.

Run 500-agent local simulation + record:

```bash
./build/dev-debug/swarmnet_test --agents=500 --ticks=200 --replay-out=swarmnet_500.replay
```

Replay the same run:

```bash
./build/dev-debug/swarmnet_test --agents=500 --ticks=0 --replay-in=swarmnet_500.replay
```

Run local multi-process mesh test:

```bash
./build/dev-debug/swarmnet_test --mode=multi --agents=500 --ticks=200 --processes=10
```

Run 100-node membership convergence check:

```bash
./build/dev-debug/swarmnet_test --mode=cluster --agents=100 --bootstrap=3 --fanout=5 --ticks=220
```

Run Week 6 chaos profile (30% crash churn) and verify zero drift:

```bash
./build/dev-debug/swarmnet_test --mode=cluster --agents=100 --bootstrap=3 --fanout=5 --ticks=260 --stabilization-ticks=120 --churn-period=12 --churn-batch=30 --churn-down-ticks=5 --heartbeat-timeout=8 --snapshot-interval=16 --partition-start=40 --partition-end=120 --partition-minority=30
```

Run Week 7 1,000-agent benchmark target check:

```bash
./build/dev-debug/swarmnet_test --mode=bench --agents=1000 --ticks=300 --payload-bytes=48 --target-cpu=40 --target-mem-mb=3072 --arena-max-mb=1024
```

## Pricing / Commercial License

Core library is MIT for open usage.  
For teams needing a commercial license, support SLA, or proprietary redistribution terms:

- Contact: `founder@swarmnet.dev`
- Commercial license template: [COMMERCIAL_LICENSE_TEMPLATE.md](COMMERCIAL_LICENSE_TEMPLATE.md)

## Repository Layout

- `include/` public headers
- `src/` implementation
- `tests/` deterministic and unit tests
- `examples/` minimal runnable samples
- `tools/` scripts and developer tooling
- `bench/` benchmark assets
- `dist/` future single-header release output
