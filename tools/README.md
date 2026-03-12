# Tools

- `benchmark_reference.cmake`: prints machine reference + Week 1 target line.
- `amalgamate.py`: generates `dist/swarmnet.hpp` + `dist/swarmnet.cpp`.
- `verify_amalgamated.cmake`: configures/builds/runs a smoke project using only `dist` files.
- `swarmnet_test.cpp`: simulation launcher
  - single-process deterministic mode (`--agents`, `--ticks`, replay in/out)
  - multi-process local mesh mode (`--mode=multi`, `--processes`, handshake/reliable/unreliable)
  - membership convergence mode (`--mode=cluster`, bootstrap/gossip/view_epoch commit)
  - performance benchmark mode (`--mode=bench`, 1k-agent target gate, CPU/memory metrics, backpressure stats)
