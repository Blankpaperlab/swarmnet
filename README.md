# SwarmNet

SwarmNet is a C++23 library for deterministic multi-agent systems. It provides a tick-based runtime for spawning agents, exchanging messages, publishing shared state, and recording or replaying external inputs.

## Features

- Deterministic agent execution on a fixed tick cadence
- Agent messaging and shared-state APIs
- Replay support for recording and reproducing external inputs
- Local cluster and multiprocess test harnesses
- CMake-based build with unit and smoke tests

## Requirements

- CMake 3.24 or newer
- A C++23 compiler such as GCC, Clang, or MSVC
- Python 3 if you need generated files under `dist/`

## Build

```bash
cmake --preset dev-debug
cmake --build --preset dev-debug
```

## Test

```bash
ctest --preset dev-debug --output-on-failure
```

## Use From CMake

Add the library to your build and link against `swarmnet::swarmnet`:

```cmake
add_subdirectory(path/to/swarmnet)
target_link_libraries(your_app PRIVATE swarmnet::swarmnet)
```

See `examples/minimal/main.cpp` for a minimal runnable example.

## Minimal Example

```cpp
#include "swarmnet/swarmnet.hpp"

int main() {
    swarmnet::Config cfg;
    cfg.swarm_id = "minimal-example";

    swarmnet::SwarmNet swarm(cfg);
    swarm.spawn([](swarmnet::Agent& self) {
        self.broadcast("heartbeat", "alive");
        self.set_shared_state("status", "running");
    });

    swarm.run_ticks(1);
}
```

## License

This project is licensed under the MIT License. See `LICENSE`.
