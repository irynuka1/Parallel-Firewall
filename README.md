# Parallel Firewall

A multithreaded packet-filtering firewall written in C with POSIX threads. A **producer** thread reads packets and pushes them into a shared **ring buffer**; multiple **consumer** threads pull packets out, run them through the filter, and write a `PASS`/`DROP` decision to a log file.

## How It Works

```
                  ┌──────────────┐        ┌───────────────────┐
  input file ───► │   Producer   │ ─────► │   Ring buffer     │
                  │ (1 thread)   │ enqueue│ mutex + 2 condvars│
                  └──────────────┘        └───────────────────┘
                                                   │ dequeue
                          ┌────────────────────────┼───────────────┐
                          ▼                        ▼               ▼
                    ┌────────────┐          ┌────────────┐   ┌────────────┐
                    │ Consumer 1 │   ...    │ Consumer N │   │ Consumer … │
                    └─────┬──────┘          └─────┬──────┘   └─────┬──────┘
                          └───────────────┬───────┴────────────────┘
                                          ▼
                                   log file (mutex-protected)
```

- **Packets** are made-up fixed-size records: source, destination, timestamp and payload (see `src/packet.h`).
- **Ring buffer** (`src/ring_buffer.c`) is a fixed-size circular FIFO. A mutex guards its state and two condition variables (`not_full`, `not_empty`) block threads instead of busy-waiting.
- **Shutdown**: when the producer reaches end of input it calls `ring_buffer_stop()`, which wakes all waiting consumers so they can exit.
- **Log format**: one line per packet — `<PASS|DROP> <hash> <timestamp>`.

## Requirements and Constraints

- No busy waiting: threads must block on condition variables when the buffer is empty or full.
- At least `num_consumers + 1` threads run (one producer plus the consumers).
- The log must be written in ascending timestamp order *while* packets are processed, not sorted afterwards.

## Ordered Logging

Packets are enqueued in timestamp order, so ordering the log means restoring arrival order after parallel processing:

1. **Sequence number**: each consumer receives one atomically with its dequeue.
2. **Parallel processing**: packets are filtered and hashed concurrently; no consumer waits for another at this stage.
3. **Slot window**: a finished log line is placed in a slot indexed by `seq % 4096`.
4. **Ordered flush**: one consumer at a time drains the contiguous run of ready slots, in sequence order, and writes it in batches outside the lock. Other consumers just deposit their entry and continue.

The log is therefore written incrementally in ascending timestamp order, with no post-processing sort and no busy waiting. A consumer blocks on a condition variable only if the slot window is full because an earlier packet is still being processed.

## Project Structure

```
.
├── src/                 # Firewall source code
│   ├── firewall.c       #   Entry point of the parallel version
│   ├── serial.c         #   Reference serial implementation
│   ├── producer.[ch]    #   Reads packets and feeds the ring buffer
│   ├── consumer.[ch]    #   Consumer threads: filter packets and write the log
│   ├── ring_buffer.[ch] #   Thread-safe circular buffer
│   ├── packet.[ch]      #   Packet definition, hashing and filter logic
│   └── Makefile
├── utils/               # Logging helpers and the DIE() macro
├── tests/               # Test inputs/references, generator, checker and grader
├── checker/             # High-level checker script used by the Docker setup
├── docs/                # Checker/infrastructure documentation and images
├── Dockerfile           # Reproducible build and test environment
├── local.sh             # Helper to build and run the checker locally
└── CPPLINT.cfg          # Linter configuration
```

## Build

```console
$ cd src
$ make
```

This produces two binaries: `serial` and `firewall`.

## Usage

```console
$ ./firewall <input-file> <output-file> <num-consumers>   # num-consumers: 1-32
$ ./serial   <input-file> <output-file>
```

Example:

```console
$ ./firewall ../tests/in/test_1_000.in out.log 4
```

## Testing

```console
$ cd tests
$ make check    # run functional tests against the reference outputs
$ make lint     # checkpatch, cpplint, shellcheck
$ ./grade.sh    # full grading (correctness + style)
```

To run everything in a pinned Docker environment, use `./local.sh docker build` followed by `./local.sh checker`. See [docs/CHECKER.md](docs/CHECKER.md) for details.

