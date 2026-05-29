# pika-1582

Reproducer and soak harness for [pika PR #1582](https://github.com/pika/pika/pull/1582), which adds the `ThreadSafeConnection` / `ThreadSafeChannel` classes.

The `pika-pika/` submodule points at the PR branch (`feature/thread-safe-connection`) and is installed editable into the local hatch env, so changes to the submodule are picked up immediately.

## Quick start

```bash
# Pull the submodule once after cloning.
git submodule update --init --recursive

# Start a local RabbitMQ container (port 5672, management UI on 15672).
hatch run rabbit-up

# 30-second concurrent-publish reproducer for #1144 / #511.  Should pass
# with zero exceptions.
hatch run repro

# 5-minute soak (channel recycle, stats every 60s, all four failure
# detectors enabled).
hatch run soak-quick

# Full 4-hour soak, the actual confidence-builder.
hatch run soak

# Stop and remove the broker.
hatch run rabbit-down
```

## Scripts

| Command | Purpose |
|---|---|
| `hatch run rabbit-up` | Start RabbitMQ via docker compose (`rabbitmq:management`). |
| `hatch run rabbit-down` | Stop and remove the broker. |
| `hatch run rabbit-logs` | Tail broker logs. |
| `hatch run repro` | Run the #1144 / #511 reproducer. Exits 0 on zero exceptions. |
| `hatch run soak` | 4-hour soak with channel recycle and stats every 60s. |
| `hatch run soak-quick` | 5-minute soak (same checks, shorter run, useful smoke test). |

All scripts accept `--help`. Common flags:

```
--host HOST              Broker host (default: localhost)
--port PORT              Broker port (default: 5672)
--threads N              Publisher threads (default: 8)
--duration TEXT          Run length: 30s, 5m, 4h, 1d ... (default per-script)
--body-size BYTES        Message body size
--heartbeat SECONDS      AMQP heartbeat interval

# soak only
--stats-interval TEXT
--channel-recycle-period TEXT
--mem-growth-threshold PCT
--mem-growth-window TEXT
--divergence-threshold N
--divergence-window TEXT
```

## Failure conditions

`soak` exits non-zero on any of:

1. Any exception raised on a publisher or consumer thread.
2. Heartbeat-driven connection close (broker thinks we died).
3. RSS growth above `--mem-growth-threshold` (default 50%) of the baseline RSS, measured after the `--mem-growth-window` (default 30m) warmup.
4. Publisher/consumer counter divergence above `--divergence-threshold` (default 1000) sustained for longer than `--divergence-window` (default 60s).

`repro` exits non-zero on any exception.

## Topology

| | Value |
|---|---|
| Exchange | `pika-1582` (direct, durable) |
| Queue | `pika-1582` (durable) |
| Routing key | `pika-1582` |

The publisher and consumer use **separate** `ThreadSafeConnection` instances (best practice). Channels are recycled on a timer.

## Note on what is being tested

The bug from #1144 / #511 was that `BlockingConnection`'s `_tx_buffers` deque was being mutated from a non-IOLoop thread when multiple threads called `basic_publish` on a shared channel. `ThreadSafeConnection` routes every publish through `add_callback_threadsafe`, so the deque is only ever touched on the IOLoop thread, eliminating the race by construction.

`repro.py` replicates the original shape (N threads sharing a single channel, hammering `basic_publish`) and asserts zero exceptions. `soak.py` runs the same pattern for hours alongside a consumer, periodic channel recycles, memory growth tracking, and producer/consumer balance checks.
