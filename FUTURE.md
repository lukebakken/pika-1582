# Future work

Things this harness does **not** currently exercise that we should add as
follow-ups.

## 1. TLS

The current harness uses plaintext.  A TLS-enabled variant would catch
heartbeat / write-buffer behavior under the higher latency and CPU cost of
TLS framing.  Probably gated on a `--tls` flag that picks port 5671 and
loads certs from a local directory.

## 2. Connection-recycle (not just channel-recycle)

We deliberately do not exercise connection recycling because pika does
not provide auto-reconnect; that is the user's responsibility.  A
"recover-from-broker-restart" soak would exercise the full close-and-
reopen path against a broker that goes away mid-run.  Worth doing once
the basic soak has passed end-to-end multiple times.

## 3. Expose publish seqno from ThreadSafeChannel (pika upstream)

The confirms harness currently reaches past `ThreadSafeChannel` to the
raw `_channel` in order to record a publish timestamp in the same IOLoop
turn as the actual `basic_publish`.  This keeps our sequence counter in
lockstep with pika's internal delivery-tag counter.

Users who want confirms with per-message latency tracking should not need
to use private internals.  The fix is upstream in pika PR #1582: either
expose a `pre_publish_hook` callback that fires on the IOLoop thread
immediately before the raw publish, or return a `Future`-like handle
carrying the delivery tag.  A TODO has been added to
`ThreadSafeChannel.basic_publish`.

## 4. Slow consumer / backpressure

The current consumer acks immediately.  A variant that simulates slow
work (e.g. `time.sleep(0.1)` per message with `prefetch_count=64`) would
exercise the per-channel worker queue and surface any starvation in the
work pool.

## Done

- **Publisher confirms** (`src/pika_1582/confirms.py`,
  `hatch run confirms`).  N publisher threads share a single
  `ThreadSafeChannel` with `confirm_delivery` enabled; per-message
  latency is tracked from the IOLoop-side publish callback to the
  per-channel worker-thread ack/nack callback.  Failure conditions
  cover nacks, sustained pending growth, mean-latency budget, and the
  usual memory and counter-divergence checks.
