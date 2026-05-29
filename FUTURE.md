# Future work

Things this harness does **not** currently exercise that we should add as
follow-ups.

## 1. Publisher confirms

`ThreadSafeChannel.confirm_delivery(ack_nack_callback)` enables RabbitMQ
publisher confirms.  The ack/nack callback runs on the per-channel worker
thread (not the IOLoop thread), so a slow listener cannot stall heartbeats.
We should add a soak variant that:

- Calls `confirm_delivery` on the publisher channel after open.
- Tracks `acked` and `nacked` counters in the callback.
- Adds a fifth failure check: any nack received, or unacked-in-flight
  growing unboundedly.
- Reports `acked`/`nacked` in the per-interval stats line.

This is the most important next test: confirms are the production-grade
publishing path, and the threading model around the ack callback is one of
the more subtle parts of the new wrapper (Java/.NET clients run this
callback inline on the I/O thread; pika deliberately diverges).

Suggested entrypoint: `src/pika_1582/soak_confirms.py` plus a
`hatch run soak-confirms` script.  The base `Publisher` and `Counters`
classes can be reused with a confirm-aware subclass.

## 2. TLS

The current harness uses plaintext.  A TLS-enabled variant would catch
heartbeat / write-buffer behavior under the higher latency and CPU cost of
TLS framing.  Probably gated on a `--tls` flag that picks port 5671 and
loads certs from a local directory.

## 3. Connection-recycle (not just channel-recycle)

We deliberately do not exercise connection recycling because pika does
not provide auto-reconnect; that is the user's responsibility.  A
"recover-from-broker-restart" soak would exercise the full close-and-
reopen path against a broker that goes away mid-run.  Worth doing once
the basic soak has passed end-to-end multiple times.

## 4. Slow consumer / backpressure

The current consumer acks immediately.  A variant that simulates slow
work (e.g. `time.sleep(0.1)` per message with `prefetch_count=64`) would
exercise the per-channel worker queue and surface any starvation in the
work pool.
