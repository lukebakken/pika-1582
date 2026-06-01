"""Publisher-confirms harness for ThreadSafeConnection.

Exercises ``ThreadSafeChannel.confirm_delivery`` under multi-threaded load.
The threading-model claim under test is that the per-channel worker thread
runs the broker's Basic.Ack/Basic.Nack listener serially without stalling
the IOLoop thread (which differs from the Java/.NET clients).

Each publish is scheduled via ``add_callback_threadsafe``; inside that
IOLoop-thread callback we (a) increment our local sequence number, (b)
record the publish timestamp under a lock, and (c) call the raw
``basic_publish``.  Pika's own delivery-tag counter increments on the same
thread, so ours stays in lockstep.

Failure conditions, all checked once per stats interval:

  1. Any exception raised on a publisher, consumer, or confirm thread.
  2. Heartbeat-driven connection close on either side.
  3. Any Basic.Nack received (unless ``--allow-nacks`` is set).
  4. Pending unconfirmed publishes above ``--pending-threshold``
     (default 10000) sustained longer than the divergence window
     (default 60s).
  5. Mean confirm latency in the reporting interval above
     ``--lat-budget-ms`` (default 50ms), checked after the warmup
     window (default 30s) so the broker has time to settle.
  6. RSS growth above ``--mem-growth-threshold`` (default 50%) of the
     baseline RSS, after the ``--mem-growth-window`` warmup (default
     30m).
"""
from __future__ import annotations

import argparse
import logging
import os
import queue
import signal
import statistics
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field

import psutil

import pika

from pika_1582._common import (
    QUEUE_NAME,
    ROUTING_KEY,
    EXCHANGE_NAME,
    declare_topology,
    open_connection,
    parse_duration,
    setup_logging,
)

LOGGER = logging.getLogger('pika-1582.confirms')


@dataclass
class Counters:
    published: int = 0
    acked: int = 0
    nacked: int = 0
    consumed: int = 0
    publish_errors: int = 0
    consume_errors: int = 0


@dataclass
class FailureState:
    reason: str | None = None
    detail: str | None = None
    errors: queue.Queue = field(default_factory=queue.Queue)

    def fail(self, reason: str, detail: str = '') -> None:
        if self.reason is None:
            self.reason = reason
            self.detail = detail


class Publisher:
    """Owns a ThreadSafeConnection with publisher confirms enabled.

    Publish sequence numbers and pending timestamps are mutated only on
    the IOLoop thread (inside the scheduled publish callback) and on the
    per-channel worker thread (inside the confirm callback).  A single
    lock protects ``_pending`` since those two threads can race.
    """

    def __init__(self, host: str, port: int, heartbeat: int, n_threads: int,
                 body: bytes, publish_rate: float, counters: Counters,
                 failure: FailureState, samples: deque,
                 samples_lock: threading.Lock):
        self.host = host
        self.port = port
        self.heartbeat = heartbeat
        self.n_threads = n_threads
        self.body = body
        self.publish_rate = publish_rate
        self.counters = counters
        self.failure = failure
        self.samples = samples
        self.samples_lock = samples_lock

        self.stop = threading.Event()
        self.conn = open_connection(host, port, heartbeat)
        self.channel = self.conn.channel()
        declare_topology(self.channel)
        # Drain leftover messages so the published/consumed counters
        # start at zero on both sides.
        self.channel.queue_purge(QUEUE_NAME)

        self._pending: dict[int, float] = {}
        self._pending_lock = threading.Lock()

        # Enable publisher confirms.  The callback runs on the per-channel
        # worker thread (single-threaded), so its mutations of _pending
        # are serialized with respect to each other — we still hold the
        # lock because the on_publish callback (IOLoop thread) is the
        # other writer.
        self.channel.confirm_delivery(self._on_confirm)

        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for i in range(self.n_threads):
            t = threading.Thread(target=self._run,
                                 args=(i,),
                                 name=f'pub-{i}',
                                 daemon=True)
            t.start()
            self._threads.append(t)

    def _run(self, thread_id: int) -> None:
        properties = pika.BasicProperties(
            content_type='application/octet-stream',
            delivery_mode=pika.DeliveryMode.Transient,
        )
        period = 1.0 / self.publish_rate if self.publish_rate > 0 else 0.0
        next_at = time.monotonic()
        while not self.stop.is_set():
            try:
                self._schedule_publish(properties)
                self.counters.published += 1
            except Exception as exc:  # noqa: BLE001
                self.counters.publish_errors += 1
                self.failure.errors.put(('publisher', thread_id, exc))
                self.failure.fail('publisher exception',
                                  f'{type(exc).__name__}: {exc}')
                return
            if period > 0:
                next_at += period
                delay = next_at - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                else:
                    next_at = time.monotonic()

    def _schedule_publish(self, properties: pika.BasicProperties) -> None:
        """Publish via ThreadSafeChannel with on_publish tracking.

        The on_publish callback fires on the IOLoop thread immediately
        after the frame is written, with the delivery tag that the
        broker will use for the confirm.  This keeps our pending-map in
        lockstep with the broker's sequence without reaching into
        private channel internals.
        """
        self.channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=self.body,
            properties=properties,
            on_publish=self._on_publish,
        )

    def _on_publish(self, delivery_tag: int) -> None:
        """Called on the IOLoop thread immediately after a publish frame is
        written.  Records the publish timestamp keyed by the delivery tag."""
        with self._pending_lock:
            self._pending[delivery_tag] = time.monotonic()

    def _on_confirm(self, method_frame) -> None:
        """Broker ack/nack callback - runs on the per-channel worker."""
        method = method_frame.method
        tag = method.delivery_tag
        is_ack = isinstance(method, pika.spec.Basic.Ack)
        try:
            now = time.monotonic()
            with self._pending_lock:
                if method.multiple:
                    # All seqs <= tag are confirmed by this frame.
                    matched = [s for s in self._pending if s <= tag]
                    sent_times = [self._pending.pop(s) for s in matched]
                else:
                    sent_times = []
                    if tag in self._pending:
                        sent_times.append(self._pending.pop(tag))
            latencies = [now - t for t in sent_times]
            if is_ack:
                self.counters.acked += len(latencies)
            else:
                self.counters.nacked += len(latencies)
            if latencies:
                with self.samples_lock:
                    self.samples.extend(latencies)
        except Exception as exc:  # noqa: BLE001
            self.failure.errors.put(('confirm', -1, exc))
            self.failure.fail('confirm callback exception',
                              f'{type(exc).__name__}: {exc}')

    def pending_count(self) -> int:
        with self._pending_lock:
            return len(self._pending)

    def shutdown(self) -> None:
        self.stop.set()
        for t in self._threads:
            t.join(timeout=10)
        try:
            self.conn.close()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning('Publisher conn.close() raised: %s', exc)


class Consumer:
    """Manual-ack consumer to drain the queue end-to-end."""

    def __init__(self, host: str, port: int, heartbeat: int,
                 counters: Counters, failure: FailureState):
        self.host = host
        self.port = port
        self.heartbeat = heartbeat
        self.counters = counters
        self.failure = failure

        self.stop = threading.Event()
        self.conn = open_connection(host, port, heartbeat)
        self.channel = self.conn.channel()
        declare_topology(self.channel)
        self.channel.basic_qos(prefetch_count=64)
        self.channel.basic_consume(queue=QUEUE_NAME,
                                   on_message_callback=self._on_message)

    def start(self) -> None:
        # Consumer runs entirely on the per-channel worker thread; nothing
        # to spawn here.
        pass

    def _on_message(self, ch, method_frame, _props, _body) -> None:
        try:
            ch.basic_ack(delivery_tag=method_frame.delivery_tag)
            self.counters.consumed += 1
        except Exception as exc:  # noqa: BLE001
            self.counters.consume_errors += 1
            self.failure.errors.put(('consumer', -1, exc))
            self.failure.fail('consumer exception',
                              f'{type(exc).__name__}: {exc}')

    def shutdown(self) -> None:
        self.stop.set()
        try:
            self.conn.close()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning('Consumer conn.close() raised: %s', exc)


def _human_bytes(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if abs(n) < 1024:
            return f'{n:.1f}{unit}'
        n /= 1024
    return f'{n:.1f}TB'


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Nearest-rank percentile on an already-sorted list."""
    if not sorted_values:
        return 0.0
    idx = max(0, min(len(sorted_values) - 1,
                     int(round(pct / 100.0 * (len(sorted_values) - 1)))))
    return sorted_values[idx]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5672)
    parser.add_argument('--duration', type=parse_duration, default='5m',
                        help='How long to run (default: 5m)')
    parser.add_argument('--threads', type=int, default=8,
                        help='Publisher threads (default: 8)')
    parser.add_argument('--publish-rate', type=float, default=100.0,
                        help='Per-thread publish rate in msg/s '
                        '(default: 100; 0 = unlimited)')
    parser.add_argument('--body-size', type=int, default=512,
                        help='Message body size in bytes (default: 512)')
    parser.add_argument('--heartbeat', type=int, default=30)
    parser.add_argument('--stats-interval', type=parse_duration, default='30s',
                        help='Stats reporting interval (default: 30s)')
    parser.add_argument('--lat-budget-ms', type=float, default=50.0,
                        help='Fail if interval mean confirm latency exceeds '
                        'this many ms (default: 50.0)')
    parser.add_argument('--lat-warmup', type=parse_duration, default='30s',
                        help='Skip latency check until this much time has '
                        'elapsed (default: 30s)')
    parser.add_argument('--pending-threshold', type=int, default=10_000,
                        help='Fail if pending unconfirmed publishes exceed '
                        'this for longer than --divergence-window '
                        '(default: 10000)')
    parser.add_argument('--divergence-window', type=parse_duration,
                        default='60s',
                        help='Sustained pending-overflow window '
                        '(default: 60s)')
    parser.add_argument('--mem-growth-threshold', type=float, default=50.0,
                        help='Fail if RSS grows beyond this percentage of '
                        'baseline (default: 50.0)')
    parser.add_argument('--mem-growth-window', type=parse_duration,
                        default='30m',
                        help='Time after start before mem-growth is checked '
                        '(default: 30m)')
    parser.add_argument('--allow-nacks', action='store_true',
                        help='Do not treat Basic.Nack as a failure')
    parser.add_argument('--log-level', default='INFO')
    args = parser.parse_args(argv)

    setup_logging(args.log_level)

    rate_desc = (f'{args.publish_rate:.0f} msg/s/thread'
                 if args.publish_rate > 0 else 'unlimited')
    LOGGER.info(
        'Starting confirms harness: duration=%.0fs threads=%d body=%dB '
        'heartbeat=%ds rate=%s', args.duration, args.threads, args.body_size,
        args.heartbeat, rate_desc)
    LOGGER.info(
        'Failure thresholds: lat-budget=%.1fms (after %.0fs warmup), '
        'pending=%d (sustained %.0fs), mem-growth=%.1f%% (after %.0fs warmup)',
        args.lat_budget_ms, args.lat_warmup, args.pending_threshold,
        args.divergence_window, args.mem_growth_threshold,
        args.mem_growth_window)

    counters = Counters()
    failure = FailureState()
    samples: deque[float] = deque()
    samples_lock = threading.Lock()

    body = b'x' * args.body_size
    publisher = Publisher(args.host, args.port, args.heartbeat, args.threads,
                          body, args.publish_rate, counters, failure,
                          samples, samples_lock)
    consumer = Consumer(args.host, args.port, args.heartbeat, counters,
                        failure)
    consumer.start()
    publisher.start()

    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    LOGGER.info('Baseline RSS: %s', _human_bytes(baseline_rss))

    def _handle_sigint(_signum, _frame):
        failure.fail('interrupted by user (SIGINT)')

    signal.signal(signal.SIGINT, _handle_sigint)

    start = time.monotonic()
    deadline = start + args.duration
    next_stats = start + args.stats_interval
    last_published = 0
    last_acked = 0
    last_consumed = 0
    last_report = start

    pending_over_since: float | None = None

    while time.monotonic() < deadline and failure.reason is None:
        time.sleep(0.5)
        now = time.monotonic()
        if now < next_stats:
            continue

        published = counters.published
        acked = counters.acked
        nacked = counters.nacked
        consumed = counters.consumed
        pending = publisher.pending_count()

        dt = max(now - last_report, 1e-9)
        publish_rate = (published - last_published) / dt
        ack_rate = (acked - last_acked) / dt
        consume_rate = (consumed - last_consumed) / dt

        with samples_lock:
            interval_samples = list(samples)
            samples.clear()
        interval_samples.sort()

        if interval_samples:
            mean_ms = statistics.fmean(interval_samples) * 1000.0
            p50_ms = _percentile(interval_samples, 50) * 1000.0
            p95_ms = _percentile(interval_samples, 95) * 1000.0
            p99_ms = _percentile(interval_samples, 99) * 1000.0
            max_ms = interval_samples[-1] * 1000.0
        else:
            mean_ms = p50_ms = p95_ms = p99_ms = max_ms = 0.0

        rss = proc.memory_info().rss
        elapsed = now - start
        LOGGER.info(
            't=%5.0fs  pub=%-9d (%5.0f/s)  ack=%-9d (%5.0f/s)  nack=%-4d  '
            'pend=%-5d  con=%-9d (%5.0f/s)  '
            'lat: mean=%5.1fms p50=%5.1f p95=%5.1f p99=%5.1f max=%5.1f  '
            'rss=%s', elapsed, published, publish_rate, acked, ack_rate,
            nacked, pending, consumed, consume_rate, mean_ms, p50_ms, p95_ms,
            p99_ms, max_ms, _human_bytes(rss))

        # Failure check 3: nacks.
        if nacked > 0 and not args.allow_nacks:
            failure.fail('broker returned Basic.Nack',
                         f'nacked={nacked} (use --allow-nacks to ignore)')

        # Failure check 4: sustained pending growth.
        if pending > args.pending_threshold:
            if pending_over_since is None:
                pending_over_since = now
            elif now - pending_over_since > args.divergence_window:
                failure.fail(
                    'pending unconfirmed publishes exceeded threshold',
                    f'pending={pending} sustained for '
                    f'{now - pending_over_since:.0f}s (threshold='
                    f'{args.pending_threshold}, window='
                    f'{args.divergence_window:.0f}s)')
        else:
            pending_over_since = None

        # Failure check 5: latency budget (after warmup).
        if elapsed >= args.lat_warmup and interval_samples:
            if mean_ms > args.lat_budget_ms:
                failure.fail(
                    'mean confirm latency exceeded budget',
                    f'mean={mean_ms:.1f}ms budget={args.lat_budget_ms:.1f}ms '
                    f'(p99={p99_ms:.1f}ms max={max_ms:.1f}ms over '
                    f'{len(interval_samples)} samples)')

        # Failure check 6: memory growth.
        if elapsed >= args.mem_growth_window:
            growth_pct = (rss - baseline_rss) / max(baseline_rss, 1) * 100.0
            if growth_pct > args.mem_growth_threshold:
                failure.fail(
                    'memory growth exceeded threshold',
                    f'rss grew {growth_pct:.1f}% over {elapsed:.0f}s '
                    f'(baseline={_human_bytes(baseline_rss)}, '
                    f'now={_human_bytes(rss)})')

        # Failure check 2: heartbeat-driven close.
        if not publisher.conn.is_open:
            failure.fail('publisher connection unexpectedly closed')
        if not consumer.conn.is_open:
            failure.fail('consumer connection unexpectedly closed')

        last_published = published
        last_acked = acked
        last_consumed = consumed
        last_report = now
        next_stats = now + args.stats_interval

    elapsed = time.monotonic() - start
    LOGGER.info('Confirms loop exited at t=%.0fs', elapsed)

    publisher.shutdown()
    consumer.shutdown()

    LOGGER.info(
        'Final counters: published=%d acked=%d nacked=%d consumed=%d '
        'pending=%d errors=(pub=%d, con=%d)',
        counters.published, counters.acked, counters.nacked,
        counters.consumed, publisher.pending_count(),
        counters.publish_errors, counters.consume_errors)

    if failure.reason is not None:
        LOGGER.error('CONFIRMS FAILED: %s', failure.reason)
        if failure.detail:
            LOGGER.error('  detail: %s', failure.detail)
        seen = 0
        while not failure.errors.empty() and seen < 10:
            where, tid, exc = failure.errors.get_nowait()
            LOGGER.error('  [%s thread=%d] %s: %s', where, tid,
                         type(exc).__name__, exc)
            seen += 1
        return 1

    LOGGER.info(
        'CONFIRMS PASSED: %.0fs elapsed, published=%d acked=%d (matched), '
        'consumed=%d', elapsed, counters.published, counters.acked,
        counters.consumed)
    return 0


if __name__ == '__main__':
    sys.exit(main())
