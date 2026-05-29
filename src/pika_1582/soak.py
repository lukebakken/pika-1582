"""Long-running soak harness for ThreadSafeConnection.

Two separate connections are used (best practice and explicitly noted in the
pika docs): one for publishers, one for consumers.  Channels are recycled
periodically to exercise channel lifecycle.

Failure conditions, all checked once per stats interval:

  1. Any exception raised on a publisher or consumer thread.
  2. Heartbeat-driven connection close (broker thinks we died).
  3. RSS growth above threshold (default 50%) over the warmup window
     (default 30m).  The first 30 minutes are treated as steady-state
     warmup and not compared.
  4. Publisher/consumer counter divergence above threshold
     (default 1000) sustained for more than the divergence-window
     (default 60s).

The script exits non-zero on any of the above and on the soak completing
its full duration with no failures it exits 0.
"""
from __future__ import annotations

import argparse
import logging
import os
import queue
import signal
import sys
import threading
import time
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

LOGGER = logging.getLogger('pika-1582.soak')


@dataclass
class Counters:
    """Atomic-ish (we only ever increment from one thread per counter)
    counters shared across reporter and worker threads."""
    published: int = 0
    consumed: int = 0
    publish_errors: int = 0
    consume_errors: int = 0
    channel_recycles: int = 0


@dataclass
class FailureState:
    """Captures the first irrecoverable failure detected during the soak."""
    reason: str | None = None
    detail: str | None = None
    errors: queue.Queue = field(default_factory=queue.Queue)

    def fail(self, reason: str, detail: str = '') -> None:
        if self.reason is None:
            self.reason = reason
            self.detail = detail


class Publisher:
    """Owns a ThreadSafeConnection and N publisher threads sharing a single
    ThreadSafeChannel.  Recycles the channel on a timer."""

    def __init__(self, host: str, port: int, heartbeat: int, n_threads: int,
                 body: bytes, channel_recycle_period: float,
                 publish_rate: float, counters: Counters,
                 failure: FailureState):
        self.host = host
        self.port = port
        self.heartbeat = heartbeat
        self.n_threads = n_threads
        self.body = body
        self.channel_recycle_period = channel_recycle_period
        self.publish_rate = publish_rate  # msg/s per thread; 0 = unlimited
        self.counters = counters
        self.failure = failure

        self.stop = threading.Event()
        self.conn = open_connection(host, port, heartbeat)
        self.channel = self.conn.channel()
        declare_topology(self.channel)
        # Drain anything left over from prior runs so producer/consumer
        # counters start aligned.
        self.channel.queue_purge(QUEUE_NAME)
        # Lock protects channel-swap during recycle.
        self.channel_lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for i in range(self.n_threads):
            t = threading.Thread(target=self._run,
                                 args=(i,),
                                 name=f'pub-{i}',
                                 daemon=True)
            t.start()
            self._threads.append(t)
        recycler = threading.Thread(target=self._recycle_loop,
                                    name='pub-recycle',
                                    daemon=True)
        recycler.start()
        self._threads.append(recycler)

    def _run(self, thread_id: int) -> None:
        properties = pika.BasicProperties(
            content_type='application/octet-stream',
            delivery_mode=pika.DeliveryMode.Transient,
        )
        period = 1.0 / self.publish_rate if self.publish_rate > 0 else 0.0
        next_at = time.monotonic()
        while not self.stop.is_set():
            try:
                with self.channel_lock:
                    ch = self.channel
                ch.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=ROUTING_KEY,
                    body=self.body,
                    properties=properties,
                )
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
                    # Falling behind; reset the schedule rather than
                    # accumulating debt so a brief stall does not
                    # turn into a sustained overshoot.
                    next_at = time.monotonic()

    def _recycle_loop(self) -> None:
        next_at = time.monotonic() + self.channel_recycle_period
        while not self.stop.is_set():
            time.sleep(1.0)
            if time.monotonic() < next_at:
                continue
            try:
                LOGGER.info('Recycling publisher channel')
                new_ch = self.conn.channel()
                # Swap atomically, close the old one outside the lock.
                with self.channel_lock:
                    old, self.channel = self.channel, new_ch
                try:
                    old.close()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning('Old publisher channel close raised: %s',
                                   exc)
                self.counters.channel_recycles += 1
            except Exception as exc:  # noqa: BLE001
                self.failure.errors.put(('publisher-recycle', -1, exc))
                self.failure.fail('publisher channel recycle failed',
                                  f'{type(exc).__name__}: {exc}')
                return
            next_at = time.monotonic() + self.channel_recycle_period

    def shutdown(self) -> None:
        self.stop.set()
        for t in self._threads:
            t.join(timeout=10)
        try:
            self.conn.close()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning('Publisher conn.close() raised: %s', exc)


class Consumer:
    """Owns a ThreadSafeConnection and a single consuming channel.  Recycles
    the channel on a timer."""

    def __init__(self, host: str, port: int, heartbeat: int,
                 channel_recycle_period: float, counters: Counters,
                 failure: FailureState):
        self.host = host
        self.port = port
        self.heartbeat = heartbeat
        self.channel_recycle_period = channel_recycle_period
        self.counters = counters
        self.failure = failure

        self.stop = threading.Event()
        self.conn = open_connection(host, port, heartbeat)
        self.channel = self.conn.channel()
        self.consumer_tag: str | None = None
        declare_topology(self.channel)
        self.channel.basic_qos(prefetch_count=64)
        self.consumer_tag = self.channel.basic_consume(
            queue=QUEUE_NAME, on_message_callback=self._on_message)
        self.channel_lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        recycler = threading.Thread(target=self._recycle_loop,
                                    name='con-recycle',
                                    daemon=True)
        recycler.start()
        self._threads.append(recycler)

    def _on_message(self, ch, method_frame, _props, _body) -> None:
        # Ack via the channel that delivered the message - critical for
        # AMQP correctness: delivery_tags are scoped to the delivering
        # channel.  If a recycle is in flight, the old channel is still
        # valid for acks until it finishes closing (we drain in-flight
        # deliveries before closing in _recycle_loop).
        try:
            ch.basic_ack(delivery_tag=method_frame.delivery_tag)
            self.counters.consumed += 1
        except Exception as exc:  # noqa: BLE001
            self.counters.consume_errors += 1
            self.failure.errors.put(('consumer', -1, exc))
            self.failure.fail('consumer exception',
                              f'{type(exc).__name__}: {exc}')

    def _recycle_loop(self) -> None:
        # Quiescent recycle: cancel the consumer first so the broker
        # stops sending new deliveries, give the per-channel worker
        # thread a moment to drain in-flight callbacks (which need to
        # ack on the *old* channel), then close.  Without this draining
        # step, in-flight acks race the channel close handshake and
        # raise ChannelWrongStateError.
        next_at = time.monotonic() + self.channel_recycle_period
        while not self.stop.is_set():
            time.sleep(1.0)
            if time.monotonic() < next_at:
                continue
            try:
                LOGGER.info('Recycling consumer channel')
                old = self.channel
                old_tag = self.consumer_tag
                new_ch = self.conn.channel()
                declare_topology(new_ch)
                new_ch.basic_qos(prefetch_count=64)
                new_tag = new_ch.basic_consume(
                    queue=QUEUE_NAME, on_message_callback=self._on_message)
                with self.channel_lock:
                    self.channel = new_ch
                    self.consumer_tag = new_tag
                if old_tag is not None:
                    try:
                        old.basic_cancel(old_tag)
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning('Old consumer basic_cancel raised: %s',
                                       exc)
                # Allow in-flight deliveries on the old channel's worker
                # thread to complete their acks before we close it.
                time.sleep(0.5)
                try:
                    old.close()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning('Old consumer channel close raised: %s',
                                   exc)
                self.counters.channel_recycles += 1
            except Exception as exc:  # noqa: BLE001
                self.failure.errors.put(('consumer-recycle', -1, exc))
                self.failure.fail('consumer channel recycle failed',
                                  f'{type(exc).__name__}: {exc}')
                return
            next_at = time.monotonic() + self.channel_recycle_period

    def shutdown(self) -> None:
        self.stop.set()
        for t in self._threads:
            t.join(timeout=10)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5672)
    parser.add_argument('--duration', type=parse_duration, default='4h',
                        help='How long to run (default: 4h)')
    parser.add_argument('--threads', type=int, default=8,
                        help='Publisher threads (default: 8)')
    parser.add_argument('--publish-rate', type=float, default=100.0,
                        help='Per-thread publish rate in msg/s (default: '
                        '100; 0 = unlimited).  Default x default threads = '
                        '~800 msg/s, comfortably within broker headroom for '
                        'a multi-hour soak')
    parser.add_argument('--body-size', type=int, default=512,
                        help='Message body size in bytes (default: 512)')
    parser.add_argument('--heartbeat', type=int, default=30)
    parser.add_argument('--stats-interval', type=parse_duration, default='60s',
                        help='Stats reporting interval (default: 60s)')
    parser.add_argument('--channel-recycle-period', type=parse_duration,
                        default='5m',
                        help='How often to recycle channels (default: 5m)')
    parser.add_argument('--mem-growth-threshold', type=float, default=50.0,
                        help='Fail if RSS grows beyond this percentage of '
                        'baseline RSS (default: 50.0)')
    parser.add_argument('--mem-growth-window', type=parse_duration,
                        default='30m',
                        help='Time after start before mem-growth is checked '
                        '(default: 30m)')
    parser.add_argument('--divergence-threshold', type=int, default=1000,
                        help='Fail if published-consumed exceeds this for '
                        'longer than --divergence-window (default: 1000)')
    parser.add_argument('--divergence-window', type=parse_duration,
                        default='60s',
                        help='Sustained divergence window (default: 60s)')
    parser.add_argument('--log-level', default='INFO')
    args = parser.parse_args(argv)

    setup_logging(args.log_level)

    rate_desc = (f'{args.publish_rate:.0f} msg/s/thread'
                 if args.publish_rate > 0 else 'unlimited')
    LOGGER.info(
        'Starting soak: duration=%.0fs threads=%d body=%dB heartbeat=%ds '
        'recycle=%.0fs rate=%s', args.duration, args.threads, args.body_size,
        args.heartbeat, args.channel_recycle_period, rate_desc)
    LOGGER.info(
        'Failure thresholds: mem-growth=%.1f%% (after %.0fs warmup), '
        'divergence=%d (sustained %.0fs)', args.mem_growth_threshold,
        args.mem_growth_window, args.divergence_threshold,
        args.divergence_window)

    counters = Counters()
    failure = FailureState()

    body = b'x' * args.body_size
    publisher = Publisher(args.host, args.port, args.heartbeat, args.threads,
                          body, args.channel_recycle_period,
                          args.publish_rate, counters, failure)
    consumer = Consumer(args.host, args.port, args.heartbeat,
                        args.channel_recycle_period, counters, failure)
    consumer.start()
    publisher.start()

    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    LOGGER.info('Baseline RSS: %s', _human_bytes(baseline_rss))

    # Forward Ctrl-C through the failure state for a clean shutdown.
    def _handle_sigint(_signum, _frame):
        failure.fail('interrupted by user (SIGINT)')

    signal.signal(signal.SIGINT, _handle_sigint)

    start = time.monotonic()
    deadline = start + args.duration
    next_stats = start + args.stats_interval
    last_published = 0
    last_consumed = 0
    last_report = start

    diverged_since: float | None = None

    while time.monotonic() < deadline and failure.reason is None:
        time.sleep(0.5)
        now = time.monotonic()
        if now < next_stats:
            continue

        published = counters.published
        consumed = counters.consumed
        publish_rate = (published - last_published) / max(now - last_report,
                                                          1e-9)
        consume_rate = (consumed - last_consumed) / max(now - last_report,
                                                        1e-9)
        rss = proc.memory_info().rss
        diff = published - consumed

        elapsed = now - start
        LOGGER.info(
            't=%5.0fs  pub=%-9d (%6.0f/s)  con=%-9d (%6.0f/s)  '
            'diff=%-6d  rss=%s  recycles=%d',
            elapsed, published, publish_rate, consumed, consume_rate, diff,
            _human_bytes(rss), counters.channel_recycles)

        # Failure check 3: memory growth.
        if elapsed >= args.mem_growth_window:
            growth_pct = (rss - baseline_rss) / max(baseline_rss, 1) * 100.0
            if growth_pct > args.mem_growth_threshold:
                failure.fail(
                    'memory growth exceeded threshold',
                    f'rss grew {growth_pct:.1f}% over {elapsed:.0f}s '
                    f'(baseline={_human_bytes(baseline_rss)}, '
                    f'now={_human_bytes(rss)})')

        # Failure check 4: sustained publisher/consumer divergence in
        # either direction (positive = consumer falling behind, negative
        # = producer not keeping up which here would mean backpressure
        # or a stuck producer).
        if abs(diff) > args.divergence_threshold:
            if diverged_since is None:
                diverged_since = now
            elif now - diverged_since > args.divergence_window:
                failure.fail(
                    'publisher/consumer divergence',
                    f'diff={diff} sustained for '
                    f'{now - diverged_since:.0f}s (threshold='
                    f'{args.divergence_threshold}, window='
                    f'{args.divergence_window:.0f}s)')
        else:
            diverged_since = None

        # Failure check 2: heartbeat-driven close.
        if not publisher.conn.is_open:
            failure.fail('publisher connection unexpectedly closed')
        if not consumer.conn.is_open:
            failure.fail('consumer connection unexpectedly closed')

        last_published = published
        last_consumed = consumed
        last_report = now
        next_stats = now + args.stats_interval

    elapsed = time.monotonic() - start
    LOGGER.info('Soak loop exited at t=%.0fs', elapsed)

    publisher.shutdown()
    consumer.shutdown()

    LOGGER.info('Final counters: published=%d consumed=%d errors=(pub=%d, '
                'con=%d) recycles=%d', counters.published, counters.consumed,
                counters.publish_errors, counters.consume_errors,
                counters.channel_recycles)

    if failure.reason is not None:
        LOGGER.error('SOAK FAILED: %s', failure.reason)
        if failure.detail:
            LOGGER.error('  detail: %s', failure.detail)
        seen = 0
        while not failure.errors.empty() and seen < 10:
            where, tid, exc = failure.errors.get_nowait()
            LOGGER.error('  [%s thread=%d] %s: %s', where, tid,
                         type(exc).__name__, exc)
            seen += 1
        return 1

    LOGGER.info('SOAK PASSED: %.0fs elapsed, %d published, %d consumed',
                elapsed, counters.published, counters.consumed)
    return 0


if __name__ == '__main__':
    sys.exit(main())
