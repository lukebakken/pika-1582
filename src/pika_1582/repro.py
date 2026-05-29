"""Reproducer for pika #1144 / #511.

The original bug: multiple threads call ``basic_publish`` on a channel of a
shared connection; ``_tx_buffers`` is mutated from a non-IOLoop thread,
producing ``IndexError: pop from an empty deque`` (or
``StreamLostError`` from the IOLoop's ``_AsyncBaseTransport._produce``
chain).

This script exercises the same shape using ``ThreadSafeConnection``: N
publisher threads share a single ``ThreadSafeChannel`` and hammer
``basic_publish`` for *duration* seconds.  Every publish goes through
``add_callback_threadsafe`` internally, so ``_tx_buffers`` is only ever
touched on the IOLoop thread and the race cannot occur.

Acceptance: zero exceptions of any kind over the full run.  The script
exits non-zero on any error.
"""
from __future__ import annotations

import argparse
import logging
import queue
import sys
import threading
import time

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

LOGGER = logging.getLogger('pika-1582.repro')


def _publish_loop(channel, stop: threading.Event,
                  errors: queue.Queue, counter: list, body: bytes,
                  thread_id: int) -> None:
    """Hammer basic_publish until stop is set."""
    local = 0
    while not stop.is_set():
        try:
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=ROUTING_KEY,
                body=body,
                properties=pika.BasicProperties(
                    content_type='application/octet-stream',
                    delivery_mode=pika.DeliveryMode.Transient,
                ),
            )
            local += 1
            counter[thread_id] = local
        except Exception as exc:  # noqa: BLE001 - we want to catch everything
            errors.put(('publish', thread_id, exc))
            return


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5672)
    parser.add_argument('--threads', type=int, default=8,
                        help='Number of publisher threads (default: 8)')
    parser.add_argument('--duration', type=parse_duration, default=30,
                        help='How long to hammer basic_publish (default: 30s)')
    parser.add_argument('--body-size', type=int, default=128,
                        help='Message body size in bytes (default: 128)')
    parser.add_argument('--heartbeat', type=int, default=30,
                        help='Heartbeat seconds (default: 30)')
    parser.add_argument('--log-level', default='INFO')
    args = parser.parse_args(argv)

    setup_logging(args.log_level)

    LOGGER.info(
        'Starting repro: %d threads x %.0fs against %s:%d (body=%d bytes)',
        args.threads, args.duration, args.host, args.port, args.body_size)

    conn = open_connection(args.host, args.port, args.heartbeat)
    ch = conn.channel()
    declare_topology(ch)
    # Drain anything left over from previous runs so consumers downstream
    # see only fresh traffic.
    ch.queue_purge(QUEUE_NAME)
    LOGGER.info('Topology declared, queue purged')

    body = b'x' * args.body_size
    stop = threading.Event()
    errors: queue.Queue = queue.Queue()
    counter = [0] * args.threads

    threads = [
        threading.Thread(
            target=_publish_loop,
            args=(ch, stop, errors, counter, body, i),
            name=f'pub-{i}',
            daemon=True,
        )
        for i in range(args.threads)
    ]

    start = time.monotonic()
    for t in threads:
        t.start()

    deadline = start + args.duration
    last_report = start
    last_total = 0
    try:
        while time.monotonic() < deadline:
            time.sleep(min(5.0, deadline - time.monotonic()))
            now = time.monotonic()
            total = sum(counter)
            rate = (total - last_total) / max(now - last_report, 1e-9)
            LOGGER.info('  publishes=%d (+%d, %.0f msg/s)',
                        total, total - last_total, rate)
            last_total = total
            last_report = now
            if not errors.empty():
                break
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=10)

    elapsed = time.monotonic() - start
    total = sum(counter)
    LOGGER.info('Stopped after %.1fs.  Total publishes: %d (%.0f msg/s)',
                elapsed, total, total / elapsed)

    failures = []
    while not errors.empty():
        failures.append(errors.get_nowait())

    try:
        conn.close()
    except Exception as exc:  # noqa: BLE001
        failures.append(('close', -1, exc))

    if failures:
        LOGGER.error('Repro FAILED with %d error(s):', len(failures))
        for where, tid, exc in failures:
            LOGGER.error('  [%s thread=%d] %s: %s', where, tid,
                         type(exc).__name__, exc)
        return 1

    LOGGER.info('Repro PASSED: zero exceptions across %d threads, %d publishes',
                args.threads, total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
