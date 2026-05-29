"""Shared helpers for the pika-1582 reproducer and soak harness."""
from __future__ import annotations

import argparse
import logging
import re

import pika
from pika.adapters.thread_safe_connection import ThreadSafeConnection
from pika.exchange_type import ExchangeType

EXCHANGE_NAME = 'pika-1582'
QUEUE_NAME = 'pika-1582'
ROUTING_KEY = 'pika-1582'

LOG_FORMAT = '%(asctime)s %(levelname)-7s %(name)-22s %(message)s'

_DURATION_RE = re.compile(r'^(\d+)([smhd])$')


def parse_duration(text: str) -> float:
    """Parse '30s', '5m', '4h', '1d' into seconds.  Bare numbers are seconds."""
    if text.isdigit():
        return float(text)
    m = _DURATION_RE.match(text)
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid duration {text!r} (expected e.g. 30s, 5m, 4h, 1d)")
    value, unit = int(m.group(1)), m.group(2)
    return value * {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[unit]


def setup_logging(level: str = 'INFO') -> None:
    logging.basicConfig(level=level, format=LOG_FORMAT)
    # Quiet pika's chatty INFO logs so harness output is readable.
    for noisy in ('pika', 'pika.adapters', 'pika.connection', 'pika.channel',
                  'pika.adapters.utils.connection_workflow',
                  'pika.adapters.utils.io_services_utils'):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def make_parameters(host: str = 'localhost',
                    port: int = 5672,
                    heartbeat: int = 30) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=pika.PlainCredentials('guest', 'guest'),
        heartbeat=heartbeat,
        # Keep the blocked-connection timeout reasonable for soak runs.
        blocked_connection_timeout=300,
    )


def declare_topology(channel) -> None:
    """Declare the shared exchange + queue + binding used by both publisher
    and consumer."""
    channel.exchange_declare(exchange=EXCHANGE_NAME,
                             exchange_type=ExchangeType.direct,
                             durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME,
                       exchange=EXCHANGE_NAME,
                       routing_key=ROUTING_KEY)


def open_connection(host: str = 'localhost',
                    port: int = 5672,
                    heartbeat: int = 30) -> ThreadSafeConnection:
    return ThreadSafeConnection(make_parameters(host, port, heartbeat))


__all__ = [
    'EXCHANGE_NAME',
    'QUEUE_NAME',
    'ROUTING_KEY',
    'LOG_FORMAT',
    'parse_duration',
    'setup_logging',
    'make_parameters',
    'declare_topology',
    'open_connection',
]
