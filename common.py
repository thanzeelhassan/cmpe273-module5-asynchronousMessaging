"""Shared settings and helpers for the order producer and workers."""
import logging
import os
import sys

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
QUEUE = os.getenv("ORDERS_QUEUE", "orders")


def connect():
    params = pika.ConnectionParameters(host=RABBIT_HOST, heartbeat=30)
    return pika.BlockingConnection(params)


def declare_queue(channel):
    # Durable queue: survives a broker restart.
    channel.queue_declare(queue=QUEUE, durable=True)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s.%(msecs)03d [%(name)s] %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger
