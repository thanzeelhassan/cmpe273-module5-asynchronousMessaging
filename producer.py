"""Order Producer: publishes order messages to a RabbitMQ queue.

Each message has: event_id, order_id, item_id, quantity.

Examples:
  python producer.py --count 6
  python producer.py --count 3 --duplicate      # also re-publishes the last event_id
"""
import argparse
import json
import random
import uuid

import pika

from common import QUEUE, connect, declare_queue, get_logger

log = get_logger("producer")

ITEMS = ["ITEM-100", "ITEM-200", "ITEM-300", "ITEM-400"]


def make_order(n):
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{n:04d}",
        "item_id": random.choice(ITEMS),
        "quantity": random.randint(1, 5),
    }


def publish(channel, order, note=""):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,  # survive broker restart
            content_type="application/json",
        ),
    )
    log.info(
        "published%s event_id=%s order_id=%s item_id=%s quantity=%s",
        note, order["event_id"], order["order_id"], order["item_id"], order["quantity"],
    )


def main():
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=5, help="number of orders")
    parser.add_argument("--start", type=int, default=1, help="first order number")
    parser.add_argument(
        "--duplicate",
        action="store_true",
        help="publish the last order a second time with the SAME event_id",
    )
    args = parser.parse_args()

    conn = connect()
    channel = conn.channel()
    declare_queue(channel)

    last = None
    for n in range(args.start, args.start + args.count):
        last = make_order(n)
        publish(channel, last)

    if args.duplicate and last:
        publish(channel, last, note=" (DUPLICATE, same event_id)")

    conn.close()


if __name__ == "__main__":
    main()
