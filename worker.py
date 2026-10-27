"""Order Worker: consumes order messages, processes them, then sends an ACK.

Run several copies against the same queue to get competing consumers.

Examples:
  python worker.py --name worker-1
  python worker.py --name worker-2
  python worker.py --name worker-crash --crash-before-ack   # dies after receiving, before ACK
"""
import argparse
import fcntl
import json
import os
import time

from common import QUEUE, connect, declare_queue, get_logger

# Processed event_ids live in a small file so that ALL worker processes share
# them (an in-memory set would only be visible to one process). A database is
# not needed for this assignment.
STORE = os.getenv("PROCESSED_FILE", "processed_events.txt")
LOCK = STORE + ".lock"


class ProcessedStore:
    """Tiny file-backed set of processed event_ids, safe across processes."""

    def __enter__(self):
        self._lock = open(LOCK, "w")
        fcntl.flock(self._lock, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        fcntl.flock(self._lock, fcntl.LOCK_UN)
        self._lock.close()

    def seen(self, event_id):
        if not os.path.exists(STORE):
            return False
        with open(STORE) as f:
            return event_id in {line.strip() for line in f}

    def add(self, event_id):
        with open(STORE, "a") as f:
            f.write(event_id + "\n")


def main():
    parser = argparse.ArgumentParser(description="Order worker")
    parser.add_argument("--name", default=f"worker-{os.getpid()}")
    parser.add_argument("--work-seconds", type=float, default=1.0,
                        help="simulated processing time per order")
    parser.add_argument("--crash-before-ack", action="store_true",
                        help="DEMO: exit abruptly after receiving a message, before ACK")
    args = parser.parse_args()

    log = get_logger(args.name)

    conn = connect()
    channel = conn.channel()
    declare_queue(channel)
    # Fair dispatch: give each worker only one unacked message at a time.
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        order = json.loads(body)
        tag = " (REDELIVERED)" if method.redelivered else ""
        log.info("received%s event_id=%s order_id=%s", tag, order["event_id"], order["order_id"])

        if args.crash_before_ack:
            log.info("simulating crash: exiting WITHOUT sending ACK for order_id=%s", order["order_id"])
            os._exit(1)  # hard exit: connection drops, broker requeues the message

        # Check + process + record under one lock so two workers can never
        # run the business action for the same event_id at the same time.
        with ProcessedStore() as store:
            if store.seen(order["event_id"]):
                log.info("DUPLICATE detected event_id=%s order_id=%s -> skipping business action",
                         order["event_id"], order["order_id"])
            else:
                log.info("processing order_id=%s item_id=%s quantity=%s",
                         order["order_id"], order["item_id"], order["quantity"])
                time.sleep(args.work_seconds)  # simulated work
                store.add(order["event_id"])
                log.info("processed order_id=%s", order["order_id"])

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("ACK sent for order_id=%s", order["order_id"])

    channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
    log.info("waiting for messages on queue '%s' (Ctrl+C to stop)", QUEUE)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("stopping")
        channel.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
