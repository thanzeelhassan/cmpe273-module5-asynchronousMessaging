# Module 5 Assignment: Asynchronous Messaging with RabbitMQ

CMPE-273 – A small order-processing system built on RabbitMQ (Python + `pika`).

- **Order Producer** (`producer.py`) publishes order messages to a queue.
- **Order Worker** (`worker.py`) consumes, logs, processes, and ACKs orders.
- Producer and workers are **independent processes**; I run **two workers** on the same queue as competing consumers.

Each message contains `event_id`, `order_id`, `item_id`, `quantity`.

```
{"event_id": "6c77a446-ccc8-4e91-b576-9673138f123f", "order_id": "ORD-0001", "item_id": "ITEM-200", "quantity": 3}
```

## Architecture

```
                        RabbitMQ broker
                 ┌──────────────────────────┐
 ┌──────────┐    │   queue: "orders"        │    ┌────────────┐
 │ Producer │───▶│   (durable, persistent   │───▶│  Worker 1  │──┐
 │          │    │    messages)             │    └────────────┘  │ check/record
 └──────────┘    │                          │    ┌────────────┐  │ event_id
                 │   manual ACK, prefetch=1 │───▶│  Worker 2  │──┤
                 └──────────────────────────┘    └────────────┘  ▼
                                                   processed_events.txt
                                                   (shared, file-locked)
```

Key design choices:

| Concern | Choice |
|---|---|
| Durability | Durable queue + persistent messages |
| Competing consumers | Both workers consume the same queue; `prefetch_count=1` gives fair dispatch |
| Reliability | Manual ACK (`auto_ack=False`), sent only **after** processing finishes |
| Failure | If a worker dies before ACK, RabbitMQ requeues the message (`redelivered=True`) |
| Duplicates | Processed `event_id`s stored in a small file guarded by `flock`, shared by all workers |

## Running it

Requirements: Python 3.9+ and Docker (or any RabbitMQ install on `localhost:5672`).

### 1. Start RabbitMQ

```bash
docker compose up -d
# management UI (optional): http://localhost:15672  (guest / guest)
```

### 2. Install dependencies

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Start two workers (two terminals)

```bash
python worker.py --name worker-1
python worker.py --name worker-2
```

### 4. Publish orders (third terminal)

```bash
python producer.py --count 6
```

### Reproduce the failure and duplicate demos by hand

```bash
# Failure / redelivery: this worker exits right after receiving a message, before ACK
python worker.py --name worker-1 --crash-before-ack
python worker.py --name worker-2
python producer.py --count 3

# Duplicate: re-publishes the last order with the SAME event_id
python producer.py --count 3 --duplicate
```

### Or run every scenario automatically

```bash
./scripts/run_demo.sh      # writes logs/1_*.log, logs/2_*.log, logs/3_*.log
```

Set `RABBIT_HOST` if the broker is not on `localhost`.

## Evidence

Full logs are in [`logs/`](logs/). Excerpts below. (Timestamps are `HH:MM:SS.mmm`.)

### 1. Successful message processing

Every message is received, logged, processed, and then ACKed
([`logs/1_success_competing_consumers.log`](logs/1_success_competing_consumers.log)):

```
18:44:02.870 [worker-1] processing order_id=ORD-0002 item_id=ITEM-300 quantity=2
18:44:03.870 [worker-1] processed order_id=ORD-0002
18:44:03.871 [worker-1] ACK sent for order_id=ORD-0002
```

### 2. Two competing consumers

Same queue, same run: the six orders are split between the two workers and **each order is handled by exactly one worker**:

| Order | Handled by |
|---|---|
| ORD-0001 | worker-2 |
| ORD-0002 | worker-1 |
| ORD-0003 | worker-1 |
| ORD-0004 | worker-2 |
| ORD-0005 | worker-1 |
| ORD-0006 | worker-2 |

```
18:44:02.869 [worker-2] received ... order_id=ORD-0001
18:44:02.869 [worker-1] received ... order_id=ORD-0002
18:44:03.872 [worker-1] received ... order_id=ORD-0003
18:44:04.873 [worker-2] received ... order_id=ORD-0004
```

### 3. Failure and redelivery

worker-1 receives ORD-0001 and dies **before** sending the ACK. RabbitMQ requeues it and worker-2 receives it again, flagged `REDELIVERED`. The system keeps running
([`logs/2_failure_and_redelivery.log`](logs/2_failure_and_redelivery.log)):

```
18:44:17.182 [worker-1] received event_id=66f9bb2e-... order_id=ORD-0001
18:44:17.182 [worker-1] simulating crash: exiting WITHOUT sending ACK for order_id=ORD-0001
18:44:18.187 [worker-2] received (REDELIVERED) event_id=66f9bb2e-... order_id=ORD-0001
18:44:18.187 [worker-2] processing order_id=ORD-0001 item_id=ITEM-300 quantity=3
18:44:19.187 [worker-2] ACK sent for order_id=ORD-0001
```

1. The first worker receives the message.
2. The message is not acknowledged.
3. Another worker receives the same message again.

### 4. Duplicate handling

The producer publishes ORD-0003 twice with the same `event_id`. worker-2 processes it; worker-1 receives the duplicate, detects it, skips the business action, and still ACKs
([`logs/3_duplicate_handling.log`](logs/3_duplicate_handling.log)):

```
18:44:27.549 [producer] published event_id=3381748c-... order_id=ORD-0003 ...
18:44:27.549 [producer] published (DUPLICATE, same event_id) event_id=3381748c-... order_id=ORD-0003 ...
18:44:29.553 [worker-2] processing order_id=ORD-0003 item_id=ITEM-300 quantity=2
18:44:30.553 [worker-2] processed order_id=ORD-0003
18:44:30.553 [worker-1] DUPLICATE detected event_id=3381748c-... order_id=ORD-0003 -> skipping business action
18:44:30.554 [worker-1] ACK sent for order_id=ORD-0003
```

The duplicate check, the business action, and the recording of the `event_id` all happen under one file lock, so two workers can never process the same `event_id` at the same time. The `event_id` is recorded *before* the ACK, so if a worker crashes between the two, the redelivery is recognized as a duplicate instead of being processed twice.

## Reflection

A message can be delivered more than once because RabbitMQ guarantees *at-least-once* delivery: if a worker receives a message but crashes, or its connection drops, before the ACK reaches the broker, the broker cannot know whether the work was done, so it requeues the message (as in the failure demo). Producers that retry a publish after a timeout can also create duplicates. Idempotent handling makes this safe: by recording each `event_id` and skipping ones it has already processed, a worker can accept the same message any number of times while the business action (e.g. charging or shipping an order) happens only once. This trades a small amount of bookkeeping for correctness, and it is what lets us use ACK-after-processing without fearing redelivery.

## Repository layout

```
producer.py            publishes order messages
worker.py              consumes, processes, ACKs, de-duplicates
common.py              shared connection/logging helpers
docker-compose.yml     RabbitMQ (with management UI)
requirements.txt
scripts/run_demo.sh    runs all three scenarios and writes logs/
scripts/purge_queue.py clears the queue between demos
logs/                  captured evidence
```
