# Kafka Share Groups Demo: Implementation Plan

Oct 30, 2026 · @Jagadish

## Goal and scope

Build `DemoKafkaShareGroups`, an interactive Java CLI plus a Docker Compose setup that demonstrates Kafka 4.2 share groups live on a terminal: create a topic, produce N records, run share-consumer workers, and watch each record move through its states on a board. It is for a conference talk, so readable output matters more than performance.

**In scope**

- Docker Compose with one Kafka 4.2 broker in KRaft mode.
- A Java CLI with a numbered menu (create topic, produce, start workers, board, group internals, poison record, consumer-group comparison).
- Share-consumer workers using explicit acknowledgement.
- A `demo-events` topic and a live terminal board built from those events.

**Out of scope**

- Web UI, multi-broker clusters, TLS or auth, persistence beyond the Compose volume, production hardening.

**Definition of done** The scripted run in the Verification section passes on a clean `docker compose up`, and every menu option prints output a non-expert can follow.

## Stack and project layout

Use one Maven module with minimal dependencies so the code stays easy to read on a slide.

| Item | Choice |
| --- | --- |
| Language | Java 21 (17 is the minimum) |
| Build | Maven |
| Kafka client | `org.apache.kafka:kafka-clients:4.2.0` (confirm the artifact exists) |
| JSON | Jackson |
| Terminal | Plain ANSI escape codes and `Scanner`; no TUI framework |
| Logging | SLF4J simple logger writing to `kdemo.log`, never to the terminal, so the board stays clean |

```text
DemoKafkaShareGroups/
  docker-compose.yml
  pom.xml
  scripts/e2e.sh
  src/main/java/org/example
    Main.java               menu loop
    Config.java             bootstrap server, topic and group names
    TopicService.java       Admin API: create and describe topics
    ProducerService.java    sends N records, prints partition and offset
    WorkerPool.java         starts and stops N worker threads
    ShareWorker.java        KafkaShareConsumer loop
    ConsumerGroupWorker.java  classic consumer for the comparison
    EventBus.java           publishes and reads demo-events
    Board.java              ANSI board renderer
    ShellRunner.java        runs docker exec commands and echoes them
```

Each worker owns its own `KafkaShareConsumer` on its own thread, because the consumer is not thread-safe.

## Docker setup

One broker acting as both broker and controller is enough. The share coordinator's internal state topic defaults to a replication factor above 1, so it must be lowered or share groups will not work on a single node.

```yaml
services:
  kafka:
    image: apache/kafka:4.2.0        # confirm this tag exists
    container_name: kafka
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_GROUP_SHARE_DELIVERY_COUNT_LIMIT: 3          # default is 5; shorter for the demo
      KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS: 1000  # allows a short lock
```

**Startup checks the agent must run**

1. `docker compose up -d`, then wait until `kafka-topics.sh --bootstrap-server localhost:9092 --list` succeeds inside the container.
2. `kafka-features.sh --bootstrap-server localhost:9092 describe | grep share` must show `share.version` finalized at level 1. If it is not, run `kafka-features.sh ... upgrade --feature share.version=1`.
3. Create the demo group's config before any consumer joins:

```bash
kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --entity-type groups --entity-name demo-share \
  --add-config share.auto.offset.reset=earliest,share.record.lock.duration.ms=5000
```

The `earliest` reset matters: a new share group starts at the latest offset by default, so records produced before the workers start would be skipped.

## Event model

The broker does not expose per-record state, so every component publishes one JSON event per state change to a single-partition `demo-events` topic, and the board replays them. Say this openly during the talk, then cross-check the board against the real `--describe` output.

Every event has these fields: `ts` (epoch ms), `event`, `topic`, `partition`, `offset`, `worker` (null for produced), `deliveryCount` (null for produced) and `note` (optional text).

| Event | Emitted by | Board state it sets |
| --- | --- | --- |
| `PRODUCED` | Producer, from the send callback | Available |
| `ACQUIRED` | Worker, when a polled record arrives (carries `record.deliveryCount()`) | Acquired by that worker |
| `ACCEPTED` | Worker, after ACCEPT | Acknowledged |
| `RELEASED` | Worker, after RELEASE | Available, with the delivery count kept |
| `REJECTED` | Worker, after REJECT | Archived |
| `ARCHIVED` | Worker, when it releases a record whose delivery count has reached the configured limit | Archived |

Lock expiry is never emitted. The board infers it when a record it shows as Acquired by one worker arrives as `ACQUIRED` at another worker with a higher delivery count, and labels it "redelivered after lock expiry".

Rules

- The events producer uses `acks=all` and flushes after every event; throughput does not matter.
- The board reads with a plain `KafkaConsumer`, a unique group id per run, and `auto.offset.reset=earliest`.
- `Config.DELIVERY_LIMIT` must equal the broker's delivery limit (3 in the Compose file).

## CLI modules

`Main` shows a numbered menu in a loop and calls one service per option. Each option prompts for its inputs with a default in brackets, so pressing Enter accepts the default.

| # | Option | Behaviour | Acceptance criteria |
| --- | --- | --- | --- |
| 1 | Create topic | Prompts for name and partitions \[3\]. Creates it with the Admin API, creates `demo-events` if missing, then runs `kafka-topics.sh --describe` and prints the output. | Topic exists with the requested partition count; running it twice does not fail. |
| 2 | Produce | Prompts for topic, count \[100\] and optional poison offsets (blank for none). Sends each record to partition `i % partitions` explicitly. Value is JSON with `id`, `durationMs` (random 200 to 2000) and `poison`. Prints one line per partition on completion. | The per-partition counts add up to N, and one `PRODUCED` event exists per record. |
| 3 | Start workers | Prompts for count \[6\] and mode (share or classic consumer group). Share mode uses group `demo-share` and explicit acknowledgement. Each worker sleeps `durationMs`, then ACCEPTs. A poison record is always RELEASEd. | All N records end Acknowledged or Archived, and no record stays Acquired. |
| 4 | Board | Renders the live board (see next section) until Enter is pressed. | Updates at least twice a second without flicker. |
| 5 | Group internals | Echoes then runs `kafka-share-groups.sh --describe`, `--members` and `--state` for `demo-share` through `docker exec`. | Shows real broker output, preceded by the exact command. |
| 6 | Inspect storage | Finds the partition 0 segment inside the container, then echoes and runs `kafka-dump-log.sh --print-data-log` on it. | Shows the produced records in the ordinary log. |
| 7 | Kill worker | Prompts for a worker number. Stops that worker's polling without acknowledging or closing, simulating a crash. | After the lock duration, the record it held is redelivered to another worker with a higher delivery count. |
| 8 | Reset | Stops all workers, deletes the demo topic, the share group and `demo-events`, then recreates `demo-events`. | A fresh run starts with an empty board. |

## Worker and board behaviour

**Share worker loop**

```java
// config: group.id=demo-share, share.acknowledgement.mode=explicit,
//         share.acquire.mode=record_limit, max.poll.records=1
consumer.subscribe(List.of(topic));
while (running) {
  for (ConsumerRecord<String,String> r : consumer.poll(Duration.ofMillis(500))) {
    emit(ACQUIRED, r, r.deliveryCount());
    sleep(durationMs(r));
    if (poison(r)) {
      consumer.acknowledge(r, AcknowledgeType.RELEASE);
      emit(deliveryCount >= DELIVERY_LIMIT ? ARCHIVED : RELEASED, r);
    } else {
      consumer.acknowledge(r, AcknowledgeType.ACCEPT);
      emit(ACCEPTED, r);
    }
  }
  consumer.commitSync();
}
```

- In explicit mode every polled record must be acknowledged before the next poll.
- `share.acquire.mode=record_limit` is needed so one worker does not grab a large batch and hide the parallelism. The default `batch_optimized` treats `max.poll.records` as a soft limit. Confirm the setting exists in 4.2.
- Classic mode uses a normal `KafkaConsumer` on group `demo-classic` with `max.poll.records=1` and a `commitSync` per record. It ignores the poison flag, so the comparison shows idle workers and a slow record blocking its partition.

**Board layout**

```text
 orders  · Available  A Acquired  ✓ Acknowledged  ✗ Archived
 Available 41   Acquired 6   Acknowledged 51   Archived 2

 orders-0  start offset 12
   ✓✓✓✓✓✓✓✓✓✓✓✓AAA····················
 orders-1  start offset 9
   ✓✓✓✓✓✓✓✓✓AA·····················

 Workers
   w1  orders-0@12  processing 1.4s   done 17
   w2  idle                          done 15

 Recent events
   12:04:11  w3  orders-1@9  RELEASED  delivery 2
```

- One symbol per record, 50 per line, in colour: grey, yellow, green, red.
- The start offset per partition comes from the group's share-partition offsets, polled once a second. Use the Admin API if it exposes them in 4.2, otherwise parse `kafka-share-groups.sh --describe`.
- The workers panel shows idle workers explicitly, because idle workers are the point of the consumer-group comparison.
- The recent-events panel shows the last 8 events with delivery counts, so redelivery is visible.
- Redraw every 250 ms using ANSI clear and home, and exit on Enter.

## Build order

Work in this order and run the result after each milestone. If a Kafka 4.2 API or setting differs from this plan, stop, say what differs, and adjust the plan before continuing.

- [ ] **M1 Broker:** Compose file up, startup checks pass, and a smoke test with `kafka-console-share-consumer.sh` receives a record.
- [ ] **M2 Skeleton:** Maven project, `Config`, `Main` menu loop, `ShellRunner`.
- [ ] **M3 Topics:** `TopicService` and menu option 1.
- [ ] **M4 Producer:** `EventBus`, `ProducerService` and menu option 2, with `PRODUCED` events appearing in `demo-events`.
- [ ] **M5 Share workers:** `ShareWorker`, `WorkerPool` and menu option 3; every record ends Acknowledged.
- [ ] **M6 Board:** menu option 4 with states, worker panel and recent events.
- [ ] **M7 Failures:** poison handling, delivery counts, archiving at the limit, and menu option 7 (kill worker).
- [ ] **M8 Internals:** menu options 5 and 6 (group describe and storage inspection).
- [ ] **M9 Comparison:** classic consumer-group mode with idle workers visible on the board.
- [ ] **M10 Finish:** menu option 8 (reset), `scripts/e2e.sh` and a README with run instructions.

## Verification and risks

**End-to-end run (`scripts/e2e.sh`)**

| Step | Expected result |
| --- | --- |
| Clean `docker compose up -d` and startup checks | `share.version` finalized at level 1 |
| Create `orders` with 3 partitions, produce 100 records with one poison record | Per-partition counts 34, 33, 33; 100 `PRODUCED` events |
| Start 6 share workers | Within 60 s: 99 Acknowledged, 1 Archived; the poison record reached delivery count 3; at least 4 distinct workers processed records |
| Run group internals | `--describe` shows start offsets that match the board; `--members` lists 6 members |
| Kill a worker mid-record | After the lock duration, another worker gets that record with delivery count 2 |
| Reset, then start 6 classic workers | Exactly 3 workers have processed records and 3 are idle |
| Inspect storage | The dump shows 34 records for partition 0 |

**Items to verify against the Kafka 4.2 docs before relying on them**

- The `apache/kafka:4.2.0` image tag and `kafka-clients:4.2.0` artifact.
- Whether `share.version` is already finalized at level 1 by default.
- That `share.acquire.mode` exists in 4.2 and gives one-record fetches with `max.poll.records=1`.
- Group-level config names set through `kafka-configs.sh --entity-type groups`, and the broker setting names for the delivery limit and minimum lock duration.
- The return type of `record.deliveryCount()` (assumed to be an optional number).
- Whether the Admin API exposes share-partition start offsets; if not, parse the CLI output.
- The log directory inside the container, used to find the segment for `kafka-dump-log.sh`.
- That a record released at the delivery limit is archived, since the board's `ARCHIVED` event assumes it.
- Share groups can only be deleted when they have no active members, so Reset must stop workers first.
