# Kafka Share Groups Demo: Implementation Plan and Current Status

Updated Oct 30, 2026

## Goal

`DemoKafkaShareGroups` is an interactive Java CLI and Docker Compose demo for Kafka 4.2 share groups. The primary flow is:

1. Create the `orders` topic.
2. Produce records, optionally marking selected records as poison records.
3. Start a configurable number of share workers.
4. Watch application-generated state events on the board.
5. Inspect the real broker-side share-group offsets, members, and state.
6. Inspect the ordinary Kafka log.
7. Stop one worker and observe a rebalance/redelivery.
8. Reset the demo for a clean run.

The demo prioritizes readable terminal output over throughput. `demo-events` is an internal, single-partition topic used by the application to maintain the board's state history. It is not a Kafka-provided per-record state stream.

## Scope

Implemented:

- One Kafka 4.2 broker in KRaft mode.
- Java 21 Maven application.
- Kafka Admin API topic creation and inspection.
- Explicit-acknowledgement `KafkaShareConsumer` workers.
- Poison-record release and archive demonstration.
- Board based on events written to `demo-events`.
- Broker diagnostics through `kafka-share-groups.sh`.
- Partition-0 log inspection through `kafka-dump-log.sh`.
- Worker shutdown/rebalance demonstration.
- Reset and startup guards for repeatable runs.

Currently retained for future comparison, but not exposed by menu option 3:

- `ConsumerGroupWorker`, which uses a normal Kafka consumer group named `demo-classic`.

Not currently implemented as originally sketched:

- The full ANSI partition grid and worker-status panel.
- Automatic classic-mode selection from the CLI.
- A full automated end-to-end assertion script.
- Dynamic topic selection for workers and the board; use `orders` for the demo flow.

## Project layout

```text
DemoKafkaShareGroups/
  docker-compose.yaml
  pom.xml
  scripts/e2e.sh
  scripts/e2e.ps1
  src/main/java/org/example
    Main.java                 menu loop and guards
    Config.java               bootstrap, topic, group, and delivery constants
    TopicService.java         Admin API topic operations
    ProducerService.java      record producer and PRODUCED events
    WorkerPool.java           starts/stops worker threads
    ShareWorker.java          KafkaShareConsumer share loop
    ConsumerGroupWorker.java  retained classic consumer implementation
    EventBus.java             internal demo-events producer/reader
    Board.java                event summary renderer
    ShellRunner.java          docker command runner
    KafkaAdmin.java           original protected skeleton; unchanged
  src/main/resources/
    simplelogger.properties   writes SLF4J simple logs to kdemo.log
```

Each share worker owns its own `KafkaShareConsumer` on its own thread. Kafka consumers are not thread-safe.

## Build dependencies

| Item | Current choice |
| --- | --- |
| Language | Java 21 |
| Build | Maven |
| Kafka client | `org.apache.kafka:kafka-clients:4.2.0` |
| Logging API | Kafka's SLF4J API dependency |
| Logging implementation | `org.slf4j:slf4j-simple:1.7.36` |
| JSON | Hand-built JSON strings; Jackson is not used |
| Terminal | `Scanner`, plain text, and polling; no TUI framework |

Build and run:

```powershell
mvn clean package dependency:copy-dependencies
java -cp "target\classes;target\dependency\*" org.example.Main
```

Logs are written to `kdemo.log` through `simplelogger.properties`, so Kafka client logs do not clutter the board terminal.

## Docker setup

The broker is both broker and controller. The share state topic replication settings are set to one because this is a single-node cluster. Automatic topic creation is disabled so a typo or an incorrect startup order cannot silently create a one-partition topic.

```yaml
services:
  kafka:
    image: apache/kafka:4.2.0
    container_name: kafka
    ports:
      - "9092:9092"
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
      KAFKA_GROUP_SHARE_DELIVERY_COUNT_LIMIT: 3
      KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS: 1000
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```

Start the broker:

```powershell
docker compose up -d
```

The setup scripts perform basic startup checks and configure the group:

```powershell
.\scripts\e2e.ps1
```

The shell version is available for a Unix-like shell:

```bash
./scripts/e2e.sh
```

The group-level configuration is:

```text
share.auto.offset.reset=earliest
share.record.lock.duration.ms=5000
```

`auto.offset.reset` must not be passed as a client property to `KafkaShareConsumer`; Kafka 4.x rejects that setting for share consumers. The reset policy is configured at the group level instead.

## Configuration

`Config.java` currently defines:

```text
BOOTSTRAP       localhost:9092, overridable with -Dkafka.bootstrap
TOPIC           orders
EVENTS_TOPIC    demo-events
SHARE_GROUP     demo-share
CLASSIC_GROUP   demo-classic (retained for future comparison)
DELIVERY_LIMIT  3
```

`demo-share` is the Kafka share-group ID. All Java share workers join that group and cooperate on the `orders` topic.

## Event model

Kafka's share protocol maintains delivery state, but the demo board needs a readable event history. The application therefore writes one JSON event per relevant state change to the internal `demo-events` topic.

Each event contains:

```text
ts, event, topic, partition, offset, worker, deliveryCount, note
```

The producer waits for the Kafka producer send to complete with `acks=all` before emitting `PRODUCED`. For worker events, the worker calls `acknowledge`, calls `commitSync`, and emits the final event only after the acknowledgement commit succeeds.

| Event | Produced by | Board meaning |
| --- | --- | --- |
| `PRODUCED` | `ProducerService` | Available |
| `ACQUIRED` | `ShareWorker` after `poll` | Acquired by a worker |
| `ACCEPTED` | `ShareWorker` after successful `ACCEPT` commit | Acknowledged |
| `RELEASED` | `ShareWorker` after poison `RELEASE` commit | Available again |
| `ARCHIVED` | `ShareWorker` when poison delivery reaches 3 | Archived by the demo |

`deliveryCount` is supplied by Kafka through `ConsumerRecord.deliveryCount()`. `PRODUCED` events intentionally have a null delivery count because no share delivery has happened yet.

The board's `ACCEPTED`, `RELEASED`, and `ARCHIVED` labels are application events used for visualization. Kafka's authoritative share state can be checked with option 5.

## CLI behavior

`Main` exposes this menu:

```text
1 Create topic
2 Produce records
3 Start workers
4 Board
5 Group internals
6 Inspect storage
7 Kill worker
8 Reset
0 Exit
```

### Option 1: Create topic

- Prompts for topic name, default `orders`.
- Prompts for partition count, default `3`.
- Creates the requested topic idempotently with replication factor 1.
- Silently ensures the internal `demo-events` topic exists with one partition.
- Configures `demo-share` with earliest reset and a five-second lock duration.
- Runs `kafka-topics.sh --describe` and prints the real topic description.

The internal topic is silent so the CLI does not confuse it with the application topic.

### Option 2: Produce records

- Verifies that the requested topic already exists.
- Rejects the operation if an external `console-share-consumer` is still in `demo-share`.
- Prompts for count, default `100`.
- Prompts for poison IDs/offset inputs, blank by default.
- Sends each record explicitly to partition `i % partitionCount`.
- Record value:

```json
{"id":7,"durationMs":1200,"poison":true}
```

- Emits one `PRODUCED` event per record.
- Prints per-partition totals after production.

Use the `orders` topic for the complete demo. Although the producer prompts for a topic, workers and the board currently use `Config.TOPIC` (`orders`).

### Option 3: Start workers

The current CLI is share-only. It asks only for worker count, default `6`:

```text
Worker count [6]:
```

Internally it starts `ShareWorker` instances using:

```text
group.id=demo-share
share.acknowledgement.mode=explicit
share.acquire.mode=record_limit
max.poll.records=1
```

Before starting, it verifies that `orders` exists, reapplies the group configuration, and rejects startup if an external console share consumer is active in `demo-share`.

Each worker sleeps for the record's `durationMs`. Normal records receive `ACCEPT`. Poison records receive `RELEASE`; at delivery count 3 the board emits `ARCHIVED`.

`ConsumerGroupWorker` remains in the source tree for a future classic comparison, but the CLI no longer asks for a mode.

### Option 4: Board

The board creates a unique normal Kafka consumer group with `auto.offset.reset=earliest`, replays `demo-events`, and summarizes the latest state for each `partition@offset` key.

It displays:

```text
Available
Acquired
Acknowledged
Archived
```

It also displays the last eight events and their delivery counts. Press Enter to return to the menu.

The current board is a compact text summary. It does not yet render the originally planned colored 50-record partition grid or worker-status panel.

### Option 5: Group internals

Runs these non-interactive commands for `demo-share`:

```text
kafka-share-groups.sh --describe ... --offsets
kafka-share-groups.sh --describe ... --members
kafka-share-groups.sh --describe ... --state
```

The option does not start `kafka-console-share-consumer.sh`. This avoids adding an external consumer that competes with the Java workers and avoids blocking the CLI.

If `demo-share` does not exist yet, the member guard skips the lookup without treating `GroupIdNotFoundException` as a demo failure.

### Option 6: Inspect storage

Searches both `/tmp/kafka-logs` and `/var/lib/kafka/data` for a segment under `orders-0`, then runs:

```text
kafka-dump-log.sh --files <segment> --print-data-log
```

The path is image-dependent, so the command no longer assumes `/var/lib/kafka/data`.

### Option 7: Kill worker

Prompts for a worker number. The selected worker:

- Stops polling.
- Wakes/interrupts its consumer.
- Closes without acknowledging its current record.
- Lets Kafka release the unacknowledged record and remove the member.
- Allows the remaining share workers to rebalance.

This is a controlled failure simulation. It is not a literal JVM crash; closing the consumer makes the rebalance deterministic for the demo. The worker shutdown path clears the thread interrupt flag before Kafka close so the expected cleanup does not print an `InterruptException` stack trace.

### Option 8: Reset

- Stops Java workers.
- Deletes the `demo-share` group so old share-partition offsets cannot affect a recreated topic.
- Refuses to continue if an external member prevents group deletion.
- Deletes `orders` and `demo-events`.
- Recreates `demo-events` silently.

After reset, run option 1 before producing or starting workers. The CLI rejects production or worker startup when `orders` does not exist. This prevents Kafka from silently creating a one-partition topic with the wrong startup state.

## Share worker behavior

The implemented worker flow is conceptually:

```java
consumer.subscribe(List.of("orders"));
while (running) {
    for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
        int delivery = record.deliveryCount().orElse((short) 1).intValue();
        emit("ACQUIRED", record, delivery);
        sleep(durationMs(record));

        if (poison(record)) {
            consumer.acknowledge(record, AcknowledgeType.RELEASE);
            consumer.commitSync();
            emit(delivery >= 3 ? "ARCHIVED" : "RELEASED", record, delivery);
        } else {
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
            consumer.commitSync();
            emit("ACCEPTED", record, delivery);
        }
    }
}
consumer.close();
```

The event is emitted after `commitSync`, because `acknowledge` updates local client state and `commitSync` sends the acknowledgement state to Kafka. The worker uses `record_limit` so `max.poll.records=1` is enforced as a one-record fetch limit rather than a soft batch hint.

## Verification

### Clean run

```powershell
docker compose up -d --force-recreate
.\scripts\e2e.ps1
mvn clean package dependency:copy-dependencies
java -cp "target\classes;target\dependency\*" org.example.Main
```

Then select:

```text
1 Create orders with 3 partitions
2 Produce 100 records and optionally mark one poison record
3 Start 6 share workers
4 Open the board
5 Inspect share-group internals
6 Inspect partition-0 storage
7 Kill a worker while it is processing
8 Reset
```

### Useful broker checks

```powershell
docker exec kafka /opt/kafka/bin/kafka-share-groups.sh --describe --bootstrap-server localhost:9092 --group demo-share
docker exec kafka /opt/kafka/bin/kafka-share-groups.sh --describe --bootstrap-server localhost:9092 --group demo-share --members
docker exec kafka /opt/kafka/bin/kafka-share-groups.sh --describe --bootstrap-server localhost:9092 --group demo-share --state
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders --time -1
```

The share-group `START-OFFSET` and `LAG` describe share-processing state; they are not the same as the physical Kafka log length. The original records remain in the `orders` log until normal Kafka retention removes them. `__share_group_state-*` is Kafka's internal share state topic, not the application message topic.

### Current validation status

- Java sources compile successfully against the installed Kafka client used for local checks.
- `docker compose config` validates the Compose file.
- Full Docker runtime verification depends on Docker Desktop being available and accessible.
- The scripted shell/PowerShell setup performs broker readiness and group configuration, but does not yet assert all board counts automatically.

## Known limitations and follow-up work

1. Pass the selected topic through workers, board, event metadata, and storage inspection instead of relying on `Config.TOPIC`.
2. Add the planned ANSI partition grid, worker panel, and color output.
3. Add an explicit classic-mode menu option when the comparison milestone is ready.
4. Make the board use a persistent/reusable reader model rather than replaying with a new group on every redraw.
5. Add automated assertions to `scripts/e2e.sh` for produced count, acknowledgements, archive count, members, and redelivery.
6. Add a dedicated test for reset followed by create, produce, and worker startup.
