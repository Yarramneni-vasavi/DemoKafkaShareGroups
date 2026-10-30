## Setup

```
docker compose up -d
docker ps
.\scripts\e2e.ps1
```

Build and run with Java 21 and Maven:

```text
mvn package
java -cp target/classes;target/dependency/* org.example.Main
```

Before starting workers, initialize the share-group configuration:

```text
docker exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type groups --entity-name demo-share --add-config share.auto.offset.reset=earliest,share.record.lock.duration.ms=5000
```

The CLI creates the topic and event topic, produces records with optional poison offsets, starts share or classic workers, renders the event board, exposes broker internals, inspects the log, kills a worker, and resets the demo.
