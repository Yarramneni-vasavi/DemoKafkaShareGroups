# Kafka Share Groups Demo

## Setup

```
docker compose up -d
docker ps
.\scripts\e2e.ps1
```

## Build with Maven and java:

```text
mvn package
java -cp target/classes;target/dependency/* org.example.Main
```

## Run the Java Program

The CLI creates the topic and event topic, produces records with optional poison offsets, starts share or classic workers, renders the event board, exposes broker internals, inspects the log, kills a worker, and resets the demo.

```
>> java .src\main\java\org\example\Main.java


1 Create topic -- Creates Topic
2 Produce records -- Produces records to given topic
3 Start workers - Start consumers
4 Board - Show how consumers are progressing
5 Group internals - internal offset and other consumer details shown
6 Inspect storage - internal details
7 Kill worker - Remove/Delete one consumer
8 Reset - Reset all the shared partitions and removes topic
0 Exit - Exit the program
```

## Good References to Read about Kafka Share groups:

- [Queues for kafka ready for prime time](https://medium.com/@andrew_schofield/queues-for-kafka-ready-for-prime-time-988f5c58b8f7)

- [Queues for kafka](https://strimzi.io/blog/2025/08/20/queues-for-kafka/)

