package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.*;

public class EventBus implements AutoCloseable {
    private final KafkaProducer<String,String> producer;
    public EventBus() { Properties p = new Properties(); p.put("bootstrap.servers", Config.BOOTSTRAP); p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); p.put("acks", "all"); producer = new KafkaProducer<>(p); }
    // Internal demo event stream used only to maintain the records needed by Board.
    // These are application events, not broker-generated per-record state events.
    public void emit(String event, int partition, long offset, String worker, Integer delivery, String note) {
        String json = "{\"ts\":" + System.currentTimeMillis() + ",\"event\":\"" + event + "\",\"topic\":\"" + Config.TOPIC + "\",\"partition\":" + partition + ",\"offset\":" + offset + ",\"worker\":" + (worker == null ? "null" : "\"" + worker + "\"") + ",\"deliveryCount\":" + (delivery == null ? "null" : delivery) + (note == null ? "" : ",\"note\":\"" + note.replace("\"", "'") + "\"") + "}";
        producer.send(new ProducerRecord<>(Config.EVENTS_TOPIC, Integer.toString(partition) + ":" + offset, json)); producer.flush();
    }
    public List<String> readAll(String group) { Properties p = Config.common(group); p.put("auto.offset.reset", "earliest"); p.put("enable.auto.commit", "false"); List<String> result = new ArrayList<>(); try (KafkaConsumer<String,String> c = new KafkaConsumer<>(p)) { c.subscribe(List.of(Config.EVENTS_TOPIC)); long quiet = System.currentTimeMillis() + 400; while (System.currentTimeMillis() < quiet) { var records = c.poll(Duration.ofMillis(100)); for (var r : records) { result.add(r.value()); quiet = System.currentTimeMillis() + 200; } } } return result; }
    @Override public void close() { producer.close(); }
}
