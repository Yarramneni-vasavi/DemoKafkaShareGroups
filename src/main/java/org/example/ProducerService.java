package org.example;

import org.apache.kafka.clients.producer.*;
import java.util.*;

public class ProducerService {
    public void produce(String topic, int count, Set<Integer> poison) {
        int partitions = new TopicService().partitions(topic); Properties p = new Properties(); p.put("bootstrap.servers", Config.BOOTSTRAP); p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); p.put("acks", "all");
        int[] totals = new int[partitions]; Random random = new Random();
        try (KafkaProducer<String,String> producer = new KafkaProducer<>(p); EventBus events = new EventBus()) {
            for (int i = 0; i < count; i++) { int part = i % partitions; totals[part]++; String value = "{\"id\":" + i + ",\"durationMs\":" + (200 + random.nextInt(1801)) + ",\"poison\":" + poison.contains(i) + "}"; RecordMetadata m = producer.send(new ProducerRecord<>(topic, part, Integer.toString(i), value)).get(); events.emit("PRODUCED", m.partition(), m.offset(), null, null, null); }
        } catch (Exception e) { throw new RuntimeException("Produce failed", e); }
        for (int i = 0; i < totals.length; i++) System.out.println("partition " + i + ": " + totals[i] + " records");
    }
}
