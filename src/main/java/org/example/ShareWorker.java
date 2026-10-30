package org.example;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class ShareWorker implements Runnable {
    private final String topic; private final String name; private final EventBus events; private volatile boolean running = true; private volatile boolean killed;
    public ShareWorker(String topic, String name, EventBus events) { this.topic = topic; this.name = name; this.events = events; }
    public void stop() { running = false; }
    public void kill() { killed = true; running = false; }
    @Override public void run() {
        // Share groups do not allow the client-side auto.offset.reset setting.
        // The reset policy is configured on the broker for demo-share.
        Properties p = Config.common(Config.SHARE_GROUP); p.remove("auto.offset.reset"); p.put("share.acknowledgement.mode", "explicit"); p.put("share.acquire.mode", "record_limit"); p.put("max.poll.records", "1");
        KafkaShareConsumer<String,String> consumer = new KafkaShareConsumer<>(p); consumer.subscribe(List.of(topic));
        try {
            while (running || killed) {
                for (ConsumerRecord<String,String> r : consumer.poll(Duration.ofMillis(500))) {
                    int delivery = r.deliveryCount().orElse((short) 1).intValue(); events.emit("ACQUIRED", r.partition(), r.offset(), name, delivery, null);
                    if (killed) { Thread.sleep(5500); return; }
                    int duration = number(r.value(), "durationMs", 500); Thread.sleep(duration);
                    if (r.value().contains("\"poison\":true")) {
                        consumer.acknowledge(r, AcknowledgeType.RELEASE);
                        commitAcknowledgement(consumer);
                        events.emit(delivery >= Config.DELIVERY_LIMIT ? "ARCHIVED" : "RELEASED", r.partition(), r.offset(), name, delivery, "poison record");
                    } else {
                        consumer.acknowledge(r, AcknowledgeType.ACCEPT);
                        commitAcknowledgement(consumer);
                        events.emit("ACCEPTED", r.partition(), r.offset(), name, delivery, null);
                    }
                }
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); } finally { if (!killed) consumer.close(); }
    }
    private static void commitAcknowledgement(KafkaShareConsumer<String,String> consumer) {
        if (consumer.commitSync().values().stream().anyMatch(Optional::isPresent))
            throw new IllegalStateException("Kafka did not confirm the share acknowledgement");
    }
    static int number(String json, String field, int fallback) { try { String needle = "\"" + field + "\":"; int i = json.indexOf(needle) + needle.length(), j = i; while (j < json.length() && Character.isDigit(json.charAt(j))) j++; return Integer.parseInt(json.substring(i, j)); } catch (Exception e) { return fallback; } }
}
