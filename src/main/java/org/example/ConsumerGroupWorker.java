package org.example;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class ConsumerGroupWorker implements Runnable {
    private final String topic, name; private final EventBus events; private volatile boolean running = true;
    public ConsumerGroupWorker(String topic, String name, EventBus events) { this.topic=topic; this.name=name; this.events=events; }
    public void stop() { running=false; }
    public void run() { Properties p=Config.common(Config.CLASSIC_GROUP); p.put("auto.offset.reset","earliest"); p.put("enable.auto.commit","false"); p.put("max.poll.records","1"); try (KafkaConsumer<String,String> c=new KafkaConsumer<>(p)) { c.subscribe(List.of(topic)); while(running) for (ConsumerRecord<String,String> r:c.poll(Duration.ofMillis(500))) { events.emit("ACQUIRED",r.partition(),r.offset(),name,1,"classic consumer group"); try { Thread.sleep(ShareWorker.number(r.value(),"durationMs",500)); } catch(InterruptedException e){Thread.currentThread().interrupt();} c.commitSync(); events.emit("ACCEPTED",r.partition(),r.offset(),name,1,"classic consumer group"); } } }
}
