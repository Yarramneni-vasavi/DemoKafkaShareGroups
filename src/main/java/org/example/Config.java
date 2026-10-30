package org.example;

import java.util.Properties;

public final class Config {
    public static final String BOOTSTRAP = System.getProperty("kafka.bootstrap", "localhost:9092");
    public static final String TOPIC = "orders";
    public static final String EVENTS_TOPIC = "demo-events";
    public static final String SHARE_GROUP = "demo-share";
    public static final String CLASSIC_GROUP = "demo-classic";
    public static final int DELIVERY_LIMIT = 3;
    private Config() {}

    public static Properties common(String group) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", group);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
