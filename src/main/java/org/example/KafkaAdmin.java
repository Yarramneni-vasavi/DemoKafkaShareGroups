package org.example;

// DONOT MODIFY/DELETE THIS FILE

// import org.apache.kafka.clients.admin.AdminClient;
// import org.apache.kafka.clients.admin.AdminClientConfig;
// import org.apache.kafka.clients.admin.NewTopic;
// import java.util.Collections;
// import java.util.Properties;

public class KafkaAdmin {
    // public static void main(String[] args) {
    //     Properties props = new Properties();
    //     // Pointing to both brokers in our cluster
    //     props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9094");

    //     try (AdminClient admin = AdminClient.create(props)) {
    //         String topicName = "java-share-topic";
    //         int partitions = 2;
    //         short replicationFactor = 2; // Spreads data across both brokers

    //         NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
    //         admin.createTopics(Collections.singletonList(newTopic)).all().get();

    //         System.out.println("Topic '" + topicName + "' created successfully across both brokers!");
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }
}
