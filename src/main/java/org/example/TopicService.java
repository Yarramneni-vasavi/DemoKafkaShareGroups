package org.example;

import org.apache.kafka.clients.admin.*;
import java.util.*;

public class TopicService {
    private final Properties props = Config.common("");
    public void ensure(String name, int partitions) { ensure(name, partitions, true); }
    private void ensure(String name, int partitions, boolean report) {
        try (Admin admin = Admin.create(props)) {
            try { admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get(); if (report) System.out.println("Created topic " + name); }
            catch (Exception e) { String message = String.valueOf(e.getCause() == null ? e : e.getCause()); if (!message.contains("TopicExists") && !message.contains("already exists")) throw e; if (report) System.out.println("Topic already exists: " + name); }
        } catch (Exception e) { throw new RuntimeException("Topic operation failed: " + e.getMessage(), e); }
    }
    // Internal demo-only topic: stores application state-change events for the terminal board.
    public void ensureEvents() { ensure(Config.EVENTS_TOPIC, 1, false); }
    public int partitions(String name) {
        try (Admin admin = Admin.create(props)) { return admin.describeTopics(List.of(name)).allTopicNames().get().get(name).partitions().size(); }
        catch (Exception e) { throw new RuntimeException(e); }
    }
    public void delete(String name) { try (Admin a = Admin.create(props)) { a.deleteTopics(List.of(name)).all().get(); } catch (Exception ignored) {} }
}
