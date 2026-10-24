package org.anmol.devsuite.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
public class KafkaMessageTracker {

    // Thread-safe map to hold the latest 50 messages per topic
    private final Map<String, Deque<String>> topicMessages = new ConcurrentHashMap<>();

    // Dynamic regex listener intercepting all topics
    @KafkaListener(topicPattern = ".*", groupId = "dev-suite-ui-tracker")
    public void listen(ConsumerRecord<String, String> record) {
        String topic = record.topic();

        topicMessages.putIfAbsent(topic, new ConcurrentLinkedDeque<>());
        Deque<String> messages = topicMessages.get(topic);

        // Add new message to the top
        String logEntry = String.format("Partition: %d | Offset: %d | Key: %s | Data: %s",
                record.partition(), record.offset(), record.key(), record.value());

        messages.addFirst(logEntry);

        // Trim deque to prevent memory leaks
        if (messages.size() > 50) {
            messages.pollLast();
        }
    }

    public List<String> getLatestMessages(String topic) {
        Deque<String> messages = topicMessages.get(topic);
        return messages != null ? new ArrayList<>(messages) : new ArrayList<>();
    }
}