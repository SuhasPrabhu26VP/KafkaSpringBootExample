package com.message_broker.kafka_consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageConsumer {
    @KafkaListener(topics = "my-topic", groupId = "sample-consumer-group")
    public void listen(UserDetail user) {
        System.out.println("Received userdetails: " + user.firstName() + " - " + user.lastName());
    }
}
