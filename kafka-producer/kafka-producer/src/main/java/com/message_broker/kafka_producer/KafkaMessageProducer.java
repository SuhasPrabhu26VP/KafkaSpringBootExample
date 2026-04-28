package com.message_broker.kafka_producer;

import com.message_broker.kafka_producer.dto.UserDetails;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageProducer {
    private final KafkaTemplate<String, UserDetails> kafkaTemplate;

    public KafkaMessageProducer(KafkaTemplate<String, UserDetails> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(UserDetails message) {
        kafkaTemplate.send("my-topic", message);
    }
}
