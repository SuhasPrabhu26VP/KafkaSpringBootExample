package com.message_broker.kafka_consumer.service;

import com.message_broker.kafka_consumer.dto.CompanyData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageConsumerService {
    @KafkaListener(topics = "${kafka.topics.user.name}",
            groupId = "${kafka.topics.user.group}",
            containerFactory = "userKafkaListenerFactory")
    public void consumeUser(ConsumerRecord<String, schema.avro.User> record) {
        schema.avro.User user = record.value();
        System.out.println("User name : " + user.getFirstName());
    }

    @KafkaListener(topics = "${kafka.topics.company.name}",
            groupId = "${kafka.topics.company.group}",
            containerFactory = "companyKafkaListenerFactory")
    public void consumeCompany(ConsumerRecord<String, CompanyData> record) {
        CompanyData company = record.value();
        System.out.println("Company name : " + company.getName());
    }

    @KafkaListener(topics = "${kafka.topics.message.name}",
            groupId = "${kafka.topics.message.group}",
            containerFactory = "messageIdKafkaListenerFactory")
    public void consumeMessageId(ConsumerRecord<String, String> record) {
        System.out.println("Message Id : " + record.value());
    }
}
