package com.message_broker.kafka_producer;

import com.message_broker.kafka_producer.dto.CompanyDto;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@RequiredArgsConstructor
@Service
public class KafkaMessageProducer {
    @Value("${kafka.topics.user.name}")
    private String userTopicName;

    @Value("${kafka.topics.company.name}")
    private String companyTopicName;

    @Value("${kafka.topics.message.name}")
    private String messageIdTopicName;

    private final KafkaTemplate<String, schema.avro.AvroUser> userKafkaTemplate;
    private final KafkaTemplate<String, schema.avro.AvroCompany> companyKafkaTemplate;
    private final KafkaTemplate<String, String> messageIdKafkaTemplate;

    public void produceUser(schema.avro.AvroUser user) {
        ProducerRecord<String, schema.avro.AvroUser> producerRecord = new ProducerRecord<>(userTopicName, user);
        userKafkaTemplate.send(producerRecord);
    }

    public void produceCompany(schema.avro.AvroCompany company) {
        ProducerRecord<String, schema.avro.AvroCompany> producerRecord = new ProducerRecord<>(companyTopicName, company);
        companyKafkaTemplate.send(producerRecord);
    }

    public void produceMessageId(String messageId) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(messageIdTopicName, messageId);
        messageIdKafkaTemplate.send(producerRecord);
    }
}
