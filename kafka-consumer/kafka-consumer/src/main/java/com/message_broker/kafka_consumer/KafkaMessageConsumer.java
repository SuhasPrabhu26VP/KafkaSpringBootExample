package com.message_broker.kafka_consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaMessageConsumer {
    @KafkaListener(topics = "${kafka.topics.user.name}",
            groupId = "${kafka.topics.user.group}",
            containerFactory = "userKafkaListenerFactory")
    public void consumeUser(ConsumerRecord<String, schema.avro.AvroUser> record) {
        schema.avro.AvroUser user = record.value();
        log.info("User received: {} {} | Partition: {} | Offset: {}",
                user.getFirstName(), user.getLastName(),
                record.partition(), record.offset());
    }

    @KafkaListener(topics = "${kafka.topics.company.name}",
            groupId = "${kafka.topics.company.group}",
            containerFactory = "companyKafkaListenerFactory")
    public void consumeCompany(ConsumerRecord<String, schema.avro.AvroCompany> record) {
        schema.avro.AvroCompany company = record.value();
        if ("ABC".equals(company.getCompanyId())) {
            throw new RuntimeException("Error for company id: " + company.getCompanyId());
        }

        log.info("Company received: {} | Partition: {} | Offset: {}",
                company.getName(), record.partition(), record.offset());
    }

    @KafkaListener(topics = "${kafka.topics.message.name}",
            groupId = "${kafka.topics.message.group}",
            containerFactory = "messageIdKafkaListenerFactory")
    public void consumeMessageId(ConsumerRecord<String, String> record) {
        log.info("Message ID received: {} | Partition: {} | Offset: {}",
                record.value(), record.partition(), record.offset());
    }
}
