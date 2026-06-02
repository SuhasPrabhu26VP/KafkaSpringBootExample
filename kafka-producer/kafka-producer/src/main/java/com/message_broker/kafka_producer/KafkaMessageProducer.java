package com.message_broker.kafka_producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import schema.avro.AvroUser;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
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
        ProducerRecord<String, schema.avro.AvroUser> producerRecord =
                new ProducerRecord<>(userTopicName, user.getUserId(), user);
        userKafkaTemplate.send(producerRecord)
                .thenAccept(result ->
                        log.info("User sent: {} to partition: {}",
                                user.getUserId(), result.getRecordMetadata().partition()))
                .exceptionally(ex -> {
                    log.error("Failed to send user: {}", user.getUserId(), ex);
                    return null;
                });
    }

    public void produceCompany(schema.avro.AvroCompany company) {
        ProducerRecord<String, schema.avro.AvroCompany> producerRecord =
                new ProducerRecord<>(companyTopicName, company.getCompanyId(), company);
        companyKafkaTemplate.send(producerRecord)
                .thenAccept(result ->
                        log.info("Company sent: {} to partition: {}",
                                company.getCompanyId(), result.getRecordMetadata().partition()))
                .exceptionally(ex -> {
                    log.error("Failed to send company: {}", company.getCompanyId(), ex);
                    return null;
                });
    }

    public void produceMessageId(String messageId) {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(messageIdTopicName, messageId, messageId);
        messageIdKafkaTemplate.send(producerRecord)
                .thenAccept(result ->
                        log.info("Message ID sent: {} to partition: {}",
                                messageId, result.getRecordMetadata().partition()))
                .exceptionally(ex -> {
                    log.error("Failed to send message ID: {}", messageId, ex);
                    return null;
                });
    }


    public void produceUserWithRelatedData(schema.avro.AvroUser user) {
        userKafkaTemplate.executeInTransaction(template -> {
            template.send(userTopicName, user.getUserId(), user);
            log.info("User sent transactionally: {}", user.getUserId());
            return true;
        });
    }

    @Transactional(transactionManager = "companyTransactionManager")
    public void produceCompanyWithRelatedData(schema.avro.AvroCompany company) {
        companyKafkaTemplate.send(companyTopicName, company.getCompanyId(), company);
        log.info("All messages sent transactionally for company: {}", company.getCompanyId());
    }

    public void produceBatchWithTransaction(List<schema.avro.AvroUser> users) {
        userKafkaTemplate.executeInTransaction(template -> {
            for (schema.avro.AvroUser user : users) {
                template.send(userTopicName, user.getUserId(), user);
                log.debug("Sent user {} in transaction", user.getUserId());
            }
            log.info("Batch of {} users sent transactionally", users.size());
            return true;
        });
    }

}
