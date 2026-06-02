package com.message_broker.kafka_consumer.config;

import com.message_broker.kafka_consumer.exception.InvalidUserDataException;
import com.message_broker.kafka_consumer.listener.KafkaRebalanceListener;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;


import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConsumerConfig {
    @Value("${kafka.brokerAddress}")
    private String brokerAddress;

    @Value("${kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;

    @Value("${kafka.topics.user.group}")
    private String userGroupId;

    @Value("${kafka.topics.company.group}")
    private String companyGroupId;

    @Value("${kafka.topics.message.group}")
    private String msgGroupId;

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class);
        ProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }

    //DLQ
    @Bean
    public DeadLetterPublishingRecoverer recoverer(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> {
                    // Preserve original partition info in DLQ
                    String dlqTopic = record.topic() + ".DLT";
                    log.error("Sending record from topic {} partition {} offset {} to DLQ {}",
                            record.topic(), record.partition(), record.offset(), dlqTopic);

                    // Keep original partition (optional - or send to partition 0)
                    return new TopicPartition(dlqTopic, record.partition());
                });
        recoverer.setHeadersFunction((record, ex) -> {
            Headers headers = record.headers();
            headers.add("original-topic", record.topic().getBytes());
            headers.add("original-partition", String.valueOf(record.partition()).getBytes());
            headers.add("original-offset", String.valueOf(record.offset()).getBytes());
            headers.add("error-message", ex.getMessage().getBytes());
            headers.add("error-timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
            return headers;
        });
        return recoverer;
    }

    // Retry
    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(1000L, 3));  // Retry 3 times, 1 second apart
        handler.addNotRetryableExceptions(DeserializationException.class, InvalidUserDataException.class);
        return handler;
    }

    @Bean
    public ConsumerFactory<String, schema.avro.AvroUser> userConsumerFactory() {
        Map<String, Object> props = createDefaultProps();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, userGroupId);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, schema.avro.AvroUser> userKafkaListenerFactory(DefaultErrorHandler errorHandler,KafkaRebalanceListener rebalanceListener) {
        ConcurrentKafkaListenerContainerFactory<String, schema.avro.AvroUser> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(userConsumerFactory());
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        factory.setConcurrency(3);
        factory.setRecordFilterStrategy((consumerRecord) -> {
            schema.avro.AvroUser user = (schema.avro.AvroUser) consumerRecord.value();
            return "INACTIVE".equals(user.getStatus()); // Skip inactive users
        });
        return factory;
    }

    @Bean
    public ConsumerFactory<String, schema.avro.AvroCompany> companyConsumerFactory() {
        Map<String, Object> props = createDefaultProps();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, companyGroupId);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,  schema.avro.AvroCompany> companyKafkaListenerFactory(DefaultErrorHandler errorHandler,KafkaRebalanceListener rebalanceListener) {
        ConcurrentKafkaListenerContainerFactory<String,  schema.avro.AvroCompany> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(companyConsumerFactory());
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> messageIdConsumerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, msgGroupId);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> messageIdKafkaListenerFactory(DefaultErrorHandler errorHandler, KafkaRebalanceListener rebalanceListener) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(messageIdConsumerFactory());
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        factory.setConcurrency(3);
        return factory;
    }



    private Map<String, Object> createDefaultProps() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());
        return props;
    }
}
