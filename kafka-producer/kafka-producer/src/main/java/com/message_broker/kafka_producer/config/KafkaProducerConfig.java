package com.message_broker.kafka_producer.config;


import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import schema.avro.AvroCompany;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@EnableTransactionManagement
public class KafkaProducerConfig {
    @Value("${kafka.brokerAddress}")
    private String brokerAddress;

    @Value("${kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;



    @Bean
    public ProducerFactory<String, schema.avro.AvroUser> userProducerFactory() {
        Map<String, Object> props = createDefaultProps();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
                RecordNameStrategy.class.getName());
        //custom practitioner or any props should be defined first any modification factory properties will not be considered post factory initialization
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                "com.message_broker.kafka_producer.partitioner.DepartmentPartitioner");
        DefaultKafkaProducerFactory<String, schema.avro.AvroUser> factory =
                new DefaultKafkaProducerFactory<>(props);

        factory.setTransactionIdPrefix("user-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, schema.avro.AvroUser> userKafkaTemplate() {
        KafkaTemplate<String, schema.avro.AvroUser> template =
                new KafkaTemplate<>(userProducerFactory());
        template.setTransactionIdPrefix("user-tx-");
        return template;
    }
    @Primary
    @Bean
    public PlatformTransactionManager userTransactionManager(
            ProducerFactory<String, schema.avro.AvroUser> userProducerFactory) {
        return new KafkaTransactionManager<>(userProducerFactory);
    }
    @Bean
    public ProducerFactory<String, schema.avro.AvroCompany> companyProducerFactory() {
        Map<String, Object> props = createDefaultProps();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
                RecordNameStrategy.class.getName());
        DefaultKafkaProducerFactory<String, schema.avro.AvroCompany> factory =
                new DefaultKafkaProducerFactory<>(props);

        factory.setTransactionIdPrefix("company-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, schema.avro.AvroCompany> companyKafkaTemplate() {
        KafkaTemplate<String, schema.avro.AvroCompany> template =
                new KafkaTemplate<>(companyProducerFactory());
        template.setTransactionIdPrefix("company-tx-");
        return template;
    }

    @Bean
    public PlatformTransactionManager companyTransactionManager(
            ProducerFactory<String, schema.avro.AvroCompany> companyProducerFactory) {
        return new KafkaTransactionManager<>(companyProducerFactory);
    }

    @Bean
    public ProducerFactory<String, String> messageIdProducerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix("message-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> messageIdKafkaTemplate() {
        return new KafkaTemplate<String, String>(messageIdProducerFactory());
    }

    private Map<String, Object> createDefaultProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return props;
    }
}
