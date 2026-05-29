package com.message_broker.kafka_producer.config;


import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
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
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, schema.avro.AvroUser> userKafkaTemplate() {
        return new KafkaTemplate<String,schema.avro.AvroUser>(userProducerFactory());
    }

    @Bean
    public ProducerFactory<String, schema.avro.AvroCompany> companyProducerFactory() {
        Map<String, Object> props = createDefaultProps();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
                RecordNameStrategy.class.getName());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, schema.avro.AvroCompany> companyKafkaTemplate() {
        return new KafkaTemplate<String, schema.avro.AvroCompany>(companyProducerFactory());
    }

    @Bean
    public ProducerFactory<String, String> messageIdProducerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> messageIdKafkaTemplate() {
        return new KafkaTemplate<String, String>(messageIdProducerFactory());
    }

    private Map<String, Object> createDefaultProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);

        return props;
    }
}
