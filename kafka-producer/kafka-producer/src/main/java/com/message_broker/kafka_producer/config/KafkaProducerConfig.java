package com.message_broker.kafka_producer.config;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.message_broker.kafka_producer.dto.CompanyData;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
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

    @Value("${kafka.sasl-username}")
    private String saslUsername;

    @Value("${kafka.sasl-password}")
    private String saslPassword;

    @Bean
    public ProducerFactory<String, schema.avro.User> userProducerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, schema.avro.User> userKafkaTemplate() {
        return new KafkaTemplate<String, schema.avro.User>(userProducerFactory());
    }

    @Bean
    public ProducerFactory<String, CompanyData> companyProducerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, CompanyData> companyKafkaTemplate() {
        return new KafkaTemplate<String, CompanyData>(companyProducerFactory());
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

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                "%s required username=\"" + saslUsername + "\" password=\"" + saslPassword + "\";", PlainLoginModule.class.getName(), "username", "password"
        ));

        return props;
    }
}
