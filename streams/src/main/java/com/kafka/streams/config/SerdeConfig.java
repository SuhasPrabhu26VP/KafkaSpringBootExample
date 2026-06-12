package com.kafka.streams.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import schema.avro.AvroCompany;
import schema.avro.AvroUser;

import java.util.Map;


@Configuration
public class SerdeConfig {


    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    private Map<String, Object> serdeConfig() {
        return Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        );
    }

    @Bean
    public SpecificAvroSerde<AvroUser> userSerde() {
        SpecificAvroSerde<AvroUser> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig(), false);
        return serde;
    }

    @Bean
    public SpecificAvroSerde<AvroCompany> companySerde() {
        SpecificAvroSerde<AvroCompany> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig(), false);
        return serde;
    }
}

