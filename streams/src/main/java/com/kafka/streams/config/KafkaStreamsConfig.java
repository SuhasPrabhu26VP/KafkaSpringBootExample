package com.kafka.streams.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.streams.num-stream-threads:3}")  // default 3 if not set
    private int numStreamThreads;
    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));
    }

    @Bean
    public KafkaAdmin.NewTopics userTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("indian-users").partitions(3).replicas(3).build(),
                TopicBuilder.name("us-users").partitions(3).replicas(3).build(),
                TopicBuilder.name("uk-users").partitions(3).replicas(3).build(),
                TopicBuilder.name("other-users").partitions(3).replicas(3).build(),
                TopicBuilder.name("user-input-topic").partitions(3).replicas(3).build()
        );
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();

        // Core
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,        applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,     bootstrapServers);

        // Serdes
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,  SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // Processing guarantees
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,  StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,    1000);

        // Threading
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,    numStreamThreads);

        return new KafkaStreamsConfiguration(props);
    }
}
