package com.kafka.streams.topology;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import schema.avro.AvroUser;
import schema.avro.Country;
import schema.avro.UserStatus;

import java.util.Map;

@Configuration
public class UserProcessingTopology {

    private static final Logger log = LoggerFactory.getLogger(UserProcessingTopology.class);

    private static final String BRANCH_PREFIX = "user-country-";
    private static final String TOPIC_INDIAN  = "indian-users";
    private static final String TOPIC_US      = "us-users";
    private static final String TOPIC_UK      = "uk-users";
    private static final String TOPIC_OTHER   = "other-users";

    @Value("${spring.kafka.topics.user.name}")
    private String userTopic;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KStream<String, AvroUser> buildPipeline(StreamsBuilder streamsBuilder) {
        SpecificAvroSerde<AvroUser> userSerde = buildUserSerde();
        Produced<String, AvroUser> produced   = Produced.with(Serdes.String(), userSerde);

        KStream<String, AvroUser> userStream = streamsBuilder
                .stream(userTopic, Consumed.with(Serdes.String(), userSerde));

        KStream<String, AvroUser> activeUsers = userStream
                .filter((key, user) -> {
                    if (user == null) return false;
                    boolean isActive = user.getStatus() != UserStatus.INACTIVE
                            && user.getStatus() != UserStatus.TERMINATED;
                    return isActive ;
                })
                .peek((key, user) -> log.info(
                        "Processing user: {} {} | status={} country={} dept={}",
                        user.getFirstName(), user.getLastName(),
                        user.getStatus(), user.getCountry(), user.getDepartment()));

        activeUsers
                .split(Named.as(BRANCH_PREFIX))
                .branch(
                        (key, user) -> Country.IN == user.getCountry(),  
                        Branched.<String, AvroUser>withConsumer(s -> s.to(TOPIC_INDIAN, produced)).withName("india")
                )
                .branch(
                        (key, user) -> Country.US == user.getCountry(),
                        Branched.<String, AvroUser>withConsumer(s -> s.to(TOPIC_US, produced)).withName("us")
                )
                .branch(
                        (key, user) -> Country.UK == user.getCountry(),
                        Branched.<String, AvroUser>withConsumer(s -> s.to(TOPIC_UK, produced)).withName("uk")
                )
                .defaultBranch(
                        Branched.<String, AvroUser>withConsumer(s -> s.to(TOPIC_OTHER, produced)).withName("other")
                );

        return userStream;
    }

    private SpecificAvroSerde<AvroUser> buildUserSerde() {
        SpecificAvroSerde<AvroUser> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                false
        );
        return serde;
    }
}