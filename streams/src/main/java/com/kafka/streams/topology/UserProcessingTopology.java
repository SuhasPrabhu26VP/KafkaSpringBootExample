package com.kafka.streams.topology;

import com.kafka.streams.dto.UserDto;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;
import org.springframework.stereotype.Component;

@Component
public class UserProcessingTopology {
    private static final Logger log = LoggerFactory.getLogger(UserProcessingTopology.class);
    private final JsonMapper objectMapper;

    @Value("${spring.kafka.topics.user.name}")
    private String userTopicName;

    public UserProcessingTopology(JsonMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        SpecificAvroSerde<schema.avro.AvroUser> userSerde = new SpecificAvroSerde<>();
        KStream<String, schema.avro.AvroUser> userStream = streamsBuilder
                .stream(userTopicName, Consumed.with(Serdes.String(), userSerde));
        userStream
                .filter((key, user) -> !user.getStatus().equals("INACTIVE"))
                .peek((key, user) -> log.info("Processing user: FirstName{}| LastName {} | Dept {}", user.getFirstName(),user.getLastName(),user.getDepartment()))
                .split()
                .branch(
                        (key, user) -> user.getCountry().equals("IN"),
                        Branched.withConsumer(stream ->
                                stream.to("indian-devs", Produced.with(Serdes.String(), userSerde)))
                )
                .defaultBranch(
                        Branched.withConsumer(stream ->
                                stream.to("software-dev", Produced.with(Serdes.String(), userSerde)))
                );
    }
}
