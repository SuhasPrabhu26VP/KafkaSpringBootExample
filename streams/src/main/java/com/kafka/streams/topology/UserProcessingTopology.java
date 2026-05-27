package com.kafka.streams.topology;

import com.kafka.streams.dto.UserDto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

public class UserProcessingTopology {
    private static final Logger log = LoggerFactory.getLogger(UserProcessingTopology.class);
    private final JsonMapper objectMapper;

    public UserProcessingTopology(JsonMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        // Create a custom Serde for user objects
        JacksonJsonSerde<UserDto> userSerde = new JacksonJsonSerde<>(UserDto.class, objectMapper);

        // Read from the User topic
        KStream<String, UserDto> userStream = streamsBuilder
                .stream("users", Consumed.with(Serdes.String(), userSerde));

        // Process the stream: filter, transform, and route
        ordersStream
                // Filter out non google company
                .filter((key, user) -> !user.getStatus().equals("INACTIVE"))
                // Log each user for debugging
                .peek((key, user) -> log.info("Processing user: FirstName{}| LastName {} | Dept {}", user.getFirstName(),user.getLastName(),user.getDepartment()))
                // Branch based on order value
                .split()
                .branch(
                        (key, user) -> user.getc() > 30,
                        Branched.withConsumer(stream ->
                                stream.to("senior-devs", Produced.with(Serdes.String(), userSerde)))
                )
                .defaultBranch(
                        Branched.withConsumer(stream ->
                                stream.to("software-dev", Produced.with(Serdes.String(), userSerde)))
                );
    }
}
