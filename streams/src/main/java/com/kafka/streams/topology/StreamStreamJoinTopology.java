package com.kafka.streams.topology;


import com.kafka.streams.config.KafkaTopicProperties;
import com.kafka.streams.config.OutputTopics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import schema.avro.AvroUser;

import java.time.Duration;


@Slf4j
@Component
@RequiredArgsConstructor
public class StreamStreamJoinTopology {

    private final KafkaTopicProperties props;
    private final SpecificAvroSerde<AvroUser> userSerde;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {


        //connect to two topics and create respective KStream
        KStream<String, AvroUser> streamA = builder
                .stream(props.getTopics().getUser().getName(),
                        Consumed.with(Serdes.String(), userSerde)
                                .withName("source-users-stream-ss"));


        KStream<String, AvroUser> streamB = builder
                .stream(props.getTopics().getUserEvents().getName(),
                        Consumed.with(Serdes.String(), userSerde)
                                .withName("source-user-events-stream-ss"));

//both streams should have common window
        JoinWindows symmetricWindow = JoinWindows
                .ofTimeDifferenceWithNoGrace(
                        Duration.ofMinutes(props.getJoinWindows().getStreamStreamMinutes()));

//there is grace period
        JoinWindows windowWithGrace = JoinWindows
                .ofTimeDifferenceAndGrace(
                        Duration.ofMinutes(props.getJoinWindows().getStreamStreamMinutes()),
                        Duration.ofSeconds(props.getJoinWindows().getGracePeriodSeconds()));
//there is no grace period and no sync

        JoinWindows asymmetricWindow = JoinWindows
                .ofTimeDifferenceWithNoGrace(
                        Duration.ofMinutes(props.getJoinWindows().getAsymmetricAfterMinutes()))
                .before(Duration.ZERO)
                .after(Duration.ofMinutes(props.getJoinWindows().getAsymmetricAfterMinutes()));

//INNER JOIN
        streamA
                .join(
                        streamB,
                        (userA, userB) -> String.format(
                                "[SS-INNER] userId=%s | dept=%s | activity-status=%s",
                                userA.getUserId(), userA.getDepartment(), userB.getStatus()),
                        symmetricWindow,
                        StreamJoined.<String, AvroUser, AvroUser>as("store-ss-inner")
                                .withName("join-ss-inner")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(userSerde))
                .to(OutputTopics.STREAM_STREAM_INNER,
                        Produced.<String, String>as("sink-ss-inner")
                                .withValueSerde(Serdes.String()));

//LEFT JOIN
        streamA
                .leftJoin(
                        streamB,
                        (userA, userB) -> {
                            String activityStatus = (userB != null)
                                    ? userB.getStatus().toString()
                                    : "NO_ACTIVITY_IN_WINDOW";
                            return String.format(
                                    "[SS-LEFT] userId=%s | dept=%s | activity=%s",
                                    userA.getUserId(), userA.getDepartment(), activityStatus);
                        },
                        symmetricWindow,
                        StreamJoined.<String, AvroUser, AvroUser>as("store-ss-left")
                                .withName("join-ss-left")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(userSerde))
                .to(OutputTopics.STREAM_STREAM_LEFT,
                        Produced.<String, String>as("sink-ss-left")
                                .withValueSerde(Serdes.String()));

//outer join
        streamA
                .outerJoin(
                        streamB,
                        (userA, userB) -> {
                            String leftId      = (userA != null) ? userA.getUserId()          : "NO_PROFILE_UPDATE";
                            String rightStatus = (userB != null) ? userB.getStatus().toString(): "NO_ACTIVITY";
                            return String.format("[SS-OUTER] left=%s | right=%s", leftId, rightStatus);
                        },
                        symmetricWindow,
                        StreamJoined.<String, AvroUser, AvroUser>as("store-ss-outer")
                                .withName("join-ss-outer")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(userSerde))
                .to(OutputTopics.STREAM_STREAM_OUTER,
                        Produced.<String, String>as("sink-ss-outer")
                                .withValueSerde(Serdes.String()));

//ASYMETRIC INNER JOIN
        streamA
                .join(
                        streamB,
                        (userA, userB) -> String.format(
                                "[SS-ASYMMETRIC] userId=%s | profile-dept=%s | post-event-status=%s",
                                userA.getUserId(), userA.getDepartment(), userB.getStatus()),
                        asymmetricWindow,
                        StreamJoined.<String, AvroUser, AvroUser>as("store-ss-asymmetric")
                                .withName("join-ss-asymmetric")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(userSerde))
                .to(OutputTopics.STREAM_STREAM_ASYMMETRIC,
                        Produced.<String, String>as("sink-ss-asymmetric")
                                .withValueSerde(Serdes.String()));

//INNER JOIN WITH GRACE
        streamA
                .join(
                        streamB,
                        (userA, userB) -> String.format(
                                "[SS-GRACE] userId=%s | dept=%s | status=%s",
                                userA.getUserId(), userA.getDepartment(), userB.getStatus()),
                        windowWithGrace,
                        StreamJoined.<String, AvroUser, AvroUser>as("store-ss-grace")
                                .withName("join-ss-grace")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(userSerde))
                .to(OutputTopics.STREAM_STREAM_GRACE,
                        Produced.<String, String>as("sink-ss-grace")
                                .withValueSerde(Serdes.String()));

        log.info("StreamStreamJoinTopology registered — topics: streamA={}, streamB={}",
                props.getTopics().getUser().getName(),
                props.getTopics().getUserEvents().getName());
    }
}
