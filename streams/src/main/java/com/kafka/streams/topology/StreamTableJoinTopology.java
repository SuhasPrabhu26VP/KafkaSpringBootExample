package com.kafka.streams.topology;

import com.kafka.streams.config.KafkaTopicProperties;
import com.kafka.streams.config.OutputTopics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import schema.avro.AvroCompany;
import schema.avro.AvroUser;

import java.time.Duration;


@Slf4j
@Component
@RequiredArgsConstructor
public class StreamTableJoinTopology {

    private final KafkaTopicProperties props;
    private final SpecificAvroSerde<AvroUser> userSerde;
    private final SpecificAvroSerde<AvroCompany> companySerde;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {


        KStream<String, AvroUser> usersStream = builder
                .stream(props.getTopics().getUser().getName(),
                        Consumed.with(Serdes.String(), userSerde)
                                .withName("source-users-stream"))
                .selectKey((userId, user) -> user.getCompanyId(),
                        Named.as("rekey-user-to-companyId"));


        KTable<String, AvroCompany> companyTable = builder
                .table(props.getTopics().getCompany().getName(),
                        Consumed.with(Serdes.String(), companySerde)
                                .withName("source-companies-changelog"),
                        Materialized.<String, AvroCompany, KeyValueStore<Bytes, byte[]>>
                                        as("store-company-lookup")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde));


        usersStream
                .join(
                        companyTable,
                        this::enrichUserWithCompany,
                        Joined.<String, AvroUser, AvroCompany>as("join-stream-table-inner")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(companySerde))
                .to(OutputTopics.STREAM_TABLE_INNER,
                        Produced.<String, String>as("sink-stream-table-inner")
                                .withValueSerde(Serdes.String()));


        usersStream
                .leftJoin(
                        companyTable,
                        this::enrichUserWithCompanyNullSafe,
                        Joined.<String, AvroUser, AvroCompany>as("join-stream-table-left")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(companySerde))
                .to(OutputTopics.STREAM_TABLE_LEFT,
                        Produced.<String, String>as("sink-stream-table-left")
                                .withValueSerde(Serdes.String()));


        VersionedBytesStoreSupplier versionedStore = Stores.persistentVersionedKeyValueStore(
                "store-versioned-company",
                Duration.ofHours(props.getJoinWindows().getVersionedStoreRetentionHours()));

        KTable<String, AvroCompany> versionedCompanyTable = builder
                .table(props.getTopics().getCompany().getName(),
                        Consumed.with(Serdes.String(), companySerde)
                                .withName("source-companies-versioned"),
                        Materialized.<String, AvroCompany>as(versionedStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde));

        usersStream
                .join(
                        versionedCompanyTable,
                        (user, company) -> String.format(
                                "[TEMPORAL] userId=%s | company-at-event-time=%s | industry=%s",
                                user.getUserId(), company.getName(), company.getIndustry()),
                        Joined.<String, AvroUser, AvroCompany>as("join-stream-table-temporal")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde)
                                .withOtherValueSerde(companySerde))
                .to(OutputTopics.STREAM_TABLE_TEMPORAL,
                        Produced.<String, String>as("sink-stream-table-temporal")
                                .withValueSerde(Serdes.String()));

        log.info("StreamTableJoinTopology registered — topics: user={}, company={}",
                props.getTopics().getUser().getName(),
                props.getTopics().getCompany().getName());
    }



    private String enrichUserWithCompany(AvroUser user, AvroCompany company) {
        return String.format("[INNER] userId=%s | name=%s %s | company=%s | industry=%s | country=%s",
                user.getUserId(),
                user.getFirstName(), user.getLastName(),
                company.getName(), company.getIndustry(), company.getCountry());
    }

    private String enrichUserWithCompanyNullSafe(AvroUser user, AvroCompany company) {
        String companyName = (company != null) ? company.getName()                : "UNKNOWN_COMPANY";
        String industry    = (company != null) ? company.getIndustry().toString() : "N/A";
        String country     = (company != null) ? company.getCountry().toString()  : "N/A";
        return String.format("[LEFT] userId=%s | name=%s %s | company=%s | industry=%s | country=%s",
                user.getUserId(),
                user.getFirstName(), user.getLastName(),
                companyName, industry, country);
    }
}
