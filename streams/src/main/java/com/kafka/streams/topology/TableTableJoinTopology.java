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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import schema.avro.AvroCompany;
import schema.avro.AvroUser;


@Slf4j
@Component
@RequiredArgsConstructor
public class TableTableJoinTopology {

    private final KafkaTopicProperties props;
    private final SpecificAvroSerde<AvroUser> userSerde;
    private final SpecificAvroSerde<AvroCompany> companySerde;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        KTable<String, AvroUser> userTable = builder
                .table(props.getTopics().getUsersChangelog().getName(),
                        Consumed.with(Serdes.String(), userSerde)
                                .withName("source-users-changelog-tt"),
                        Materialized.<String, AvroUser, KeyValueStore<Bytes, byte[]>>
                                        as("store-user-tt")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde));


        KTable<String, AvroCompany> companyTable = builder
                .table(props.getTopics().getCompany().getName(),
                        Consumed.with(Serdes.String(), companySerde)
                                .withName("source-companies-changelog-tt"),
                        Materialized.<String, AvroCompany, KeyValueStore<Bytes, byte[]>>
                                        as("store-company-tt")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde));
        userTable
                .join(
                        companyTable,
                        (user, company) -> String.format(
                                "[TT-INNER] userId=%s | name=%s %s | company=%s [%s] | country=%s",
                                user.getUserId(),
                                user.getFirstName(), user.getLastName(),
                                company.getName(), company.getIndustry(), company.getCountry()),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>
                                        as("store-tt-inner-result")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()))
                .toStream()
                .to(OutputTopics.TABLE_TABLE_INNER,
                        Produced.<String, String>as("sink-tt-inner")
                                .withValueSerde(Serdes.String()));

        userTable
                .leftJoin(
                        companyTable,
                        (user, company) -> {
                            String companyName = (company != null) ? company.getName()                : "UNREGISTERED";
                            String industry    = (company != null) ? company.getIndustry().toString() : "N/A";
                            return String.format(
                                    "[TT-LEFT] userId=%s | dept=%s | company=%s | industry=%s",
                                    user.getUserId(), user.getDepartment(), companyName, industry);
                        },
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>
                                        as("store-tt-left-result")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()))
                .toStream()
                .to(OutputTopics.TABLE_TABLE_LEFT,
                        Produced.<String, String>as("sink-tt-left")
                                .withValueSerde(Serdes.String()));


  //foreign key joins
        KTable<String, AvroUser> userTableFk = builder
                .table(props.getTopics().getUsersChangelog().getName(),
                        Consumed.with(Serdes.String(), userSerde)
                                .withName("source-users-changelog-fk"),
                        Materialized.<String, AvroUser, KeyValueStore<Bytes, byte[]>>
                                        as("store-user-fk")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde));

        KTable<String, AvroCompany> companyTableFk = builder
                .table(props.getTopics().getCompany().getName(),
                        Consumed.with(Serdes.String(), companySerde)
                                .withName("source-companies-changelog-fk"),
                        Materialized.<String, AvroCompany, KeyValueStore<Bytes, byte[]>>
                                        as("store-company-fk")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde));
        userTableFk
                .join(
                        companyTableFk,
                        user -> user.getCompanyId(),
                        (user, company) -> String.format(
                                "[FK-JOIN] userId=%s | name=%s %s | company=%s | industry=%s | country=%s | revenue=%s",
                                user.getUserId(),
                                user.getFirstName(), user.getLastName(),
                                company.getName(), company.getIndustry(), company.getCountry(),
                                company.getRevenue() != null ? company.getRevenue() : "N/A"),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>
                                        as("store-tt-fk-result")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()))
                .toStream()
                .to(OutputTopics.TABLE_TABLE_FK,
                        Produced.<String, String>as("sink-tt-fk")
                                .withValueSerde(Serdes.String()));

        log.info("TableTableJoinTopology registered — topics: usersChangelog={}, company={}",
                props.getTopics().getUsersChangelog().getName(),
                props.getTopics().getCompany().getName());
    }
}
