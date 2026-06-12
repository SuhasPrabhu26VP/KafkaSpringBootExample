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
public class GlobalTableJoinTopology {

    private final KafkaTopicProperties props;
    private final SpecificAvroSerde<AvroUser> userSerde;
    private final SpecificAvroSerde<AvroCompany> companySerde;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {

        KStream<String, AvroUser> usersStream = builder
                .stream(props.getTopics().getUser().getName(),
                        Consumed.with(Serdes.String(), userSerde)
                                .withName("source-users-stream-global"));
        GlobalKTable<String, AvroCompany> globalCompanyTable = builder
                .globalTable(props.getTopics().getCompaniesGlobal().getName(),
                        Consumed.with(Serdes.String(), companySerde)
                                .withName("source-companies-global"),
                        Materialized.<String, AvroCompany, KeyValueStore<Bytes, byte[]>>
                                        as("store-global-company")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde));
        usersStream
                .join(
                        globalCompanyTable,
                        (userId, user) -> user.getCompanyId(),
                        (user, company) -> String.format(
                                "[GLOBAL-INNER] userId=%s | name=%s %s | dept=%s | company=%s | industry=%s | country=%s",
                                user.getUserId(),
                                user.getFirstName(), user.getLastName(),
                                user.getDepartment(),
                                company.getName(), company.getIndustry(), company.getCountry()))
                .to(OutputTopics.GLOBAL_INNER,
                        Produced.<String, String>as("sink-global-inner")
                                .withValueSerde(Serdes.String()));
        usersStream
                .leftJoin(
                        globalCompanyTable,
                        (userId, user) -> user.getCompanyId(),
                        (user, company) -> {
                            String companyInfo = (company != null)
                                    ? company.getName() + " [" + company.getIndustry() + "]"
                                    : "COMPANY_NOT_YET_REPLICATED";
                            return String.format(
                                    "[GLOBAL-LEFT] userId=%s | name=%s %s | dept=%s | status=%s | company=%s",
                                    user.getUserId(),
                                    user.getFirstName(), user.getLastName(),
                                    user.getDepartment(), user.getStatus(),
                                    companyInfo);
                        })
                .to(OutputTopics.GLOBAL_LEFT,
                        Produced.<String, String>as("sink-global-left")
                                .withValueSerde(Serdes.String()));

        log.info("GlobalTableJoinTopology registered — topics: user={}, global={}",
                props.getTopics().getUser().getName(),
                props.getTopics().getCompaniesGlobal().getName());
    }
}
