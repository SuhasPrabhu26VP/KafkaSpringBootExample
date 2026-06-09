package com.kafka.streams.topology;

import com.kafka.streams.dto.CompanyStats;
import com.kafka.streams.dto.UserDto;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

import java.util.Map;

@Configuration
public class UserCompanyAggregationTopology {

    private static final Logger log = LoggerFactory.getLogger(UserCompanyAggregationTopology.class);

    public static final String COMPANY_STATS_STORE = "company-stats-store";

    @Value("${spring.kafka.topics.user.name}")
    private String userTopicName;

    @Value("${spring.kafka.topics.company.statistics}")
    private String companyStatsTopic;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KTable<String, schema.avro.AvroCompanyStats> buildAggregation(StreamsBuilder streamsBuilder) {
        SpecificAvroSerde<schema.avro.AvroUser> userSerde         = buildSerde();
        SpecificAvroSerde<schema.avro.AvroCompanyStats> statsSerde = buildSerde();

        KTable<String, schema.avro.AvroCompanyStats> companyStats = streamsBuilder
                .stream(userTopicName, Consumed.with(Serdes.String(), userSerde))
                .filter((key, user) -> user != null && user.getCompanyId() != null)
                .selectKey((key, user) -> user.getCompanyId().toString())
                .groupByKey(Grouped.with(Serdes.String(), userSerde))
                .aggregate(
                        () -> new schema.avro.AvroCompanyStats(),
                        (companyId, user, stats) -> aggregate(companyId, user, stats),
                        Materialized.<String, schema.avro.AvroCompanyStats, KeyValueStore<Bytes, byte[]>>
                                        as(COMPANY_STATS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(statsSerde)
                );

        companyStats
                .toStream()
                .to(companyStatsTopic, Produced.with(Serdes.String(), statsSerde));

        return companyStats;
    }

    private schema.avro.AvroCompanyStats aggregate(String companyId, schema.avro.AvroUser user, schema.avro.AvroCompanyStats stats) {
        stats.setCompanyId(companyId);
        stats.setHeadcount(stats.getHeadcount() + 1);

        double newTotalSalary = stats.getTotalSalary() + user.getSalary();
        stats.setTotalSalary(newTotalSalary);
        stats.setAverageSalary(newTotalSalary / stats.getHeadcount());

        if ("ACTIVE".equalsIgnoreCase(user.getStatus().toString())) {
            stats.setActiveCount(stats.getActiveCount() + 1);
        } else {
            stats.setInactiveCount(stats.getInactiveCount() + 1);
        }

        String dept = user.getDepartment() == null ? "" : user.getDepartment().toString().toUpperCase();
        switch (dept) {
            case "ENGINEERING" -> stats.setEngineeringCount(stats.getEngineeringCount() + 1);
            case "HR"          -> stats.setHrCount(stats.getHrCount() + 1);
            case "SALES"       -> stats.setSalesCount(stats.getSalesCount() + 1);
            case "FINANCE"     -> stats.setFinanceCount(stats.getFinanceCount() + 1);
            default            -> log.warn("Unknown department '{}' for company {}", dept, companyId);
        }

        stats.setLastUpdatedAt(System.currentTimeMillis());
        return stats;
    }

    private <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> buildSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                false
        );
        return serde;
    }
}
