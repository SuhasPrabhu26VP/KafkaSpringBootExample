package com.kafka.streams.topology;

import com.kafka.streams.dto.CompanyStats;
import com.kafka.streams.dto.UserDto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

@Component
public class UserCompanyAggregationTopology {

    public static final String COMPANY_STATS_STORE = "company-stats-store";
    @Value("${spring.kafka.topics.user.name}")
    private String userTopicName;

    @Autowired
    void buildAggregation(StreamsBuilder streamsBuilder) {
        JacksonJsonSerde<UserDto> userSerde = new JacksonJsonSerde<>(UserDto.class);
        JacksonJsonSerde<CompanyStats> companySerde = new JacksonJsonSerde<>(CompanyStats.class);
        KTable<String, CompanyStats> companyStats = streamsBuilder
                .stream(userTopicName, Consumed.with(Serdes.String(), userSerde))
                .selectKey((key, user) -> user.getCompanyId())
                .groupByKey(Grouped.with(Serdes.String(), userSerde))
                .aggregate(
                        CompanyStats::new,
                        (companyId, user, stats) -> {
                            stats.setCompanyId(companyId);
                            stats.setHeadcount(stats.getHeadcount() + 1);
                            stats.setTotalSalary(stats.getTotalSalary() + user.getSalary());
                            stats.setAverageSalary(stats.getTotalSalary() / stats.getHeadcount());
                            if ("ACTIVE".equalsIgnoreCase(user.getStatus())) {
                                stats.setActiveCount(stats.getActiveCount() + 1);
                            } else {
                                stats.setInactiveCount(stats.getInactiveCount() + 1);
                            }
                            switch (user.getDepartment() == null ? "" : user.getDepartment().toUpperCase()) {
                                case "ENGINEERING" -> stats.setEngineeringCount(stats.getEngineeringCount() + 1);
                                case "HR"          -> stats.setHrCount(stats.getHrCount() + 1);
                                case "SALES"       -> stats.setSalesCount(stats.getSalesCount() + 1);
                                case "FINANCE"     -> stats.setFinanceCount(stats.getFinanceCount() + 1);
                            }
                            stats.setLastUpdatedAt(System.currentTimeMillis());

                            return stats;
                        },
                        Materialized.<String, CompanyStats, KeyValueStore<Bytes, byte[]>>
                                        as(COMPANY_STATS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde)
                );
        companyStats
                .toStream()
                .to("company-statistics", Produced.with(Serdes.String(), companySerde));
    }
}
