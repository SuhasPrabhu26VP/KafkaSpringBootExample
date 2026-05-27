package com.kafka.streams.topology;

import com.kafka.streams.dto.CompanyDto;
import com.kafka.streams.dto.UserDto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

@Component
public class UserCompanyAggregationTopology {
    @Autowired
    void buildAggregation(StreamsBuilder streamsBuilder) {
        JacksonJsonSerde<UserDto> userSerder = new JacksonJsonSerde<>(UserDto.class);
        JacksonJsonSerde<CompanyDto> companySerde = new JacksonJsonSerde<>(CompanyDto.class);

        // Read orders and group by customer ID
        KTable<String, CompanyDto> customerStats = streamsBuilder
                .stream("orders", Consumed.with(Serdes.String(), userSerder))
                // Re-key by customer ID for aggregation
                .selectKey((key, user) -> user.getCompany())
                // Group by the new key
                .groupByKey(Grouped.with(Serdes.String(), userSerder))
                // Aggregate into customer statistics
                .aggregate(
                        // Initializer - creates empty stats for new customers
                        CompanyDto::new,
                        // Aggregator - updates stats with each order
                        (customerId, user, comp) -> {
                            comp.setId(customerId);
                            comp.setName(user.getFirstName());
                            return comp;
                        },
                        // Materialized view configuration
                        Materialized.<String, CompanyDto, KeyValueStore<Bytes, byte[]>>as("customer-stats-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde)
                );

        // Output the aggregated stats to a topic
        customerStats.toStream().to("customer-statistics",
                Produced.with(Serdes.String(), companySerde));
    }
}
