package com.kafka.streams.controller;

import com.kafka.streams.dto.CompanyStats;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/customers")
public class CustomerStatsController {

    private final StreamsBuilderFactoryBean factoryBean;

    public CustomerStatsController(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @GetMapping("/{customerId}/stats")
    public ResponseEntity<CompanyStats> getCustomerStats(@PathVariable String customerId) {
        // Get the Kafka Streams instance
        KafkaStreams streams = factoryBean.getKafkaStreams();

        // Query the state store directly - no database needed
        ReadOnlyKeyValueStore<String, CompanyStats> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        "company-stats-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );

        CompanyStats stats = store.get(customerId);
        if (stats == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(stats);
    }
}