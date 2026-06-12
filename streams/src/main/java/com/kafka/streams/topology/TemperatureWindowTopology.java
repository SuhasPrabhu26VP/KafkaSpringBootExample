package com.kafka.streams.topology;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.SessionStore;
import schema.avro.AvroTemperatureAggregate;
import schema.avro.AvroTemperatureSensor;

import java.time.Duration;
import java.util.Map;

@Configuration
@Slf4j
public class TemperatureWindowTopology {

    @Value("${spring.kafka.topics.temp.name}")
    private String tempTopicName;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    private SpecificAvroSerde<AvroTemperatureSensor> sensorSerde() {
        SpecificAvroSerde<AvroTemperatureSensor> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                false
        );
        return serde;
    }

    private SpecificAvroSerde<AvroTemperatureAggregate> aggregateSerde() {
        SpecificAvroSerde<AvroTemperatureAggregate> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                false
        );
        return serde;
    }

    /**
     * Aggregator: called for every incoming sensor record.
     * agg is always a fresh AvroTemperatureAggregate instance from the initializer — never null.
     * totalTemp is double in Avro schema, so no cast needed.
     */
    private AvroTemperatureAggregate doAggregate(
            String sensorId,
            AvroTemperatureSensor sensor,
            AvroTemperatureAggregate agg) {

        agg.setSensorId(sensorId);
        agg.setTotalTemp(agg.getTotalTemp() + sensor.getTemp());
        agg.setCount(agg.getCount() + 1);
        agg.setAverageTemp(agg.getTotalTemp() / agg.getCount());
        return agg;
    }

    /**
     * TUMBLING WINDOW
     *
     * Fixed size, non-overlapping windows.
     * Each event belongs to exactly one window.
     * Example: count sensor readings every 5 minutes.
     *
     * Window: |--5min--|--5min--|--5min--|
     */
    @Bean
    public KTable<Windowed<String>, AvroTemperatureAggregate> tumblingWindowTopology(
            StreamsBuilder streamsBuilder) {

        TimeWindows tumblingWindow = TimeWindows
                .ofSizeAndGrace(
                        Duration.ofMinutes(5),   // window size
                        Duration.ofSeconds(30)   // how long to wait for late records
                );

        KTable<Windowed<String>, AvroTemperatureAggregate> result = streamsBuilder
                .stream(tempTopicName, Consumed.with(Serdes.String(), sensorSerde()))
                .groupByKey(Grouped.with(Serdes.String(), sensorSerde()))
                .windowedBy(tumblingWindow)
                .aggregate(
                        AvroTemperatureAggregate::new,  // initializer: always provides fresh agg, never null
                        this::doAggregate,
                        Materialized
                                .<String, AvroTemperatureAggregate, WindowStore<Bytes, byte[]>>
                                        as("tumbling-temp-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregateSerde())
                );

        result.toStream()
                .map((windowedKey, agg) -> {
                    if (agg != null) {
                        agg.setWindowStart(windowedKey.window().start());
                        agg.setWindowEnd(windowedKey.window().end());
                    }
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("tumbling-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }

    /**
     * HOPPING WINDOW
     *
     * Fixed size, overlapping windows. Advances by a smaller interval than window size.
     * Same event can appear in multiple windows.
     * Example: rolling 10-minute average, recalculated every 2 minutes.
     *
     * Window: |----10min----|
     *              |----10min----|
     *                   |----10min----|
     *         ^2min^2min^2min (advance interval)
     */
    @Bean
    public KTable<Windowed<String>, AvroTemperatureAggregate> hoppingWindowTopology(
            StreamsBuilder streamsBuilder) {

        TimeWindows hoppingWindow = TimeWindows
                .ofSizeAndGrace(
                        Duration.ofMinutes(10),  // window size
                        Duration.ofSeconds(30)   // grace period for late records
                )
                .advanceBy(Duration.ofMinutes(2)); // hop interval — must be < window size

        KTable<Windowed<String>, AvroTemperatureAggregate> result = streamsBuilder
                .stream(tempTopicName, Consumed.with(Serdes.String(), sensorSerde()))
                .groupByKey(Grouped.with(Serdes.String(), sensorSerde()))
                .windowedBy(hoppingWindow)
                .aggregate(
                        AvroTemperatureAggregate::new,
                        this::doAggregate,
                        Materialized
                                .<String, AvroTemperatureAggregate, WindowStore<Bytes, byte[]>>
                                        as("hopping-temp-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregateSerde())
                );

        result.toStream()
                .map((windowedKey, agg) -> {
                    if (agg != null) {
                        agg.setWindowStart(windowedKey.window().start());
                        agg.setWindowEnd(windowedKey.window().end());
                    }
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("hopping-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }

    /**
     * SESSION WINDOW
     *
     * Dynamic, activity-driven windows — NOT clock-aligned.
     * A window stays open as long as events keep arriving within the inactivity gap.
     * When no event arrives for 3 minutes, the session closes.
     * If a late record bridges two sessions, they are MERGED via sessionMerger.
     * Example: group all readings from a sensor during an active burst.
     *
     * Window: [A--B--C]  3min gap  [D--E]
     *          session1              session2
     */
    @Bean
    public KTable<Windowed<String>, AvroTemperatureAggregate> sessionWindowTopology(
            StreamsBuilder streamsBuilder) {

        SessionWindows sessionWindow = SessionWindows
                .ofInactivityGapAndGrace(
                        Duration.ofMinutes(3),   // close session after 3min of silence
                        Duration.ofSeconds(30)   // grace period for late records
                );

        /**
         * Merger: called when a late record bridges two previously separate sessions.
         * Both aggOne and aggTwo are valid aggregates — combine them into one.
         * windowStart/windowEnd are NOT set here intentionally —
         * they are set downstream in the .map() on toStream() using the Windowed key,
         * which reflects the final merged window boundaries.
         */
        Merger<String, AvroTemperatureAggregate> sessionMerger = (aggKey, aggOne, aggTwo) -> {
            AvroTemperatureAggregate merged = new AvroTemperatureAggregate();
            merged.setSensorId(aggKey);
            merged.setTotalTemp(aggOne.getTotalTemp() + aggTwo.getTotalTemp());
            merged.setCount(aggOne.getCount() + aggTwo.getCount());
            if (merged.getCount() > 0) {
                merged.setAverageTemp(merged.getTotalTemp() / merged.getCount());
            }
            return merged;
        };

        KTable<Windowed<String>, AvroTemperatureAggregate> result = streamsBuilder
                .stream(tempTopicName, Consumed.with(Serdes.String(), sensorSerde()))
                .groupByKey(Grouped.with(Serdes.String(), sensorSerde()))
                .windowedBy(sessionWindow)
                .aggregate(
                        AvroTemperatureAggregate::new,
                        this::doAggregate,
                        sessionMerger,
                        Materialized
                                .<String, AvroTemperatureAggregate, SessionStore<Bytes, byte[]>>
                                        as("session-temp-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregateSerde())
                );

        result.toStream()
                .map((windowedKey, agg) -> {
                    if (agg != null) {
                        agg.setWindowStart(windowedKey.window().start());
                        agg.setWindowEnd(windowedKey.window().end());
                    }
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("session-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }

    /**
     * SLIDING WINDOW
     *
     * Event-driven windows — NOT clock-aligned like tumbling/hopping.
     * A window contains all records whose timestamps are within 5 minutes OF EACH OTHER.
     * A new window opens/closes every time an event arrives or drops out of the time difference.
     * This is NOT "last 5 minutes from wall clock" — it's relative to event timestamps.
     * Example: detect if the same sensor fires multiple times within any 5-minute span.
     *
     * Window: events A(t=0), B(t=3), C(t=6), D(t=12)
     *   w1: [A,B,C] — all within 5min of each other
     *   w2: [B,C] — A drops out
     *   w3: [C,D] — new window when D arrives within 5min of C
     */
    @Bean
    public KTable<Windowed<String>, AvroTemperatureAggregate> slidingWindowTopology(
            StreamsBuilder streamsBuilder) {

        SlidingWindows slidingWindow = SlidingWindows
                .ofTimeDifferenceAndGrace(
                        Duration.ofMinutes(5),   // max time difference between events in the same window
                        Duration.ofSeconds(30)   // grace period for late records
                );

        KTable<Windowed<String>, AvroTemperatureAggregate> result = streamsBuilder
                .stream(tempTopicName, Consumed.with(Serdes.String(), sensorSerde()))
                .groupByKey(Grouped.with(Serdes.String(), sensorSerde()))
                .windowedBy(slidingWindow)
                .aggregate(
                        AvroTemperatureAggregate::new,
                        this::doAggregate,
                        Materialized
                                .<String, AvroTemperatureAggregate, WindowStore<Bytes, byte[]>>
                                        as("sliding-temp-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregateSerde())
                );

        result.toStream()
                .map((windowedKey, agg) -> {
                    if (agg != null) {
                        agg.setWindowStart(windowedKey.window().start());
                        agg.setWindowEnd(windowedKey.window().end());
                    }
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("sliding-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }
}