package com.kafka.streams.topology;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Merger;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.streams.state.KeyValueStore;
import schema.avro.*;
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

    // TUMBLING WINDOW

    @Bean
    public KTable<Windowed<String>,AvroTemperatureAggregate> tumblingWindowTopology(
            StreamsBuilder streamsBuilder) {

        TimeWindows tumblingWindow = TimeWindows
                .ofSizeAndGrace(
                        Duration.ofMinutes(5),
                        Duration.ofSeconds(30)
                );

        KTable<Windowed<String>,AvroTemperatureAggregate> result = streamsBuilder
                .stream(tempTopicName, Consumed.with(Serdes.String(), sensorSerde()))
                .groupByKey(Grouped.with(Serdes.String(), sensorSerde()))
                .windowedBy(tumblingWindow)
                .aggregate(
                        AvroTemperatureAggregate::new,
                        this::doAggregate,
                        Materialized
                                .<String, AvroTemperatureAggregate, WindowStore<Bytes, byte[]>>
                                        as("tumbling-temp-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregateSerde())
                );

        result.toStream()
                .map((windowedKey, agg) -> {
                    agg.setWindowStart(windowedKey.window().start());
                    agg.setWindowEnd(windowedKey.window().end());
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("tumbling-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }

    // HOPPING WINDOW

    @Bean
    public KTable<Windowed<String>, AvroTemperatureAggregate> hoppingWindowTopology(
            StreamsBuilder streamsBuilder) {

        TimeWindows hoppingWindow = TimeWindows
                .ofSizeAndGrace(
                        Duration.ofMinutes(10),
                        Duration.ofSeconds(30)
                )
                .advanceBy(Duration.ofMinutes(2));

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
                    agg.setWindowStart(windowedKey.window().start());
                    agg.setWindowEnd(windowedKey.window().end());
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("hopping-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }

    // SESSION WINDOW

    @Bean
    public KTable<Windowed<String>, AvroTemperatureAggregate> sessionWindowTopology(
            StreamsBuilder streamsBuilder) {

        SessionWindows sessionWindow = SessionWindows
                .ofInactivityGapAndGrace(
                        Duration.ofMinutes(3),
                        Duration.ofSeconds(30)
                );


        KTable<Windowed<String>, AvroTemperatureAggregate> result = streamsBuilder
                .stream(tempTopicName, Consumed.with(Serdes.String(), sensorSerde()))
                .groupByKey(Grouped.with(Serdes.String(), sensorSerde()))
                .windowedBy(sessionWindow)
                .aggregate(
                        AvroTemperatureAggregate::new,
                        this::doAggregate,((aggKey, aggOne, aggTwo) -> {
                            aggTwo.setSensorId(aggOne.getSensorId());
                            aggTwo.setTotalTemp(aggOne.getTotalTemp() + aggOne.getTotalTemp());
                            aggTwo.setCount(aggOne.getCount() + aggOne.getCount());
                            aggTwo.setAverageTemp(aggOne.getTotalTemp() / aggOne.getCount());
                            return aggOne;
                        })
                );

        result.toStream()
                .map((windowedKey, agg) -> {
                    agg.setWindowStart(windowedKey.window().start());
                    agg.setWindowEnd(windowedKey.window().end());
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("session-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }

    // SLIDING WINDOW

    @Bean
    public KTable<Windowed<String>,AvroTemperatureAggregate> slidingWindowTopology(
            StreamsBuilder streamsBuilder) {

        SlidingWindows slidingWindow = SlidingWindows
                .ofTimeDifferenceAndGrace(
                        Duration.ofMinutes(5),
                        Duration.ofSeconds(30)
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
                    agg.setWindowStart(windowedKey.window().start());
                    agg.setWindowEnd(windowedKey.window().end());
                    return KeyValue.pair(windowedKey.key(), agg);
                })
                .to("sliding-temp-output",
                        Produced.with(Serdes.String(), aggregateSerde()));

        return result;
    }
}