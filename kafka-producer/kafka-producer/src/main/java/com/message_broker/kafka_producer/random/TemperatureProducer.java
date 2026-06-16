package com.message_broker.kafka_producer.random;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import schema.avro.AvroTemperatureSensor;

import java.util.List;
import java.util.Random;

@Component
@Slf4j
@RequiredArgsConstructor
public class TemperatureProducer {

    @Value("${kafka.topics.temp.name}")
    private String tempTopicName;

    private final KafkaTemplate<String, AvroTemperatureSensor> tempKafkaTemplate;

    private static final List<String> SENSOR_IDS = List.of(
            "SENSOR-001", "SENSOR-002", "SENSOR-003", "SENSOR-004"
    );

  /*  @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        produceTemp();
    }*/

    public void produceTemp() {
        Random random = new Random();

        while (true) {
            int temperature = random.nextInt(61) - 10;
            String sensorId = SENSOR_IDS.get(random.nextInt(SENSOR_IDS.size()));

            AvroTemperatureSensor sensor = new AvroTemperatureSensor();
            sensor.setSensorId(sensorId);
            sensor.setTemp(temperature);
            sensor.setCreatedTime(System.currentTimeMillis());

            ProducerRecord<String, AvroTemperatureSensor> record =
                    new ProducerRecord<>(tempTopicName, sensorId, sensor);

            tempKafkaTemplate.send(record)
                    .thenAccept(result -> log.info(
                            "Sent sensorId: {} temp: {}°C to partition: {}",
                            sensorId, temperature,
                            result.getRecordMetadata().partition()))
                    .exceptionally(ex -> {
                        log.error("Failed to send for sensorId: {}", sensorId, ex);
                        return null;
                    });

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
