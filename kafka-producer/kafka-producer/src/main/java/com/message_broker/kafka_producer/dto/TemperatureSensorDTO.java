package com.message_broker.kafka_producer.dto;

import lombok.Data;

import java.util.Date;

@Data
public class TemperatureSensorDTO {
    private String sensorId;
    private int temp;
    private Date createdTime;
}
