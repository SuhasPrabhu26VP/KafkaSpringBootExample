package com.message_broker.kafka_producer.dto.request;

import lombok.Data;

@Data
public class UserDto {

    private String firstName;
    private String lastName;
    private int age;
    private boolean active;
}