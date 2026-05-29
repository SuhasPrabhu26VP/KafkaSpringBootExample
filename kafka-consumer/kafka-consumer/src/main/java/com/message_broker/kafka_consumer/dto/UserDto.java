package com.message_broker.kafka_consumer.dto;

import lombok.Data;

@Data
public class UserDto {
    private String firstName;
    private String lastName;
    private int age;
    private boolean active;

    private String userId;
    private String companyId;

    private String department;
    private String country;
    private double salary;
    private String status;
    private long createdAt;
}