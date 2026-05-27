package com.kafka.streams.dto;

import lombok.Data;

@Data
public class UserDto {
    private String firstName;
    private String lastName;
    private int age;
    private boolean active;

    private String userId;
    private String companyId;

    private String department;   // "ENGINEERING", "HR", "SALES", "FINANCE"
    private String country;      // "IN", "US", "UK"

    private double salary;

    private String status;       // "ACTIVE", "INACTIVE", "SUSPENDED"

    private long createdAt;

}
