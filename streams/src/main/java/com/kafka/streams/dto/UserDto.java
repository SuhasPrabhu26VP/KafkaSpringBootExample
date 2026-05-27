package com.kafka.streams.dto;

import lombok.Data;

@Data
public class UserDto {
    private String firstName;
    private String lastName;
    private int age;
    private boolean active;

    // --- key fields (needed for groupByKey / selectKey / joins) ---
    private String userId;       // unique identifier — becomes the KStream key
    private String companyId;    // foreign key linking user to CompanyData

    // --- grouping fields (useful for groupBy before aggregation) ---
    private String department;   // "ENGINEERING", "HR", "SALES", "FINANCE"
    private String country;      // "IN", "US", "UK" — for regional branching

    // --- aggregation fields ---
    private double salary;       // aggregated into totalSalary per company

    // --- filtering fields ---
    private String status;       // "ACTIVE", "INACTIVE", "SUSPENDED" — used in filter()

    // --- time field (needed for windowed aggregations) ---
    private long createdAt;      // epoch millis — e.g. System.currentTimeMillis()

}
