package com.kafka.streams.dto;

import lombok.Data;

@Data
public class CompanyDto {
    private String name;
    private String address;
    private int employeeCount;
    private boolean softwareCompany;

    // --- key field (matches UserDto.companyId — used in joins) ---
    private String companyId;    // unique identifier — becomes the KTable key

    // --- routing/filtering fields ---
    private String industry;     // "TECH", "FINANCE", "HEALTH" — for branch routing
    private String country;      // "IN", "US", "UK" — for regional filtering

    // --- aggregation fields ---
    private double revenue;      // can be aggregated per industry

    // --- time field ---
    private long updatedAt;      // epoch millis

}
