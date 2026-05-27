package com.kafka.streams.dto;

import lombok.Data;

@Data
public class CompanyDto {
    private String name;
    private String address;
    private int employeeCount;
    private boolean softwareCompany;
    private String companyId;
    // --- routing/filtering fields ---
    private String industry;     // "TECH", "FINANCE", "HEALTH"
    private String country;      // "IN", "US", "UK"
    private double revenue;
    private long updatedAt;

}
