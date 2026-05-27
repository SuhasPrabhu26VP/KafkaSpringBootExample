package com.kafka.streams.dto;

import lombok.Data;

@Data
public class CompanyStats {

    private String companyId;

    // --- aggregated from UserDto ---
    private int headcount;           // total number of users in this company
    private double totalSalary;      // sum of all user salaries
    private double averageSalary;    // computed after each update

    // --- department breakdown ---
    private int engineeringCount;
    private int hrCount;
    private int salesCount;
    private int financeCount;

    // --- active vs inactive breakdown ---
    private int activeCount;
    private int inactiveCount;

    // --- last updated ---
    private long lastUpdatedAt;
}
