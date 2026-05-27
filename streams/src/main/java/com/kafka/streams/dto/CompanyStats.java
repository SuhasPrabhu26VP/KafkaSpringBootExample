package com.kafka.streams.dto;

import lombok.Data;

@Data
public class CompanyStats {
    private String companyId;
    private int headcount;
    private double totalSalary;
    private double averageSalary;
    private int engineeringCount;
    private int hrCount;
    private int salesCount;
    private int financeCount;
    private int activeCount;
    private int inactiveCount;
    private long lastUpdatedAt;
}
