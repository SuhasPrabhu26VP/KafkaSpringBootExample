package com.message_broker.kafka_consumer.dto;

import lombok.Data;

@Data
public class CompanyData {
    private String name;
    private String address;
    private int employeeCount;
    private boolean softwareCompany;
}
