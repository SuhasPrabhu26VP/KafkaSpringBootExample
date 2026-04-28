package com.message_broker.kafka_consumer.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record UserDetail (
        @JsonProperty("id") Long id ,
        @JsonProperty("firstName") String firstName,
        @JsonProperty("lastName") String lastName){ }
