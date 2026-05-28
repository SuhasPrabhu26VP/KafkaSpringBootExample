package com.message_broker.kafka_producer.controller;

import com.message_broker.kafka_producer.KafkaMessageProducer;
import com.message_broker.kafka_producer.dto.CompanyData;
import com.message_broker.kafka_producer.dto.CompanyDto;
import com.message_broker.kafka_producer.dto.UserDto;
import com.message_broker.kafka_producer.dto.request.UserDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1")
public class KafkaUserController {

    private final KafkaMessageProducer producerService;

    @PostMapping("/user")
    public ResponseEntity<String> produceUserToKafka(@RequestBody UserDto user) {
        schema.avro.User userAvro = schema.avro.User.newBuilder()
                .setFirstName(user.getFirstName())
                .setLastName(user.getLastName())
                .setAge(user.getAge())
                .setActive(user.isActive())
                .build();
        producerService.produceUser(userAvro);
        return ResponseEntity.ok("OK");
    }

    @PostMapping("/company")
    public ResponseEntity<String> produceCompanyToKafka(@RequestBody CompanyDto company) {
        producerService.produceCompany(company);
        return ResponseEntity.ok("OK");
    }

    @PostMapping("/message")
    public ResponseEntity<String> produceMessageIdToKafka(@RequestBody String messageId) {
        producerService.produceMessageId(messageId);
        return ResponseEntity.ok("OK");
    }

}
