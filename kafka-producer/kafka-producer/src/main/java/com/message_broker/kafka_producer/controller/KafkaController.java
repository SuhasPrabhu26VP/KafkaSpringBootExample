package com.message_broker.kafka_producer.controller;

import com.message_broker.kafka_producer.KafkaMessageProducer;
import com.message_broker.kafka_producer.dto.CompanyDto;
import com.message_broker.kafka_producer.dto.UserDto;
import com.message_broker.kafka_producer.mapper.CompanyMapper;
import com.message_broker.kafka_producer.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/")
public class KafkaController {

    private final KafkaMessageProducer producerService;

    @PostMapping("/user")
    public ResponseEntity<String> produceUserToKafka(@RequestBody UserDto user) {
        schema.avro.AvroUser userAvro = UserMapper.INSTANCE.toAvro(user);
        producerService.produceUserWithRelatedData(userAvro);
        return ResponseEntity.ok("OK");
    }

    @PostMapping("/company")
    public ResponseEntity<String> produceCompanyToKafka(@RequestBody CompanyDto company) {
        schema.avro.AvroCompany avroCompany = CompanyMapper.INSTANCE.toAvro(company);
        producerService.produceCompanyWithRelatedData(avroCompany);
        return ResponseEntity.ok("OK");
    }

    @PostMapping("/message")
    public ResponseEntity<String> produceMessageIdToKafka(@RequestBody String messageId) {
        producerService.produceMessageId(messageId);
        return ResponseEntity.ok("OK");
    }

}
