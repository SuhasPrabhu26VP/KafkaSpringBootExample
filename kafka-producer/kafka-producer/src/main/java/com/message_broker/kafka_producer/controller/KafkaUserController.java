package com.message_broker.kafka_producer.controller;

import com.message_broker.kafka_producer.KafkaMessageProducer;
import com.message_broker.kafka_producer.dto.UserDetails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Description;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class KafkaUserController {

    @Autowired
    private KafkaMessageProducer kafkaMessageProducer;
    @PostMapping("/userinfo")
    @Description("Send User Details to Kafka Consumer")
    public ResponseEntity<String> sendUserDetails(@RequestBody UserDetails userDetails){
        kafkaMessageProducer.sendMessage(userDetails);
        return ResponseEntity.ok("Message SENT");
    }
}
