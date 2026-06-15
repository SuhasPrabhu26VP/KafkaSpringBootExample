package com.message_broker.kafka_producer.random;

import com.message_broker.kafka_producer.service.NameFetcherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import schema.avro.*;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class RandomUserGenerate {
    @Value("${kafka.topics.user.name}")
    private String userTopicName;

    private final KafkaTemplate<String, schema.avro.AvroUser> userKafkaTemplate;
    private final NameFetcherService fetcherService;



    public void produceUser() throws JobExecutionException {
            final AvroUser user =  createRandomUser();
            ProducerRecord<String, AvroUser> record =
                    new ProducerRecord<>(userTopicName, user.getUserId(), user);

            userKafkaTemplate.send(record)
                    .thenAccept(result -> log.info(
                            "Sent User: {} : {}",
                            user.getFirstName()+" "+user.getLastName(),
                            result.getRecordMetadata().partition()))
                    .exceptionally(ex -> {
                        log.error("Failed to send for user: {}",  user.getFirstName()+" "+user.getLastName(), ex);
                        return null;
                    });
    }

    private AvroUser createRandomUser() throws JobExecutionException {
        AvroUser user = new AvroUser();
        user.setActive(ThreadLocalRandom.current().nextBoolean());
        user.setUserId("USER-" + ThreadLocalRandom.current().nextInt(1000));
        user.setCompanyId("COMP-0" + ThreadLocalRandom.current().nextInt(50));
        String fullName = fetcherService.getRandomUserName();
        if (fullName == null || fullName.isBlank()) {
            throw new JobExecutionException("No user names available in cache");
        }
        String[] nameParts = fullName.trim().split("\\s+");
        String firstName = nameParts[0];
        String lastName = nameParts.length > 1 ? nameParts[1] : "";
        user.setFirstName(firstName);
        user.setLastName(lastName);
        UserStatus[] statuses = UserStatus.values();
        user.setStatus(statuses[ThreadLocalRandom.current().nextInt(statuses.length)]);
        Country[] countries = Country.values();
        user.setCountry(countries[ThreadLocalRandom.current().nextInt(countries.length)]);
        Department[] departments = Department.values();
        user.setDepartment(departments[ThreadLocalRandom.current().nextInt(departments.length)]);
        user.setCreatedAt(Instant.now().toEpochMilli());
        user.setAge(ThreadLocalRandom.current().nextInt(20, 61));
        user.setSalary(30000 + ThreadLocalRandom.current().nextDouble() * 120000);
        return user;
    }
}
