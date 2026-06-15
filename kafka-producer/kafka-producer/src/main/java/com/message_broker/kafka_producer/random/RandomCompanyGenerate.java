package com.message_broker.kafka_producer.random;


import com.message_broker.kafka_producer.service.NameFetcherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
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
public class RandomCompanyGenerate {
    @Value("${kafka.topics.company.name}")
    private String companyTopicName;

    private final KafkaTemplate<String, AvroCompany> companyKafkaTemplate;

    @Autowired
    private NameFetcherService fetcherService;



    public void produceCompany() {
        final AvroCompany company =  createRandomCompanyData();
        ProducerRecord<String, schema.avro.AvroCompany> producerRecord =
                new ProducerRecord<>(companyTopicName, company.getCompanyId(), company);
        companyKafkaTemplate.send(producerRecord)
                .thenAccept(result ->
                        log.info("Company sent: {} to partition: {}",
                                company.getCompanyId(), result.getRecordMetadata().partition()))
                .exceptionally(ex -> {
                    log.error("Failed to send company: {}", company.getCompanyId(), ex);
                    return null;
                });
    }

    private AvroCompany createRandomCompanyData() {
        AvroCompany company = new AvroCompany();
        company.setCompanyId("COMP-00" + ThreadLocalRandom.current().nextInt(0, 50));
        String[] companyNameAndAddress = splitNameAndAddress(fetcherService.getRandomCompanyName());
        company.setName(companyNameAndAddress[0]);
        Industry[] industries = Industry.values();
        company.setIndustry(industries[ThreadLocalRandom.current().nextInt(industries.length)]);
        Country[] countries = Country.values();
        company.setCountry(countries[ThreadLocalRandom.current().nextInt(countries.length)]);
        company.setAddress(companyNameAndAddress[1]);
        company.setEmployeeCount(ThreadLocalRandom.current().nextInt(10, 10001));
        company.setRevenue(100000 + ThreadLocalRandom.current().nextDouble() * 99900000);
        company.setUpdatedAt(Instant.now().toEpochMilli());
        return company;
    }
    public static String[] splitNameAndAddress(String line) {
        String[] parts = line.split("\\|", 2);
        if (parts.length == 2) {
            return parts;
        } else {
            return new String[]{line, ""};
        }
    }
}
