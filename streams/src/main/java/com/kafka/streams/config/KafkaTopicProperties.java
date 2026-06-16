package com.kafka.streams.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

@Getter
@Setter
@Validated
@Component
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaTopicProperties {

    private Topics topics = new Topics();
    private JoinWindows joinWindows = new JoinWindows();

    @Getter
    @Setter
    public static class Topics {

        private User user = new User();
        private Company company = new Company();
        private Message message = new Message();
        private Temp temp = new Temp();



        @Getter @Setter
        public static class User {
            @NotBlank private String name = "user-topic";
        }

        @Getter @Setter
        public static class Company {
            @NotBlank private String name = "company-topic";
            @NotBlank private String statistics = "company-stats-topic";
        }

        @Getter @Setter
        public static class Message {
            @NotBlank private String name = "message-topic";
        }

        @Getter @Setter
        public static class Temp {
            @NotBlank private String name = "temperature-sensor";
        }

    }

    @Getter
    @Setter
    public static class JoinWindows {
        @Positive private long streamStreamMinutes = 5L;
        @Positive private long gracePeriodSeconds = 30L;
        @Positive private long versionedStoreRetentionHours = 24L;
        @Positive private long asymmetricAfterMinutes = 10L;
    }
}