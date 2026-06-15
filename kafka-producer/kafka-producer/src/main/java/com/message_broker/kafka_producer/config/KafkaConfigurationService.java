package com.message_broker.kafka_producer.config;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
@RefreshScope
public class KafkaConfigurationService {
    @Autowired
    private Environment env;

    public String get(String key) {
        return env.getProperty(key);
    }
}
