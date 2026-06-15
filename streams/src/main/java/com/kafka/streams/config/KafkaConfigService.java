package com.kafka.streams.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

/**
 * Kafka Config Service
 */
@Service
@Slf4j
public class KafkaConfigService {

    private static final String NOT_FOUND_ERR = "Property '{}' not found, using default: {}";

    private final Environment environment;

    public KafkaConfigService(final Environment environment) {
        this.environment = environment;
    }


    public String getConfigById(final String key) {
        String value = environment.getProperty(key);
        if (value == null) {
            log.warn("Property '{}' not found, returning null", key);
        }
        return value;
    }


    public String getConfigById(final String key, final String defaultValue) {
        if (!environment.containsProperty(key)) {
            log.debug(NOT_FOUND_ERR, key, defaultValue);
            return defaultValue;
        }
        return environment.getProperty(key, defaultValue);
    }


    public <T> T getConfigByIdAndType(final String key, final Class<T> targetType) {
        try {
            T value = environment.getProperty(key, targetType);
            if (value == null) {
                log.warn("Property '{}' of type {} not found, returning null",
                        key, targetType.getSimpleName());
            }
            return value;
        } catch (Exception e) {
            log.error("Failed to convert property '{}' to type {}",
                    key, targetType.getSimpleName(), e);
            return null;
        }
    }


    public <T> T getConfigByIdAndType(final String key, final Class<T> targetType, final T defaultValue) {
        try {
            if (!environment.containsProperty(key)) {
                log.debug(NOT_FOUND_ERR, key, defaultValue);
                return defaultValue;
            }
            return environment.getProperty(key, targetType, defaultValue);
        } catch (Exception e) {
            log.error("Property '{}' conversion error, using default: {}", key, defaultValue, e);
            return defaultValue;
        }
    }


    public boolean containsProperty(final String key) {
        return environment.containsProperty(key);
    }


    public String getRequiredConfig(final String key) {
        if (!environment.containsProperty(key)) {
            throw new IllegalStateException(
                    "Required configuration property '" + key + "' is missing");
        }
        return environment.getRequiredProperty(key);
    }

    public <T> T getRequiredConfig(final String key, final Class<T> targetType) {
        if (!environment.containsProperty(key)) {
            throw new IllegalStateException(
                    "Required configuration property '" + key + "' is missing");
        }
        try {
            return environment.getRequiredProperty(key, targetType);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to convert required property '" + key +
                            "' to " + targetType.getSimpleName(), e);
        }
    }
}