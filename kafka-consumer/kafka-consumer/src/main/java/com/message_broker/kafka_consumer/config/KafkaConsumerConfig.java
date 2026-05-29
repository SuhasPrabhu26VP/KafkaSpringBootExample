package com.message_broker.kafka_consumer.config;

import com.message_broker.kafka_consumer.dto.CompanyDto;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;



import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    @Value("${kafka.brokerAddress}")
    private String brokerAddress;

    @Value("${kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;


    @Bean
    public ConsumerFactory<String, schema.avro.AvroUser> userConsumerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);

        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, schema.avro.AvroUser> userKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, schema.avro.AvroUser> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(userConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, schema.avro.AvroCompany> companyConsumerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);

        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, CompanyDto.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,  schema.avro.AvroCompany> companyKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String,  schema.avro.AvroCompany> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(companyConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> messageIdConsumerFactory() {
        Map<String, Object> props = createDefaultProps();

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> messageIdKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(messageIdConsumerFactory());
        return factory;
    }


    private Map<String, Object> createDefaultProps() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return props;
    }
}
