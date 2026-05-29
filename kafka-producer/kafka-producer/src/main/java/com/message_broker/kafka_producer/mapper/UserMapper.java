package com.message_broker.kafka_producer.mapper;

import com.message_broker.kafka_producer.dto.CompanyDto;
import com.message_broker.kafka_producer.dto.UserDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper
public interface UserMapper {

    UserMapper INSTANCE = Mappers.getMapper(UserMapper.class);

    schema.avro.AvroUser toAvro(UserDto source);

    UserDto fromAvro(schema.avro.AvroUser source);
}
