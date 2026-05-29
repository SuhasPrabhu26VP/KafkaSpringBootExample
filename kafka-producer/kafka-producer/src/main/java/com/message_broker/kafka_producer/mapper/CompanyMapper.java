package com.message_broker.kafka_producer.mapper;


import com.message_broker.kafka_producer.dto.CompanyDto;
import com.message_broker.kafka_producer.dto.UserDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper
public interface CompanyMapper {
    CompanyMapper INSTANCE = Mappers.getMapper(CompanyMapper.class);

    schema.avro.AvroCompany toAvro(CompanyDto source);

    CompanyDto fromAvro(schema.avro.AvroCompany source);
}
