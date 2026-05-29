package com.message_broker.kafka_producer.mapper;

import com.message_broker.kafka_producer.dto.CompanyDto;
import com.message_broker.kafka_producer.dto.UserDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper
public interface UserMapper {
    UserMapper INSTANCE = Mappers.getMapper(UserMapper.class);

    @Mapping(source = "companyId", target = "companyId")
    @Mapping(source = "name", target = "name")
    @Mapping(source = "address", target = "address")
    @Mapping(source = "employeeCount", target = "employeeCount")
    @Mapping(source = "softwareCompany", target = "softwareCompany")
    @Mapping(source = "industry", target = "industry")
    @Mapping(source = "country", target = "country")
    @Mapping(source = "revenue", target = "revenue")
    @Mapping(source = "updatedAt", target = "updatedAt")
    schema.avro.AvroUser toAvro(UserDto source);

    @Mapping(source = "companyId", target = "companyId")
    @Mapping(source = "name", target = "name")
    @Mapping(source = "address", target = "address")
    @Mapping(source = "employeeCount", target = "employeeCount")
    @Mapping(source = "softwareCompany", target = "softwareCompany")
    @Mapping(source = "industry", target = "industry")
    @Mapping(source = "country", target = "country")
    @Mapping(source = "revenue", target = "revenue")
    @Mapping(source = "updatedAt", target = "updatedAt")
    UserDto fromAvro(schema.avro.AvroUser source);
}
