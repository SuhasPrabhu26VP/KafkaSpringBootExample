# Kafka Spring Boot Example
Kafka SpringBoot Simple Example

Test with below API 


POST http://localhost:8083/api/v1/user

{
  "firstName": "Suhas",
  "lastName": "Prabhu",
  "age": 28,
  "active": true,
  "userId": "USER-001",
  "companyId": "COMP-001",
  "department": "ENGINEERING",
  "country": "IN",
  "salary": 2.00,
  "status": "ACTIVE",
  "createdAt": 1717000000000
}


POST http://localhost:8083/api/v1/company
{
  "name": "Kafka Solutions",
  "address": "42 Manyata Park, Bangalore",
  "employeeCount": 500,
  "softwareCompany": true,
  "companyId": "COMP-002",
  "industry": "TECH",
  "country": "IN",
  "revenue": 15000000.00,
  "updatedAt": 1717000000000
}
