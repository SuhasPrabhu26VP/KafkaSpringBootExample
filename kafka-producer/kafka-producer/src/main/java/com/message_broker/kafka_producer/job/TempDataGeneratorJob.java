package com.message_broker.kafka_producer.job;

import com.message_broker.kafka_producer.random.RandomCompanyGenerate;
import com.message_broker.kafka_producer.random.TemperatureProducer;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

@DisallowConcurrentExecution
public class TempDataGeneratorJob implements Job {
    @Autowired
    private TemperatureProducer temperatureProducer;
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        temperatureProducer.produceTemp();
    }

}
