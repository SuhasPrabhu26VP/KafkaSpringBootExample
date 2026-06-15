package com.message_broker.kafka_producer.job;

import com.message_broker.kafka_producer.random.RandomCompanyGenerate;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

@DisallowConcurrentExecution
public class CompanyDataGeneratorJob implements Job {
    @Autowired
    private RandomCompanyGenerate randomCompanyGenerate;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        randomCompanyGenerate.produceCompany();
    }
}
