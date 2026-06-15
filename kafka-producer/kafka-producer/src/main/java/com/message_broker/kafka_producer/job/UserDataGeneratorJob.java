package com.message_broker.kafka_producer.job;


import com.message_broker.kafka_producer.random.RandomUserGenerate;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@DisallowConcurrentExecution
public class UserDataGeneratorJob implements Job {

    @Autowired
    private RandomUserGenerate randomUserGenerate;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      randomUserGenerate.produceUser();
    }
}
