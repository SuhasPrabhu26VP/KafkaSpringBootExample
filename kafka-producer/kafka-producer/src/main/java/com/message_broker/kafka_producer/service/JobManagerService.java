package com.message_broker.kafka_producer.service;

import com.message_broker.kafka_producer.job.CompanyDataGeneratorJob;
import com.message_broker.kafka_producer.job.TempDataGeneratorJob;
import com.message_broker.kafka_producer.job.UserDataGeneratorJob;
import lombok.RequiredArgsConstructor;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class JobManagerService {

    private final Scheduler scheduler;

    private static final Map<String, Class<? extends Job>> JOB_REGISTRY = Map.of(
            "user-generator",    UserDataGeneratorJob.class,
            "company-generator", CompanyDataGeneratorJob.class,
            "temp-generator", TempDataGeneratorJob.class
    );

    public String startJob(String jobCode) throws SchedulerException {

        Class<? extends Job> jobClass = JOB_REGISTRY.get(jobCode.toLowerCase());
        if (jobClass == null) {
            throw new IllegalArgumentException("Invalid job code: " + jobCode);
        }

        String jobId = jobCode + "-" + UUID.randomUUID();

        JobDetail job = JobBuilder.newJob(jobClass)
                .withIdentity(jobId)
                .usingJobData("jobCode", jobCode)
                .storeDurably()
                .build();

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(jobId + "-trigger")
                .withSchedule(SimpleScheduleBuilder
                        .repeatSecondlyForever(1))
                .build();

        scheduler.scheduleJob(job, trigger);
        return jobId;
    }

    public void stopJob(String jobId) throws SchedulerException {
        if (scheduler.getJobDetail(new JobKey(jobId)) == null) {
            throw new IllegalArgumentException("Job not found: " + jobId);
        }
        scheduler.deleteJob(new JobKey(jobId));
    }

    public String getStatus(String jobId) throws SchedulerException {
        JobDetail job = scheduler.getJobDetail(new JobKey(jobId));
        if (job == null) return "NOT_FOUND";

        List<? extends Trigger> triggers = scheduler.getTriggersOfJob(new JobKey(jobId));
        if (triggers.isEmpty()) return "STOPPED";

        Trigger.TriggerState state = scheduler.getTriggerState(
                triggers.get(0).getKey());
        return state.name();
    }

    public List<String> listRunningJobs() throws SchedulerException {
        return scheduler.getJobKeys(GroupMatcher.anyGroup())
                .stream()
                .map(JobKey::getName)
                .toList();
    }


    public List<String> listAvailableJobCodes() {
        return JOB_REGISTRY.keySet().stream().sorted().toList();
    }
}