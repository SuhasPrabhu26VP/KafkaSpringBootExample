package com.message_broker.kafka_producer.controller;

import com.message_broker.kafka_producer.service.JobManagerService;
import lombok.RequiredArgsConstructor;
import org.quartz.SchedulerException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class JobController {
    private final JobManagerService jobManagerService;

    @PostMapping("/start")
    public ResponseEntity<Map<String, String>> start(@RequestParam String jobCode)
            throws SchedulerException {
        String jobId = jobManagerService.startJob(jobCode);
        return ResponseEntity.ok(Map.of(
                "jobId", jobId,
                "status", "STARTED"
        ));
    }

    @DeleteMapping("/{jobId}/stop")
    public ResponseEntity<String> stop(@PathVariable String jobId)
            throws SchedulerException {
        jobManagerService.stopJob(jobId);
        return ResponseEntity.ok("Job " + jobId + " stopped");
    }

    @GetMapping("/{jobId}/status")
    public ResponseEntity<String> status(@PathVariable String jobId)
            throws SchedulerException {
        return ResponseEntity.ok(jobManagerService.getStatus(jobId));
    }

    @GetMapping
    public ResponseEntity<List<String>> listAll() throws SchedulerException {
        return ResponseEntity.ok(jobManagerService.listRunningJobs());
    }
}
