package com.ddori.sample.batch.scheduler;

import com.ddori.sample.batch.job.PayCursorJobConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
//@Component
public class PayScheduler {
    private final JobLauncher jobLauncher;
    private final PayCursorJobConfig payCursorJobConfig;

    //@Scheduled(initialDelay = 10000, fixedDelay = 1000, cron = "1 * * * * ? *")
    public void runPaySuccessJob() {
        try {
            /*
            Map<String, JobParameter> confMap = new HashMap<>();
            confMap.put("time", new JobParameter(System.currentTimeMillis()));
            JobParameters jobParameters = new JobParameters(confMap);
            */
            jobLauncher.run(payCursorJobConfig.payPagingJob(), null);
        } catch (JobExecutionAlreadyRunningException | JobInstanceAlreadyCompleteException
                | JobParametersInvalidException | org.springframework.batch.core.repository.JobRestartException e) {

            log.error(e.getMessage());
        }

    }

}
