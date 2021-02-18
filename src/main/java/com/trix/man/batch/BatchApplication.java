package com.trix.man.batch;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableBatchProcessing
@EnableScheduling
public class BatchApplication {

//    @Autowired
//    private static JobExplorer jobExplorer;
//
//    @Autowired
//    private static Job job;

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
//        BatchApplication batchApplication = new BatchApplication();
//        batchApplication.shutDown();
    }

//    public void shutDown() {
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                System.out.println("Shutdown hook ran!");
//                jobExplorer.findRunningJobExecutions(job.getName()).forEach(jobExecution -> {
//                    jobExecution.setStatus(BatchStatus.STOPPED);
//                    jobExecution.setExitStatus(ExitStatus.STOPPED);
//                    jobExecution.stop();
//                });
//            }
//        });
//
//    }

}
