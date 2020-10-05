package com.trix.man.batch.config;

import com.trix.man.batch.logic.DeptFilterProcessor;
import com.trix.man.batch.logic.UserDBWriter;
import com.trix.man.batch.logic.UserItemReader;
import com.trix.man.batch.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import javax.annotation.PreDestroy;
import java.util.Arrays;

@Configuration
public class BatchConfig {

    private static final String CSV_FILE = "/Users/chakkaru/Desktop/GCP/batch/batch/src/main/resources/user.csv";

    @Value("${taxis.batch.chunk.size}")
    private int chunkSize;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private static final String JOB_NAME = "taxis-batch";

    @Autowired
    private JobExplorer jobs;

    @Autowired
    private JobLauncher jobLauncher;

    @Bean
    public Job job(){
        return jobBuilderFactory.get(JOB_NAME)
                .incrementer(new RunIdIncrementer())
                .start(chunkStep())
                .build();

    }

    @PreDestroy
    public void destroy() throws NoSuchJobException {
        jobs.getJobNames().forEach(name -> System.out.println("job name: {}"+name));
        jobs.getJobInstances(JOB_NAME, 0, jobs.getJobInstanceCount(JOB_NAME)).forEach(
                jobInstance -> {
                    System.out.println("job instance id {}"+jobInstance.getInstanceId());
                }
        );

    }

    @Scheduled(fixedRate = 5000)
    public void run() throws Exception {
        JobExecution execution = jobLauncher.run(
                job(),
                new JobParametersBuilder().addLong("uniqueness", System.nanoTime()).toJobParameters()
        );
        System.out.println("Exit status: {}"+execution.getStatus());
    }

    @Bean
    public Step chunkStep() {
        Step step = stepBuilderFactory.get("taxis-load")
                .<User, User>chunk(chunkSize)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
        return step;
    }

    @StepScope
    @Bean
    public ItemWriter<User> writer() {
        return new UserDBWriter();
    }



    @StepScope
    @Bean
    public ItemProcessor<User, User> processor() {
        final CompositeItemProcessor<User, User> processor = new CompositeItemProcessor<>();
        processor.setDelegates(Arrays.asList(new DeptFilterProcessor()));
        return processor;
    }

    @Bean
    @StepScope
    public ItemReader<User> reader() {
        return new UserItemReader(CSV_FILE);
    }


}
