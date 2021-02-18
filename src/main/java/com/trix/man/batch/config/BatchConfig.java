package com.trix.man.batch.config;

import com.trix.man.batch.logic.DeptFilterProcessor;
import com.trix.man.batch.logic.JdbcItemReader;
import com.trix.man.batch.logic.UserDBWriter;
import com.trix.man.batch.model.User;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Set;

@Configuration
public class BatchConfig {

    private static final String CSV_FILE = "/Users/chakkaru/Desktop/GCP/batch/batch/src/main/resources/user.csv";

    @Value("${taxis.batch.chunk.size}")
    private int chunkSize;

    @Value("${taxis.batch.thread.limit}")
    private int threadSize;

    @Value("${taxis.batch.thread.core.pool.size}")
    private int corePoolSize;

    @Value("${taxis.batch.thread.max.pool.size}")
    private int maxPoolSize;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private static final String JOB_NAME = "taxis-batch";

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    private JobExplorer jobs;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private DataSource dataSource;

    @Bean
    public Job job(){
        return jobBuilderFactory.get(JOB_NAME)
            .incrementer(new RunIdIncrementer())
                .start(chunkStep(taskExecutor()))
                .build();

    }

    @Bean
    public TaskExecutor simpleTaskExecutor(){
        return new SimpleAsyncTaskExecutor("taxis_batch");
    }


    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
        pool.setCorePoolSize(corePoolSize);
        pool.setMaxPoolSize(maxPoolSize);
        pool.setWaitForTasksToCompleteOnShutdown(false);
        return pool;
    }

    @PreDestroy
    public void destroy() throws NoSuchJobException, NoSuchJobExecutionException, JobExecutionNotRunningException {
        /**Graceful shutdown**/
        jobs.getJobNames().forEach(name -> System.out.println("job name: {}"+name));
        Set<Long> executions = jobOperator.getRunningExecutions(JOB_NAME);
        jobOperator.stop(executions.iterator().next());
    }

    @Scheduled(fixedRate = 500000)
    public void run() throws Exception {
        JobExecution execution = jobLauncher.run(
                job(),
                new JobParametersBuilder().addLong("uniqueness", System.nanoTime()).toJobParameters()
        );
        System.out.println("Exit status: {}"+execution.getStatus());
    }

    @Bean
    public Step chunkStep(TaskExecutor taskExecutor) {
        Step step = stepBuilderFactory.get("taxis-load")
                .<User, User>chunk(chunkSize)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .listener(itemWriteListener())
                .taskExecutor(taskExecutor)
                .throttleLimit(threadSize)
                .build();
        return step;
    }

    @StepScope
    @Bean
    public ItemWriter<User> writer() {
        return new UserDBWriter();
    }


    @Bean
    public ItemWriteListener<User> itemWriteListener(){
        return new UserItemWriteListener();
    }


    @StepScope
    @Bean
    public ItemProcessor<User, User> processor() {
        final CompositeItemProcessor<User, User> processor = new CompositeItemProcessor<>();
        processor.setDelegates(Arrays.asList(new DeptFilterProcessor()));
        return processor;
    }

//    @Bean
//    @StepScope
//    public ItemReader<User> reader() {
//        return new UserItemReader(CSV_FILE);
//    }

    @Bean
    @StepScope
    public JdbcItemReader reader() {
        return new JdbcItemReader(dataSource, chunkSize);
    }

}
