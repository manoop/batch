package com.trix.man.batch.config;

import com.trix.man.batch.logic.DeptFilterProcessor;
import com.trix.man.batch.logic.UserDBWriter;
import com.trix.man.batch.model.User;
import com.trix.man.batch.repository.UserRepository;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.util.*;

@Configuration
public class BatchConfig {


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
    JobOperator jobOperator;

    @Autowired
    private JobExplorer jobs;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private DataSource dataSource;

    @Autowired private UserRepository userRepository;

    @Autowired PlatformTransactionManager jpaTransactionManager;

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
                .transactionManager(jpaTransactionManager)
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
        return new UserDBWriter(userRepository);
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


    @Bean
    @StepScope
    public RepositoryItemReader<User> reader() {
        Map<String, Sort.Direction> sorts = new HashMap<>();
        sorts.put("id", Sort.Direction.DESC);

        List<Object> methodArgs = new ArrayList<>();
        methodArgs.add("NEW");
        RepositoryItemReader itemReader = new RepositoryItemReaderBuilder()
                .repository(userRepository)
                .methodName("findByTempstatus")
                .arguments(methodArgs)
                .pageSize(10)
                .sorts(sorts)
                .saveState(false)
                .build();
        return itemReader;
    }

}
