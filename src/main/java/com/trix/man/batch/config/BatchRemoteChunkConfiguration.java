package com.trix.man.batch.config;

import com.trix.man.batch.logic.DeptFilterProcessor;
import com.trix.man.batch.logic.JdbcItemReader;
import com.trix.man.batch.logic.UserDBWriter;
import com.trix.man.batch.model.User;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

import javax.sql.DataSource;
import java.util.Arrays;

@Configuration
public class BatchRemoteChunkConfiguration {

    @Configuration
    @Profile("!worker")
    public static class MasterConfiguration {

        @Autowired
        private JobBuilderFactory jobBuilderFactory;

        @Autowired
        private StepBuilderFactory stepBuilderFactory;

        @Autowired
        private DataSource dataSource;

        @Value("${taxis.batch.chunk.size}")
        private int chunkSize;

        @Bean
        public DirectChannel requests() {
            return new DirectChannel();
        }

        @Bean
        public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
            return IntegrationFlows.from("requests")
                    .handle(Amqp.outboundAdapter(amqpTemplate)
                            .routingKey("requests"))
                    .get();
        }

        @Bean
        public MessagingTemplate messagingTemplate() {
            MessagingTemplate template = new MessagingTemplate();
            template.setDefaultChannel(requests());
            template.setReceiveTimeout(2000);
            return template;
        }

        @Bean
        @StepScope
        public ChunkMessageChannelItemWriter<User> itemWriter() {
            ChunkMessageChannelItemWriter<User> chunkMessageChannelItemWriter =
                    new ChunkMessageChannelItemWriter<>();
            chunkMessageChannelItemWriter.setMessagingOperations(messagingTemplate());
            chunkMessageChannelItemWriter.setReplyChannel(replies());
            return chunkMessageChannelItemWriter;
        }

        @Bean
        public RemoteChunkHandlerFactoryBean<User> chunkHandler() {
            RemoteChunkHandlerFactoryBean<User> remoteChunkHandlerFactoryBean = new RemoteChunkHandlerFactoryBean<>();
            remoteChunkHandlerFactoryBean.setChunkWriter(itemWriter());
            remoteChunkHandlerFactoryBean.setStep(step1());
            return remoteChunkHandlerFactoryBean;
        }

        @Bean
        public QueueChannel replies() {
            return new QueueChannel();
        }

        @Bean
        public IntegrationFlow replyFlow(ConnectionFactory connectionFactory) {
            return IntegrationFlows
                    .from(Amqp.inboundAdapter(connectionFactory, "replies"))
                    .channel(replies())
                    .get();
        }

        @Bean
        @StepScope
        public JdbcItemReader reader() {
            return new JdbcItemReader(dataSource, chunkSize);
        }

        @Bean
        public TaskletStep step1() {
            return this.stepBuilderFactory.get("step1")
                    .<User, User>chunk(chunkSize)
                    .reader(reader())
                    .writer(itemWriter())
                    .build();
        }

        @Bean
        public Job remoteChunkingJob() {
            return this.jobBuilderFactory.get("remoteChunkingJob")
                    .start(step1())
                    .build();
        }
    }

    @Configuration
    @Profile("worker")
    public static class WorkerConfiguration {

        @Bean
        public Queue requestQueue() {
            return new Queue("requests", false);
        }

        @Bean
        public Queue repliesQueue() {
            return new Queue("replies", false);
        }

        @Bean
        public TopicExchange exchange() {
            return new TopicExchange("remote-chunking-exchange");
        }

        @Bean
        Binding repliesBinding(TopicExchange exchange) {
            return BindingBuilder.bind(repliesQueue()).to(exchange).with("replies");
        }

        @Bean
        Binding requestBinding(TopicExchange exchange) {
            return BindingBuilder.bind(requestQueue()).to(exchange).with("requests");
        }

        @Bean
        public DirectChannel requests() {
            return new DirectChannel();
        }

        @Bean
        public DirectChannel replies() {
            return new DirectChannel();
        }

        @Bean
        public IntegrationFlow mesagesIn(ConnectionFactory connectionFactory) {
            return IntegrationFlows
                    .from(Amqp.inboundAdapter(connectionFactory, "requests"))
                    .channel(requests())
                    .get();
        }

        @Bean
        public IntegrationFlow outgoingReplies(AmqpTemplate template) {
            return IntegrationFlows.from("replies")
                    .handle(Amqp.outboundAdapter(template)
                            .routingKey("replies"))
                    .get();
        }

        @Bean
        @ServiceActivator(inputChannel = "requests", outputChannel = "replies", sendTimeout = "10000")
        public ChunkProcessorChunkHandler<User> chunkProcessorChunkHandler() {
            ChunkProcessorChunkHandler<User> chunkProcessorChunkHandler = new ChunkProcessorChunkHandler<>();
            chunkProcessorChunkHandler.setChunkProcessor(
                    new SimpleChunkProcessor((user) -> processor(), writer()));

            return chunkProcessorChunkHandler;
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
        public ItemWriter<User> writer() {
            return new UserDBWriter();
        }


    }

}
