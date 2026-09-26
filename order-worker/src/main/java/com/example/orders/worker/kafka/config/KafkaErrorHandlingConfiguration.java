package com.example.orders.worker.kafka.config;

import com.example.orders.worker.exception.InvalidOrderEventException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlingConfiguration {

    @Bean
    public NewTopic ordersDltTopic(
            @Value("${app.kafka.orders-dlt-topic}") String dltTopic) {

        return TopicBuilder.name(dltTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${app.kafka.orders-dlt-topic}") String dltTopic) {

        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) ->
                        new TopicPartition(dltTopic, record.partition())
        );

        // 1 original attempt + 2 retries, with 1 second between retries
        var backOff = new FixedBackOff(1_000L, 2L);

        var errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Invalid input will not become valid by retrying it.
        errorHandler.addNotRetryableExceptions(InvalidOrderEventException.class);

        return errorHandler;
    }
}