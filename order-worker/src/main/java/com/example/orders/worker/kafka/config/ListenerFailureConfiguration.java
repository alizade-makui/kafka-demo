package com.example.orders.worker.kafka.config;

import org.springframework.context.annotation.*;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;

@Configuration
public class ListenerFailureConfiguration {

    @Bean
    CommonContainerStoppingErrorHandler kafkaErrorHandler() {
        return new CommonContainerStoppingErrorHandler();
    }
}
