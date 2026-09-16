package com.example.orders.api.kafka.producer;

import com.example.orders.api.exception.OrderPublishException;
import com.example.orders.contract.kafka.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class OrderPublisher {

    private final KafkaTemplate<String, OrderCreatedEvent> kafka;
    private final String topic;

    public OrderPublisher(KafkaTemplate<String, OrderCreatedEvent> kafka, @Value("${app.kafka.orders-topic}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    public SendResult<String, OrderCreatedEvent> publish(OrderCreatedEvent event) {

        try {
            return kafka.send(topic, event.orderId().toString(), event).get(20, TimeUnit.SECONDS);

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new OrderPublishException("Interrupted while waiting for Kafka", exception);

        } catch (ExecutionException | TimeoutException exception) {
            throw new OrderPublishException("Kafka did not confirm the send", exception);

        } catch (org.springframework.kafka.KafkaException | org.apache.kafka.common.KafkaException exception) {
            throw new OrderPublishException("Kafka send failed", exception);
        }
    }
}
