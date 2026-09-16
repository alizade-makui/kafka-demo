package com.example.orders.worker.kafka.consumer;

import com.example.orders.contract.kafka.events.OrderCreatedEvent;
import com.example.orders.worker.model.entity.ProcessedOrder;
import com.example.orders.worker.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);
    private final OrderProcessingService processor;

    @KafkaListener(topics = "${app.kafka.orders-topic}")
    public void onOrder(ConsumerRecord<String, OrderCreatedEvent> record) {

        ProcessedOrder result = processor.process(record.value());

        log.info("orderId={} status={} total={} partition={} offset={}",
                result.getOrderId(), result.getStatus(), result.getTotalAmount(), record.partition(), record.offset());
    }
}
