package com.example.orders.api.service.impl;

import com.example.orders.api.kafka.producer.OrderPublisher;
import com.example.orders.api.model.request.CreateOrderRequest;
import com.example.orders.api.model.response.AcceptedOrderResponse;
import com.example.orders.api.service.OrderService;
import com.example.orders.contract.kafka.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderPublisher publisher;

    @Override
    public AcceptedOrderResponse create(CreateOrderRequest request) {

        OrderCreatedEvent event = new OrderCreatedEvent(UUID.randomUUID(), 1, Instant.now(), UUID.randomUUID(),
                request.customerId(), request.productCode(), request.quantity(), request.unitPrice());

        SendResult<String, OrderCreatedEvent> result = publisher.publish(event);

        return new AcceptedOrderResponse(
                event.orderId(), event.eventId(), "ACCEPTED",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }
}
