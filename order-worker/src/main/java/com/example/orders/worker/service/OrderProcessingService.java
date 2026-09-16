package com.example.orders.worker.service;

import com.example.orders.contract.kafka.events.OrderCreatedEvent;
import com.example.orders.worker.model.entity.ProcessedOrder;
import com.example.orders.worker.model.record.response.ProcessedOrderResponse;
import org.springframework.data.domain.Page;
import java.util.Optional;
import java.util.UUID;

public interface OrderProcessingService {

    ProcessedOrder process(OrderCreatedEvent event);

    Optional<ProcessedOrderResponse> findById(UUID orderId);

    Page<ProcessedOrderResponse> findAll(int page);
}
