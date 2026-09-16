package com.example.orders.worker.service.impl;

import com.example.orders.contract.kafka.events.OrderCreatedEvent;
import com.example.orders.worker.exception.InvalidOrderEventException;
import com.example.orders.worker.model.entity.ProcessedOrder;
import com.example.orders.worker.model.record.response.ProcessedOrderResponse;
import com.example.orders.worker.repository.ProcessedOrderRepository;
import com.example.orders.worker.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderProcessingServiceImpl implements OrderProcessingService {

    private final ProcessedOrderRepository repository;

    @Override
    public ProcessedOrder process(OrderCreatedEvent event) {
        validate(event);
        BigDecimal total = event.unitPrice().multiply(BigDecimal.valueOf(event.quantity()));
        ProcessedOrder order = new ProcessedOrder(event.orderId(), event.eventId(), "PROCESSED", total, Instant.now());
        return repository.findById(event.orderId()).orElseGet(() -> repository.save(order));
    }

    @Override
    public Optional<ProcessedOrderResponse> findById(UUID orderId) {
        return repository.findById(orderId).map(this::toResponse);
    }

    @Override
    public Page<ProcessedOrderResponse> findAll(int page) {
        return repository.findAll(PageRequest.of(page, 10)).map(this::toResponse);
    }

    private ProcessedOrderResponse toResponse(ProcessedOrder order) {
        return new ProcessedOrderResponse(
                order.getOrderId(),
                order.getEventId(),
                order.getStatus(),
                order.getTotalAmount(),
                order.getProcessedAt());
    }

    private void validate(OrderCreatedEvent event) {
        if (event == null || event.schemaVersion() != 1 || event.orderId() == null || event.eventId() == null
                || event.occurredAt() == null || event.customerId() == null || event.customerId().isBlank()
                || event.productCode() == null || event.productCode().isBlank()
                || event.quantity() < 1 || event.quantity() > 1000
                || event.unitPrice() == null || event.unitPrice().signum() <= 0) {
            throw new InvalidOrderEventException("Invalid order event or unsupported schema version");
        }
    }
}
