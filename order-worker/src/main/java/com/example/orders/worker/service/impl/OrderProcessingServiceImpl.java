package com.example.orders.worker.service.impl;

import com.example.orders.contract.kafka.events.OrderCreatedEvent;
import com.example.orders.worker.exception.InvalidOrderEventException;
import com.example.orders.worker.model.entity.ProcessedOrder;
import com.example.orders.worker.model.record.response.ProcessedOrderResponse;
import com.example.orders.worker.repository.ProcessedOrderRepository;
import com.example.orders.worker.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProcessingServiceImpl implements OrderProcessingService {

    private static final String PROCESSED_STATUS = "PROCESSED";
    private static final int PAGE_SIZE = 10;

    private final ProcessedOrderRepository repository;

    @Override
    @Transactional
    public ProcessedOrder process(OrderCreatedEvent event) {
        validate(event);

        Optional<ProcessedOrder> existingOrder = repository.findById(event.orderId());

        if (existingOrder.isPresent()) {
            ProcessedOrder order = existingOrder.get();

            log.info(
                    "Duplicate order event ignored. orderId={}, incomingEventId={}, existingEventId={}",
                    event.orderId(),
                    event.eventId(),
                    order.getEventId()
            );

            return order;
        }

        return createProcessedOrder(event);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<ProcessedOrderResponse> findById(UUID orderId) {
        return repository.findById(orderId)
                .map(this::toResponse);
    }

    @Override
    @Transactional(readOnly = true)
    public Page<ProcessedOrderResponse> findAll(int page) {
        return repository.findAll(PageRequest.of(page, PAGE_SIZE)).map(this::toResponse);
    }

    private ProcessedOrder createProcessedOrder(OrderCreatedEvent event) {
        BigDecimal total = event.unitPrice()
                .multiply(BigDecimal.valueOf(event.quantity()));

        ProcessedOrder order = new ProcessedOrder(
                event.orderId(),
                event.eventId(),
                PROCESSED_STATUS,
                total,
                Instant.now()
        );

        return repository.save(order);
    }

    private ProcessedOrderResponse toResponse(ProcessedOrder order) {
        return new ProcessedOrderResponse(
                order.getOrderId(),
                order.getEventId(),
                order.getStatus(),
                order.getTotalAmount(),
                order.getProcessedAt()
        );
    }

    private void validate(OrderCreatedEvent event) {
        if (event == null
                || event.schemaVersion() != 1
                || event.orderId() == null
                || event.eventId() == null
                || event.occurredAt() == null
                || event.customerId() == null
                || event.customerId().isBlank()
                || event.productCode() == null
                || event.productCode().isBlank()
                || event.quantity() < 1
                || event.quantity() > 1000
                || event.unitPrice() == null
                || event.unitPrice().signum() <= 0) {

            throw new InvalidOrderEventException(
                    "Invalid order event or unsupported schema version"
            );
        }
    }
}