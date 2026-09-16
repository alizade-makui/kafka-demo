package com.example.orders.contract.kafka.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        int schemaVersion,
        Instant occurredAt,
        UUID orderId,
        String customerId,
        String productCode,
        int quantity,
        BigDecimal unitPrice) {
}
