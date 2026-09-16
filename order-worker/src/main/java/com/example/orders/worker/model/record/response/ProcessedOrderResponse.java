package com.example.orders.worker.model.record.response;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ProcessedOrderResponse(UUID orderId, UUID eventId, String status, BigDecimal totalAmount, Instant processedAt) {
}
