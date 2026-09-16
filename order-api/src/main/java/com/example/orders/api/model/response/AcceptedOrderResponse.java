package com.example.orders.api.model.response;

import java.util.UUID;

public record AcceptedOrderResponse(UUID orderId, UUID eventId, String status, int partition, long offset) {
}
