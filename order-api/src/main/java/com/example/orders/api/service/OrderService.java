package com.example.orders.api.service;

import com.example.orders.api.model.request.CreateOrderRequest;
import com.example.orders.api.model.response.AcceptedOrderResponse;

public interface OrderService {
    AcceptedOrderResponse create(CreateOrderRequest request);
}
