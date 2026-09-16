package com.example.orders.api.controller;

import com.example.orders.api.model.request.CreateOrderRequest;
import com.example.orders.api.model.response.AcceptedOrderResponse;
import com.example.orders.api.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<AcceptedOrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {

        AcceptedOrderResponse response = orderService.create(request);

        return ResponseEntity.accepted().body(response);
    }
}
