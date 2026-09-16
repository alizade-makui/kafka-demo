package com.example.orders.worker.controller;

import com.example.orders.worker.model.record.response.ProcessedOrderResponse;
import com.example.orders.worker.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.UUID;

@RestController
@RequestMapping("/api/processed-orders")
@RequiredArgsConstructor
public class OrderQueryController {

    private final OrderProcessingService orderProcessingService;

    @GetMapping
    public ResponseEntity<Page<ProcessedOrderResponse>> getAll(@RequestParam(defaultValue = "0") int page) {
        return ResponseEntity.ok(orderProcessingService.findAll(page));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<ProcessedOrderResponse> find(@PathVariable UUID orderId) {
        return ResponseEntity.of(orderProcessingService.findById(orderId));
    }
}
