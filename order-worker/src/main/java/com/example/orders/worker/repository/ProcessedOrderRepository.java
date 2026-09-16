package com.example.orders.worker.repository;

import com.example.orders.worker.model.entity.ProcessedOrder;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, UUID> {
}
