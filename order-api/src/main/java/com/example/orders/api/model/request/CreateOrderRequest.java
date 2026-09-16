package com.example.orders.api.model.request;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank @Size(max = 100) String customerId,
        @NotBlank @Size(max = 100) String productCode,
        @Min(1) @Max(1000) int quantity,
        @NotNull @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal unitPrice) {
}
