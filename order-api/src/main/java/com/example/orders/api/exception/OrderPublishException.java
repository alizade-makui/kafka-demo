package com.example.orders.api.exception;

public class OrderPublishException extends RuntimeException {

    public OrderPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
