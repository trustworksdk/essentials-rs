package com.acme.orders;

import java.math.BigDecimal;
import java.time.Instant;

/** The normal-message payload: {@code SingleValueType}s, a {@code BigDecimal} and an {@code Instant}. */
public class OrderPlaced {
    public OrderId    orderId;
    public CustomerId customerId;
    public BigDecimal amount;
    public Instant    placedAt;

    private OrderPlaced() {
    }

    public OrderPlaced(OrderId orderId, CustomerId customerId, BigDecimal amount, Instant placedAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.placedAt = placedAt;
    }
}
