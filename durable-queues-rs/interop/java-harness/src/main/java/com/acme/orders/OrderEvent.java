package com.acme.orders;

/** Nested classes, so {@code message_payload_type} carries a {@code $} (CONTRACT.md §2). */
public class OrderEvent {
    public OrderId orderId;

    private OrderEvent() {
    }

    protected OrderEvent(OrderId orderId) {
        this.orderId = orderId;
    }

    /** The ordered-message payload. */
    public static class OrderAdded extends OrderEvent {
        public long step;

        private OrderAdded() {
        }

        public OrderAdded(OrderId orderId, long step) {
            super(orderId);
            this.step = step;
        }
    }

    public static class OrderCancelled extends OrderEvent {
        public String reason;

        private OrderCancelled() {
        }

        public OrderCancelled(OrderId orderId, String reason) {
            super(orderId);
            this.reason = reason;
        }
    }
}
