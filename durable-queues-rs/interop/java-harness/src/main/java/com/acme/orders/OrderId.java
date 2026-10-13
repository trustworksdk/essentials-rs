package com.acme.orders;

import dk.trustworks.essentials.types.CharSequenceType;

/** A {@code SingleValueType}: Jackson writes it as a bare string (CONTRACT.md §12, hazard 2). */
public class OrderId extends CharSequenceType<OrderId> {
    protected OrderId(CharSequence value) {
        super(value);
    }

    public static OrderId of(CharSequence value) {
        return new OrderId(value);
    }
}
