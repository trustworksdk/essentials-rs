package com.acme.orders;

import dk.trustworks.essentials.types.CharSequenceType;

public class CustomerId extends CharSequenceType<CustomerId> {
    protected CustomerId(CharSequence value) {
        super(value);
    }

    public static CustomerId of(CharSequence value) {
        return new CustomerId(value);
    }
}
