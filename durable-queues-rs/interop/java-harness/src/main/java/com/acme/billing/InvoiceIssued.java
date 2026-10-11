package com.acme.billing;

public class InvoiceIssued {
    public String invoiceId;

    private InvoiceIssued() {
    }

    public InvoiceIssued(String invoiceId) {
        this.invoiceId = invoiceId;
    }
}
