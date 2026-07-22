package com.training.ecommerce.payment.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "payments")
public class Payment {

    public enum Status {SUCCEEDED, FAILED}

    @Id
    private String id;
    private String orderId;
    private double amount;

    @Enumerated(EnumType.STRING)
    private Status status;
    private Instant processedAt;

    protected Payment() {
    }

    public Payment(String id, String orderId, double amount, Status status) {
        this.id = id;
        this.orderId = orderId;
        this.amount = amount;
        this.status = status;
        this.processedAt = Instant.now();
    }

    public String getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }

    public double getAmount() {
        return amount;
    }

    public Status getStatus() {
        return status;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
