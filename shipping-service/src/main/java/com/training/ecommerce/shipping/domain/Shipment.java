package com.training.ecommerce.shipping.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "shipments")
public class Shipment {

    public enum Status {DISPATCHED, DELIVERED}

    @Id
    private String id;
    private String orderId;
    private String trackingNumber;

    @Enumerated(EnumType.STRING)
    private Status status;
    private Instant dispatchedAt;
    private Instant deliveredAt;

    protected Shipment() {
    }

    public Shipment(String id, String orderId, String trackingNumber) {
        this.id = id;
        this.orderId = orderId;
        this.trackingNumber = trackingNumber;
        this.status = Status.DISPATCHED;
        this.dispatchedAt = Instant.now();
    }

    public void markDelivered() {
        this.status = Status.DELIVERED;
        this.deliveredAt = Instant.now();
    }

    public String getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }

    public Status getStatus() {
        return status;
    }

    public Instant getDispatchedAt() {
        return dispatchedAt;
    }

    public Instant getDeliveredAt() {
        return deliveredAt;
    }
}
