package com.training.ecommerce.payment.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Déduplication applicative — voir le commentaire dans inventory-service :
 * un paiement ne doit JAMAIS être traité deux fois, même si Kafka relivre
 * l'événement (at-least-once).
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    private String eventId;
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId) {
        this.eventId = eventId;
        this.processedAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }
}
