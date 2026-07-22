package com.training.ecommerce.inventory.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Déduplication applicative : Kafka garantit "at-least-once", un même événement
 * peut donc être livré plusieurs fois (rebalancing, retry producteur...).
 * On mémorise les eventId déjà traités dans la même transaction que l'effet métier,
 * ce qui donne un "exactly-once" applicatif.
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

    public Instant getProcessedAt() {
        return processedAt;
    }
}
