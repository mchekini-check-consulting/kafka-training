package com.training.ecommerce.common.event;

import java.time.Instant;

/**
 * Événement JSON (volontairement, pour comparer avec Avro pendant la formation) :
 * la commande a été expédiée.
 */
public record ShippingDispatched(
        String eventId,
        String orderId,
        String trackingNumber,
        Instant dispatchedAt) {
}
