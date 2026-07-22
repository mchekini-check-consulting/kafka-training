package com.training.ecommerce.common.event;

import java.time.Instant;

/**
 * Événement JSON : la commande a été livrée au client.
 */
public record ShippingDelivered(
        String eventId,
        String orderId,
        String trackingNumber,
        Instant deliveredAt) {
}
