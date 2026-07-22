package com.training.ecommerce.order.domain;

/**
 * Cycle de vie d'une commande, piloté par les événements Kafka de la saga :
 * CREATED → STOCK_RESERVED → PAID → SHIPPED → DELIVERED
 *        └→ REJECTED       └→ PAYMENT_FAILED
 */
public enum OrderStatus {
    CREATED,
    STOCK_RESERVED,
    REJECTED,
    PAID,
    PAYMENT_FAILED,
    SHIPPED,
    DELIVERED
}
