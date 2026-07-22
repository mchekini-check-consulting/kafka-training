package com.training.ecommerce.common;

/**
 * Noms des topics Kafka de la plateforme.
 * Clé de partitionnement : orderId pour tous les topics métier,
 * afin de garantir l'ordre des événements d'une même commande.
 */
public final class Topics {

    public static final String ORDER_EVENTS = "order.events";
    public static final String INVENTORY_EVENTS = "inventory.events";
    public static final String PAYMENT_EVENTS = "payment.events";
    public static final String SHIPPING_EVENTS = "shipping.events";

    public static final String NOTIFICATION_DLT = "notification.dlt";

    private Topics() {
    }
}
