package com.training.ecommerce.notification.kafka;

import com.training.ecommerce.avro.OrderCreated;
import com.training.ecommerce.avro.PaymentFailed;
import com.training.ecommerce.avro.PaymentSucceeded;
import com.training.ecommerce.avro.StockRejected;
import com.training.ecommerce.common.Topics;
import com.training.ecommerce.common.event.ShippingDelivered;
import com.training.ecommerce.common.event.ShippingDispatched;
import com.training.ecommerce.notification.service.NotificationSender;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    private final NotificationSender sender;

    public NotificationListener(NotificationSender sender) {
        this.sender = sender;
    }

    @KafkaListener(topics = {Topics.ORDER_EVENTS, Topics.PAYMENT_EVENTS})
    public void onAvroEvent(ConsumerRecord<String, SpecificRecord> record) {
        switch (record.value()) {
            case OrderCreated e -> {
                // Démo "poison pill" : un email contenant "poison" fait échouer le
                // traitement → 3 tentatives puis Dead Letter Topic.
                if (e.getCustomerEmail().contains("poison")) {
                    throw new IllegalStateException(
                            "Adresse email invalide : " + e.getCustomerEmail());
                }
                sender.send(e.getCustomerEmail(), "Commande reçue",
                        "Votre commande " + e.getOrderId() + " de " + e.getTotalAmount()
                        + " € est en cours de traitement");
            }
            case StockRejected e -> sender.send("client-" + e.getOrderId(), "Commande annulée",
                    "Commande " + e.getOrderId() + " : " + e.getReason());
            case PaymentSucceeded e -> sender.send("client-" + e.getOrderId(), "Paiement accepté",
                    "Paiement de " + e.getAmount() + " € accepté pour la commande " + e.getOrderId());
            case PaymentFailed e -> sender.send("client-" + e.getOrderId(), "Paiement refusé",
                    "Commande " + e.getOrderId() + " : " + e.getReason());
            default -> log.debug("Événement ignoré : {}", record.value().getClass().getSimpleName());
        }
    }

    @KafkaListener(topics = Topics.SHIPPING_EVENTS, containerFactory = "jsonListenerFactory")
    public void onShippingEvent(ConsumerRecord<String, Object> record) {
        switch (record.value()) {
            case ShippingDispatched e -> sender.send("client-" + e.orderId(), "Commande expédiée",
                    "Commande " + e.orderId() + " expédiée, numéro de suivi " + e.trackingNumber());
            case ShippingDelivered e -> sender.send("client-" + e.orderId(), "Commande livrée",
                    "Commande " + e.orderId() + " livrée !");
            default -> log.debug("Événement ignoré : {}", record.value().getClass().getSimpleName());
        }
    }
}
