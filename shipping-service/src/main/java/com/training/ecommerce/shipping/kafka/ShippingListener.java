package com.training.ecommerce.shipping.kafka;

import com.training.ecommerce.avro.PaymentSucceeded;
import com.training.ecommerce.common.Topics;
import com.training.ecommerce.common.event.ShippingDispatched;
import com.training.ecommerce.shipping.domain.Shipment;
import com.training.ecommerce.shipping.repository.ShipmentRepository;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class ShippingListener {

    private static final Logger log = LoggerFactory.getLogger(ShippingListener.class);

    private final ShipmentRepository shipments;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public ShippingListener(ShipmentRepository shipments,
                            KafkaTemplate<Object, Object> kafkaTemplate) {
        this.shipments = shipments;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onPaymentEvent(ConsumerRecord<String, SpecificRecord> record) {
        switch (record.value()) {
            case PaymentSucceeded event -> dispatch(event);
            default -> log.debug("Événement ignoré : {}", record.value().getClass().getSimpleName());
        }
    }

    private void dispatch(PaymentSucceeded event) {
        // Idempotence par contrainte métier : une seule expédition par commande
        if (shipments.findByOrderId(event.getOrderId()).isPresent()) {
            log.warn("Expédition déjà créée pour la commande {}, événement ignoré", event.getOrderId());
            return;
        }
        String trackingNumber = "TRK-" + ThreadLocalRandom.current().nextInt(100_000, 999_999);
        Shipment shipment = shipments.save(
                new Shipment(UUID.randomUUID().toString(), event.getOrderId(), trackingNumber));

        kafkaTemplate.send(Topics.SHIPPING_EVENTS, event.getOrderId(),
                new ShippingDispatched(UUID.randomUUID().toString(), event.getOrderId(),
                        trackingNumber, Instant.now()));
        log.info("Commande {} expédiée, suivi {}", event.getOrderId(), shipment.getTrackingNumber());
    }
}
