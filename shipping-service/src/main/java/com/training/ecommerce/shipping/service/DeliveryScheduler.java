package com.training.ecommerce.shipping.service;

import com.training.ecommerce.common.Topics;
import com.training.ecommerce.common.event.ShippingDelivered;
import com.training.ecommerce.shipping.domain.Shipment;
import com.training.ecommerce.shipping.repository.ShipmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Simule le transporteur : un colis expédié depuis plus de
 * shipping.delivery-delay-seconds est marqué livré.
 */
@Component
public class DeliveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(DeliveryScheduler.class);

    private final ShipmentRepository shipments;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final int deliveryDelaySeconds;

    public DeliveryScheduler(ShipmentRepository shipments,
                             KafkaTemplate<Object, Object> kafkaTemplate,
                             @Value("${shipping.delivery-delay-seconds}") int deliveryDelaySeconds) {
        this.shipments = shipments;
        this.kafkaTemplate = kafkaTemplate;
        this.deliveryDelaySeconds = deliveryDelaySeconds;
    }

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void deliverPendingShipments() {
        Instant threshold = Instant.now().minusSeconds(deliveryDelaySeconds);
        for (Shipment shipment : shipments.findByStatusAndDispatchedAtBefore(
                Shipment.Status.DISPATCHED, threshold)) {
            shipment.markDelivered();
            kafkaTemplate.send(Topics.SHIPPING_EVENTS, shipment.getOrderId(),
                    new ShippingDelivered(UUID.randomUUID().toString(), shipment.getOrderId(),
                            shipment.getTrackingNumber(), Instant.now()));
            log.info("Commande {} livrée (suivi {})", shipment.getOrderId(), shipment.getTrackingNumber());
        }
    }
}
