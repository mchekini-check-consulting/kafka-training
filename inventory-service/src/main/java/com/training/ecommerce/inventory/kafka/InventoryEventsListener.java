package com.training.ecommerce.inventory.kafka;

import com.training.ecommerce.avro.OrderCreated;
import com.training.ecommerce.avro.PaymentFailed;
import com.training.ecommerce.common.Topics;
import com.training.ecommerce.inventory.service.InventoryService;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventsListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventsListener.class);

    private final InventoryService inventoryService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public InventoryEventsListener(InventoryService inventoryService,
                                   KafkaTemplate<Object, Object> kafkaTemplate) {
        this.inventoryService = inventoryService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS)
    public void onOrderEvent(ConsumerRecord<String, SpecificRecord> record) {
        // Pattern matching Java 21 : dispatch par type d'événement Avro
        switch (record.value()) {
            case OrderCreated event -> inventoryService.reserve(event)
                    .ifPresent(result -> publish(event.getOrderId(), result));
            default -> log.debug("Événement ignoré sur {} : {}", record.topic(),
                    record.value().getClass().getSimpleName());
        }
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onPaymentEvent(ConsumerRecord<String, SpecificRecord> record) {
        switch (record.value()) {
            case PaymentFailed event -> inventoryService.release(event)
                    .ifPresent(result -> publish(event.getOrderId(), result));
            default -> log.debug("Événement ignoré sur {} : {}", record.topic(),
                    record.value().getClass().getSimpleName());
        }
    }

    /**
     * Publication après commit de la transaction métier.
     * Note formation : entre le commit DB et l'envoi Kafka, un crash perdrait
     * l'événement — le pattern "transactional outbox" résout ce problème.
     */
    private void publish(String orderId, SpecificRecord event) {
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, orderId, event);
        log.info("Publié {} pour la commande {}", event.getClass().getSimpleName(), orderId);
    }
}
