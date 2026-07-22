package com.training.ecommerce.payment.kafka;

import com.training.ecommerce.avro.StockReserved;
import com.training.ecommerce.common.Topics;
import com.training.ecommerce.payment.service.PaymentService;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PaymentListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentListener.class);

    private final PaymentService paymentService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public PaymentListener(PaymentService paymentService,
                           KafkaTemplate<Object, Object> kafkaTemplate) {
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Commit manuel (ack-mode: manual_immediate) : l'offset n'est commité
     * qu'après traitement complet. Si le service crashe avant l'ack,
     * l'événement sera relivré — c'est la déduplication en base qui évite
     * un double débit.
     */
    @KafkaListener(topics = Topics.INVENTORY_EVENTS)
    public void onInventoryEvent(ConsumerRecord<String, SpecificRecord> record, Acknowledgment ack) {
        switch (record.value()) {
            case StockReserved event -> paymentService.process(event)
                    .ifPresent(result -> {
                        kafkaTemplate.send(Topics.PAYMENT_EVENTS, event.getOrderId(), result);
                        log.info("Publié {} pour la commande {}",
                                result.getClass().getSimpleName(), event.getOrderId());
                    });
            default -> log.debug("Événement ignoré : {}", record.value().getClass().getSimpleName());
        }
        ack.acknowledge();
    }
}
