package com.training.ecommerce.analytics.kafka;

import com.training.ecommerce.analytics.repository.MetricRepository;
import com.training.ecommerce.avro.OrderCreated;
import com.training.ecommerce.avro.PaymentFailed;
import com.training.ecommerce.avro.PaymentSucceeded;
import com.training.ecommerce.avro.StockRejected;
import com.training.ecommerce.common.Topics;
import com.training.ecommerce.common.event.ShippingDelivered;
import com.training.ecommerce.common.event.ShippingDispatched;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

/**
 * Consommateur pur : agrège tous les événements de la plateforme en métriques.
 *
 * Étend AbstractConsumerSeekAware pour la démo "replay" : repositionner les
 * offsets au début des topics et reconstruire les agrégats — l'un des grands
 * avantages de Kafka sur un broker classique (le log est conservé).
 */
@Component
public class AnalyticsListener extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsListener.class);

    private final MetricRepository metrics;

    public AnalyticsListener(MetricRepository metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(topics = {Topics.ORDER_EVENTS, Topics.INVENTORY_EVENTS, Topics.PAYMENT_EVENTS})
    public void onAvroEvent(ConsumerRecord<String, SpecificRecord> record) {
        switch (record.value()) {
            case OrderCreated e -> {
                metrics.increment("orders.created", 1);
                metrics.increment("orders.pending.amount", e.getTotalAmount());
            }
            case StockRejected e -> metrics.increment("orders.rejected", 1);
            case PaymentSucceeded e -> {
                metrics.increment("payments.succeeded", 1);
                metrics.increment("revenue", e.getAmount());
            }
            case PaymentFailed e -> metrics.increment("payments.failed", 1);
            default -> log.trace("Événement non agrégé : {}", record.value().getClass().getSimpleName());
        }
    }

    @KafkaListener(topics = Topics.SHIPPING_EVENTS, containerFactory = "jsonListenerFactory")
    public void onShippingEvent(ConsumerRecord<String, Object> record) {
        switch (record.value()) {
            case ShippingDispatched e -> metrics.increment("shipments.dispatched", 1);
            case ShippingDelivered e -> metrics.increment("shipments.delivered", 1);
            default -> log.trace("Événement non agrégé : {}", record.value().getClass().getSimpleName());
        }
    }

    /**
     * Repart du début de tous les topics assignés. Les seeks demandés depuis un
     * autre thread sont appliqués au prochain poll() de chaque consumer.
     */
    public void replayFromBeginning() {
        metrics.deleteAllInBatch();
        seekToBeginning();
        log.info("Replay demandé : offsets repositionnés au début, métriques remises à zéro");
    }
}
