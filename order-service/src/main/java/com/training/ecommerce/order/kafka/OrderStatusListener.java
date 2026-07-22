package com.training.ecommerce.order.kafka;

import com.training.ecommerce.avro.PaymentFailed;
import com.training.ecommerce.avro.PaymentSucceeded;
import com.training.ecommerce.avro.StockRejected;
import com.training.ecommerce.avro.StockReserved;
import com.training.ecommerce.common.Topics;
import com.training.ecommerce.common.event.ShippingDelivered;
import com.training.ecommerce.common.event.ShippingDispatched;
import com.training.ecommerce.order.domain.OrderStatus;
import com.training.ecommerce.order.repository.OrderRepository;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Fait avancer la state machine de la commande au fil des événements de la saga.
 */
@Component
public class OrderStatusListener {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusListener.class);

    private final OrderRepository orders;

    public OrderStatusListener(OrderRepository orders) {
        this.orders = orders;
    }

    @KafkaListener(topics = {Topics.INVENTORY_EVENTS, Topics.PAYMENT_EVENTS})
    public void onAvroEvent(ConsumerRecord<String, SpecificRecord> record) {
        switch (record.value()) {
            case StockReserved e -> updateStatus(e.getOrderId(), OrderStatus.STOCK_RESERVED);
            case StockRejected e -> updateStatus(e.getOrderId(), OrderStatus.REJECTED);
            case PaymentSucceeded e -> updateStatus(e.getOrderId(), OrderStatus.PAID);
            case PaymentFailed e -> updateStatus(e.getOrderId(), OrderStatus.PAYMENT_FAILED);
            default -> log.debug("Événement ignoré : {}", record.value().getClass().getSimpleName());
        }
    }

    @KafkaListener(topics = Topics.SHIPPING_EVENTS, containerFactory = "jsonListenerFactory")
    public void onShippingEvent(ConsumerRecord<String, Object> record) {
        switch (record.value()) {
            case ShippingDispatched e -> updateStatus(e.orderId(), OrderStatus.SHIPPED);
            case ShippingDelivered e -> updateStatus(e.orderId(), OrderStatus.DELIVERED);
            default -> log.debug("Événement ignoré : {}", record.value().getClass().getSimpleName());
        }
    }

    @Transactional
    void updateStatus(String orderId, OrderStatus status) {
        orders.findById(orderId).ifPresentOrElse(order -> {
            order.updateStatus(status);
            orders.save(order);
            log.info("Commande {} → {}", orderId, status);
        }, () -> log.warn("Commande inconnue : {}", orderId));
    }
}
