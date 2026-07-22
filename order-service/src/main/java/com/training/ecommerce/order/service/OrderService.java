package com.training.ecommerce.order.service;

import com.training.ecommerce.avro.OrderCreated;
import com.training.ecommerce.avro.OrderItem;
import com.training.ecommerce.common.Topics;
import com.training.ecommerce.order.api.CreateOrderRequest;
import com.training.ecommerce.order.client.InventoryClient;
import com.training.ecommerce.order.domain.Order;
import com.training.ecommerce.order.domain.OrderLine;
import com.training.ecommerce.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final InventoryClient inventoryClient;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OrderService(OrderRepository orders,
                        InventoryClient inventoryClient,
                        KafkaTemplate<Object, Object> kafkaTemplate) {
        this.orders = orders;
        this.inventoryClient = inventoryClient;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Order createOrder(CreateOrderRequest request) {
        // Validation synchrone des produits et des prix via REST
        List<OrderLine> lines = new ArrayList<>();
        List<OrderItem> avroItems = new ArrayList<>();
        double total = 0;
        for (var item : request.items()) {
            var product = inventoryClient.getProduct(item.productId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.BAD_REQUEST,
                            "Produit inconnu : " + item.productId()));
            lines.add(new OrderLine(product.id(), item.quantity(), product.price()));
            avroItems.add(OrderItem.newBuilder()
                    .setProductId(product.id())
                    .setQuantity(item.quantity())
                    .setUnitPrice(product.price())
                    .build());
            total += product.price() * item.quantity();
        }

        String orderId = UUID.randomUUID().toString();
        Order order = orders.save(new Order(orderId, request.customerId(),
                request.customerEmail(), lines, total));

        OrderCreated event = OrderCreated.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setCustomerId(request.customerId())
                .setCustomerEmail(request.customerEmail())
                .setItems(avroItems)
                .setTotalAmount(total)
                .setCreatedAt(Instant.now())
                .build();

        // Clé = orderId : tous les événements d'une commande vont sur la même
        // partition, donc sont consommés dans l'ordre.
        kafkaTemplate.send(Topics.ORDER_EVENTS, orderId, event);
        log.info("Commande {} créée ({} €), OrderCreated publié", orderId, total);
        return order;
    }
}
