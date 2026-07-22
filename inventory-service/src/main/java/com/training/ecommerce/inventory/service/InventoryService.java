package com.training.ecommerce.inventory.service;

import com.training.ecommerce.avro.OrderCreated;
import com.training.ecommerce.avro.PaymentFailed;
import com.training.ecommerce.avro.StockRejected;
import com.training.ecommerce.avro.StockReleased;
import com.training.ecommerce.avro.StockReserved;
import com.training.ecommerce.inventory.domain.ProcessedEvent;
import com.training.ecommerce.inventory.domain.Product;
import com.training.ecommerce.inventory.domain.Reservation;
import com.training.ecommerce.inventory.repository.ProcessedEventRepository;
import com.training.ecommerce.inventory.repository.ProductRepository;
import com.training.ecommerce.inventory.repository.ReservationRepository;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final ProductRepository products;
    private final ReservationRepository reservations;
    private final ProcessedEventRepository processedEvents;

    public InventoryService(ProductRepository products,
                            ReservationRepository reservations,
                            ProcessedEventRepository processedEvents) {
        this.products = products;
        this.reservations = reservations;
        this.processedEvents = processedEvents;
    }

    /**
     * Réserve le stock d'une commande. Retourne l'événement à publier
     * (StockReserved ou StockRejected), ou empty si l'événement est un duplicat.
     */
    @Transactional
    public Optional<SpecificRecord> reserve(OrderCreated event) {
        if (isDuplicate(event.getEventId())) {
            return Optional.empty();
        }

        for (var item : event.getItems()) {
            Product product = products.findByIdForUpdate(item.getProductId()).orElse(null);
            if (product == null || product.getStock() < item.getQuantity()) {
                String reason = product == null
                        ? "Produit inconnu : " + item.getProductId()
                        : "Stock insuffisant pour " + product.getName()
                          + " (demandé : " + item.getQuantity() + ", disponible : " + product.getStock() + ")";
                log.warn("Commande {} rejetée : {}", event.getOrderId(), reason);
                return Optional.of(StockRejected.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setOrderId(event.getOrderId())
                        .setReason(reason)
                        .setRejectedAt(Instant.now())
                        .build());
            }
        }

        for (var item : event.getItems()) {
            Product product = products.findByIdForUpdate(item.getProductId()).orElseThrow();
            product.decrementStock(item.getQuantity());
            reservations.save(new Reservation(event.getOrderId(), item.getProductId(), item.getQuantity()));
        }
        log.info("Stock réservé pour la commande {}", event.getOrderId());
        return Optional.of(StockReserved.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(event.getOrderId())
                .setTotalAmount(event.getTotalAmount())
                .setReservedAt(Instant.now())
                .build());
    }

    /**
     * Compensation : le paiement a échoué, on restitue le stock réservé.
     */
    @Transactional
    public Optional<SpecificRecord> release(PaymentFailed event) {
        if (isDuplicate(event.getEventId())) {
            return Optional.empty();
        }

        List<Reservation> orderReservations = reservations.findByOrderId(event.getOrderId());
        if (orderReservations.isEmpty()) {
            log.warn("Aucune réservation à libérer pour la commande {}", event.getOrderId());
            return Optional.empty();
        }
        for (Reservation reservation : orderReservations) {
            products.findByIdForUpdate(reservation.getProductId())
                    .ifPresent(p -> p.incrementStock(reservation.getQuantity()));
        }
        reservations.deleteByOrderId(event.getOrderId());
        log.info("Stock libéré pour la commande {} (compensation)", event.getOrderId());
        return Optional.of(StockReleased.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(event.getOrderId())
                .setReleasedAt(Instant.now())
                .build());
    }

    private boolean isDuplicate(String eventId) {
        if (processedEvents.existsById(eventId)) {
            log.warn("Événement {} déjà traité, ignoré (déduplication)", eventId);
            return true;
        }
        processedEvents.save(new ProcessedEvent(eventId));
        return false;
    }
}
