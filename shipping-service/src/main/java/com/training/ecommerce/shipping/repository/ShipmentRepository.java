package com.training.ecommerce.shipping.repository;

import com.training.ecommerce.shipping.domain.Shipment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface ShipmentRepository extends JpaRepository<Shipment, String> {

    Optional<Shipment> findByOrderId(String orderId);

    List<Shipment> findByStatusAndDispatchedAtBefore(Shipment.Status status, Instant before);
}
