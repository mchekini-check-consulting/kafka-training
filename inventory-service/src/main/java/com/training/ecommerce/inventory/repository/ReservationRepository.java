package com.training.ecommerce.inventory.repository;

import com.training.ecommerce.inventory.domain.Reservation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {

    List<Reservation> findByOrderId(String orderId);

    void deleteByOrderId(String orderId);
}
