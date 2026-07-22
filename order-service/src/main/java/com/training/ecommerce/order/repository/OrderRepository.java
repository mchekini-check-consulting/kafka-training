package com.training.ecommerce.order.repository;

import com.training.ecommerce.order.domain.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {
}
