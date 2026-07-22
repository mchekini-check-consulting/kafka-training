package com.training.ecommerce.payment.repository;

import com.training.ecommerce.payment.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, String> {
}
