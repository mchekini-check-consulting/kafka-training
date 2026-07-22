package com.training.ecommerce.inventory.repository;

import com.training.ecommerce.inventory.domain.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
