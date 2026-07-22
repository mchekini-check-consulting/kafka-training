package com.training.ecommerce.analytics.repository;

import com.training.ecommerce.analytics.domain.Metric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface MetricRepository extends JpaRepository<Metric, String> {

    /**
     * Upsert atomique : plusieurs threads consumers (concurrency: 3)
     * incrémentent les mêmes compteurs sans se marcher dessus.
     */
    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO metrics(name, value) VALUES (:name, :delta)
            ON CONFLICT (name) DO UPDATE SET value = metrics.value + :delta
            """, nativeQuery = true)
    void increment(@Param("name") String name, @Param("delta") double delta);
}
