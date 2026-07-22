package com.training.ecommerce.analytics.api;

import com.training.ecommerce.analytics.domain.Metric;
import com.training.ecommerce.analytics.kafka.AnalyticsListener;
import com.training.ecommerce.analytics.repository.MetricRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.TreeMap;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final MetricRepository metrics;
    private final AnalyticsListener listener;

    public AnalyticsController(MetricRepository metrics, AnalyticsListener listener) {
        this.metrics = metrics;
        this.listener = listener;
    }

    @GetMapping("/dashboard")
    public Map<String, Double> dashboard() {
        Map<String, Double> dashboard = new TreeMap<>();
        for (Metric metric : metrics.findAll()) {
            dashboard.put(metric.getName(), metric.getValue());
        }
        return dashboard;
    }

    /**
     * Démo replay : remet les métriques à zéro et reconsomme tous les topics
     * depuis l'offset 0.
     */
    @PostMapping("/replay")
    public ResponseEntity<String> replay() {
        listener.replayFromBeginning();
        return ResponseEntity.accepted().body("Replay lancé : les agrégats vont être reconstruits\n");
    }
}
