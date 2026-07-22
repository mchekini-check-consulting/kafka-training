package com.training.ecommerce.analytics.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "metrics")
public class Metric {

    @Id
    private String name;
    private double value;

    protected Metric() {
    }

    public Metric(String name, double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public double getValue() {
        return value;
    }
}
