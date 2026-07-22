package com.training.ecommerce.order.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotBlank String customerEmail,
        @NotEmpty @Valid List<Item> items) {

    public record Item(
            @NotBlank String productId,
            @Min(1) int quantity) {
    }
}
