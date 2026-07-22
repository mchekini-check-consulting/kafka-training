package com.training.ecommerce.order.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Optional;

/**
 * Appel REST synchrone vers inventory-service : la validation du prix au moment
 * de la commande a besoin d'une réponse immédiate (contrairement au reste de la
 * saga, asynchrone). Le correlation ID est propagé automatiquement dans les
 * headers HTTP par Micrometer Tracing.
 */
@Component
public class InventoryClient {

    public record ProductDto(String id, String name, double price, int stock) {
    }

    private final RestClient restClient;

    public InventoryClient(RestClient.Builder builder, @Value("${inventory.url}") String inventoryUrl) {
        this.restClient = builder.baseUrl(inventoryUrl).build();
    }

    public Optional<ProductDto> getProduct(String productId) {
        try {
            return Optional.ofNullable(restClient.get()
                    .uri("/api/products/{id}", productId)
                    .retrieve()
                    .body(ProductDto.class));
        } catch (org.springframework.web.client.HttpClientErrorException.NotFound e) {
            return Optional.empty();
        }
    }
}
