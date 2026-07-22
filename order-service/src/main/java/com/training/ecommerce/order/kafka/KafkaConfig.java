package com.training.ecommerce.order.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class KafkaConfig {

    /**
     * Factory dédiée aux topics JSON (shipping.events). La factory par défaut,
     * auto-configurée depuis application.yml, désérialise en Avro.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> jsonListenerFactory(KafkaProperties properties) {
        Map<String, Object> config = properties.buildConsumerProperties(null);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // Le type concret est résolu via le header __TypeId__ écrit par le producteur
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.training.ecommerce.common.event");

        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        return factory;
    }
}
