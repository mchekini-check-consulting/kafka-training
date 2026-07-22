package com.training.ecommerce.notification.service;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Use case virtual threads (Java 21) : chaque envoi simule un appel SMTP de
 * ~300 ms. Avec un pool de threads plateforme classique, 1000 notifications
 * simultanées satureraient le pool ; avec un virtual thread par envoi, le
 * débit ne dépend plus de la taille d'un pool.
 */
@Service
public class NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(NotificationSender.class);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public void send(String recipient, String subject, String body) {
        executor.submit(() -> {
            simulateSmtpCall();
            log.info("Email envoyé à {} [{}] : {} (thread virtuel : {})",
                    recipient, subject, body, Thread.currentThread().isVirtual());
        });
    }

    private void simulateSmtpCall() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    void shutdown() {
        executor.shutdown();
    }
}
