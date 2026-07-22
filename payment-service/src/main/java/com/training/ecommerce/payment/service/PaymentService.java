package com.training.ecommerce.payment.service;

import com.training.ecommerce.avro.PaymentFailed;
import com.training.ecommerce.avro.PaymentSucceeded;
import com.training.ecommerce.avro.StockReserved;
import com.training.ecommerce.payment.domain.Payment;
import com.training.ecommerce.payment.domain.ProcessedEvent;
import com.training.ecommerce.payment.repository.PaymentRepository;
import com.training.ecommerce.payment.repository.ProcessedEventRepository;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository payments;
    private final ProcessedEventRepository processedEvents;
    private final double successRate;

    public PaymentService(PaymentRepository payments,
                          ProcessedEventRepository processedEvents,
                          @Value("${payment.success-rate}") double successRate) {
        this.payments = payments;
        this.processedEvents = processedEvents;
        this.successRate = successRate;
    }

    /**
     * Traite le paiement d'une commande dont le stock est réservé.
     * Simulation : latence de 200 ms + échec aléatoire selon payment.success-rate.
     */
    @Transactional
    public Optional<SpecificRecord> process(StockReserved event) {
        if (processedEvents.existsById(event.getEventId())) {
            log.warn("Événement {} déjà traité — le client ne sera PAS débité deux fois", event.getEventId());
            return Optional.empty();
        }
        processedEvents.save(new ProcessedEvent(event.getEventId()));

        simulateBankCall();

        boolean success = ThreadLocalRandom.current().nextDouble() < successRate;
        String paymentId = UUID.randomUUID().toString();
        payments.save(new Payment(paymentId, event.getOrderId(), event.getTotalAmount(),
                success ? Payment.Status.SUCCEEDED : Payment.Status.FAILED));

        if (success) {
            log.info("Paiement {} accepté pour la commande {} ({} €)",
                    paymentId, event.getOrderId(), event.getTotalAmount());
            return Optional.of(PaymentSucceeded.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setOrderId(event.getOrderId())
                    .setPaymentId(paymentId)
                    .setAmount(event.getTotalAmount())
                    .setProcessedAt(Instant.now())
                    .build());
        }
        log.warn("Paiement refusé pour la commande {}", event.getOrderId());
        return Optional.of(PaymentFailed.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(event.getOrderId())
                .setReason("Carte refusée par la banque")
                .setProcessedAt(Instant.now())
                .build());
    }

    private void simulateBankCall() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
