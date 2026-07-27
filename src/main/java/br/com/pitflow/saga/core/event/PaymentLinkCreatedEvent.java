package br.com.pitflow.saga.core.event;

import java.time.Instant;
import java.util.UUID;

public record PaymentLinkCreatedEvent(
        UUID messageId,
        UUID correlationId,
        UUID sagaId,
        UUID serviceOrderId,
        UUID paymentId,
        String preferenceId,
        String checkoutUrl,
        Instant expiresAt,
        Instant occurredAt
) {
}
