package br.com.pitflow.saga.core.event;

import br.com.pitflow.saga.core.entity.Money;

import java.time.Instant;
import java.util.UUID;

public record PaymentApprovedEvent(
        UUID messageId,
        UUID correlationId,
        UUID sagaId,
        UUID serviceOrderId,
        UUID paymentId,
        Money approvedAmount,
        String externalPaymentId,
        Instant occurredAt
) {
}
