package br.com.pitflow.saga.core.event;

import java.time.Instant;
import java.util.UUID;

public record ServiceOrderReadyForExecutionEvent(
        UUID messageId,
        UUID correlationId,
        UUID causationId,
        UUID sagaId,
        UUID serviceOrderId,
        UUID paymentId,
        Instant occurredAt
) {
}
