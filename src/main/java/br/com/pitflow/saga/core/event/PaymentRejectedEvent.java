package br.com.pitflow.saga.core.event;

import java.time.Instant;
import java.util.UUID;

public record PaymentRejectedEvent(UUID messageId, UUID correlationId, UUID sagaId,
                                   UUID serviceOrderId, UUID paymentId,
                                   String reasonCode, String reason, Instant occurredAt) {}
