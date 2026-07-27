package br.com.pitflow.saga.core.event;

import java.time.Instant;
import java.util.UUID;

public record ServiceOrderCancelledEvent(UUID messageId, UUID correlationId, UUID causationId,
                                         UUID sagaId, UUID serviceOrderId, UUID paymentId,
                                         String reason, Instant occurredAt) {}
