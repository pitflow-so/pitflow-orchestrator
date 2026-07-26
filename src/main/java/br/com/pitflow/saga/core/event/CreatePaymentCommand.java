package br.com.pitflow.saga.core.event;
import br.com.pitflow.saga.core.entity.Money;
import java.time.Instant;
import java.util.UUID;
public record CreatePaymentCommand(UUID messageId, UUID correlationId, UUID causationId, UUID sagaId,
        UUID serviceOrderId, Money amount, String description, UUID idempotencyKey, Instant occurredAt) {}
