package br.com.pitflow.saga.core.event;
import br.com.pitflow.saga.core.entity.Money;
import java.time.Instant;
import java.util.UUID;
public record BudgetApprovedEvent(UUID messageId, UUID correlationId, UUID serviceOrderId, Money amount, Instant occurredAt) {}
