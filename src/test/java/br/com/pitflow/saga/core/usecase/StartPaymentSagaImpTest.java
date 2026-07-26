package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.entity.Money;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.gateway.SagaStartGateway;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class StartPaymentSagaImpTest {
    @Test
    void createsSagaAndPaymentCommandWithTraceability() {
        var captured = new ArrayList<Object>();
        SagaStartGateway gateway = (source, saga, command) -> {
            captured.add(saga); captured.add(command);
            return SagaStartGateway.StartResult.CREATED;
        };
        var sagaId = UUID.randomUUID();
        var commandId = UUID.randomUUID();
        var ids = new ArrayDeque<>(List.of(sagaId, commandId));
        var now = Instant.parse("2026-07-26T20:00:00Z");
        var useCase = new StartPaymentSagaImp(
                gateway, Clock.fixed(now, ZoneOffset.UTC), ids::remove
        );
        var sourceId = UUID.randomUUID();
        var correlationId = UUID.randomUUID();
        var orderId = UUID.randomUUID();

        var result = useCase.execute(new BudgetApprovedEvent(
                sourceId, correlationId, orderId,
                new Money(new BigDecimal("450.00"), "BRL"), now
        ));

        assertEquals(SagaStartGateway.StartResult.CREATED, result);
        var command = (br.com.pitflow.saga.core.event.CreatePaymentCommand) captured.get(1);
        assertEquals(sagaId, command.sagaId());
        assertEquals(sourceId, command.causationId());
        assertEquals(correlationId, command.correlationId());
        assertEquals(sagaId, command.idempotencyKey());
        assertEquals("450.00", command.amount().serializedAmount());
    }
}
