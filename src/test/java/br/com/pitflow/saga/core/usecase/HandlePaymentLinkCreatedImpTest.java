package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class HandlePaymentLinkCreatedImpTest {

    @Test
    void createsOperationCommandPreservingSagaCorrelation() {
        var gateway = mock(SagaPaymentLinkGateway.class);
        var commandId = UUID.randomUUID();
        var now = Instant.parse("2026-07-27T03:00:00Z");
        var useCase = new HandlePaymentLinkCreatedImp(
                gateway,
                Clock.fixed(now, ZoneOffset.UTC),
                () -> commandId
        );
        var event = event();
        when(gateway.handleAtomically(eq(event), any()))
                .thenReturn(SagaPaymentLinkGateway.HandleResult.UPDATED);

        var result = useCase.execute(event);

        var captor = ArgumentCaptor.forClass(
                br.com.pitflow.saga.core.event
                        .MarkServiceOrderAwaitingPaymentCommand.class
        );
        verify(gateway).handleAtomically(eq(event), captor.capture());
        var command = captor.getValue();
        assertEquals(commandId, command.messageId());
        assertEquals(event.messageId(), command.causationId());
        assertEquals(event.sagaId(), command.sagaId());
        assertEquals(event.checkoutUrl(), command.checkoutUrl());
        assertEquals(now, command.occurredAt());
        assertEquals(SagaPaymentLinkGateway.HandleResult.UPDATED, result);
    }

    private PaymentLinkCreatedEvent event() {
        return new PaymentLinkCreatedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                "pref-1",
                "https://sandbox.mercadopago.com/checkout",
                Instant.parse("2026-07-28T03:00:00Z"),
                Instant.parse("2026-07-27T02:59:00Z")
        );
    }
}
