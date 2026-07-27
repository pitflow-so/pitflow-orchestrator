package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.entity.Money;
import br.com.pitflow.saga.core.event.MarkServiceOrderReadyForExecutionCommand;
import br.com.pitflow.saga.core.event.PaymentApprovedEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class HandlePaymentApprovedImpTest {
    @Test
    void createsReleaseCommandPreservingSagaCorrelation() {
        var gateway = mock(SagaPaymentApprovalGateway.class);
        var commandId = UUID.randomUUID();
        var now = Instant.parse("2026-07-27T05:00:00Z");
        var useCase = new HandlePaymentApprovedImp(
                gateway,
                Clock.fixed(now, ZoneOffset.UTC),
                () -> commandId
        );
        var event = new PaymentApprovedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                new Money(new BigDecimal("250.99"), "BRL"),
                "170713813744",
                Instant.parse("2026-07-27T04:00:00Z")
        );
        when(gateway.handleAtomically(any(), any())).thenReturn(
                SagaPaymentApprovalGateway.HandleResult.UPDATED
        );

        assertThat(useCase.execute(event)).isEqualTo(
                SagaPaymentApprovalGateway.HandleResult.UPDATED
        );
        var command = ArgumentCaptor.forClass(
                MarkServiceOrderReadyForExecutionCommand.class
        );
        verify(gateway).handleAtomically(eq(event), command.capture());
        assertThat(command.getValue().messageId()).isEqualTo(commandId);
        assertThat(command.getValue().causationId())
                .isEqualTo(event.messageId());
        assertThat(command.getValue().correlationId())
                .isEqualTo(event.correlationId());
        assertThat(command.getValue().sagaId()).isEqualTo(event.sagaId());
        assertThat(command.getValue().serviceOrderId())
                .isEqualTo(event.serviceOrderId());
        assertThat(command.getValue().paymentId())
                .isEqualTo(event.paymentId());
        assertThat(command.getValue().externalPaymentId())
                .isEqualTo(event.externalPaymentId());
        assertThat(command.getValue().occurredAt()).isEqualTo(now);
    }
}
