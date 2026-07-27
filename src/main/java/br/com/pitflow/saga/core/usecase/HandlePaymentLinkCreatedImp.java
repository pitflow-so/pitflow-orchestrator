package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.MarkServiceOrderAwaitingPaymentCommand;
import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway;

import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

public final class HandlePaymentLinkCreatedImp implements HandlePaymentLinkCreated {
    private final SagaPaymentLinkGateway gateway;
    private final Clock clock;
    private final Supplier<UUID> ids;

    public HandlePaymentLinkCreatedImp(
            SagaPaymentLinkGateway gateway,
            Clock clock,
            Supplier<UUID> ids
    ) {
        this.gateway = gateway;
        this.clock = clock;
        this.ids = ids;
    }

    @Override
    public SagaPaymentLinkGateway.HandleResult execute(
            PaymentLinkCreatedEvent event
    ) {
        validate(event);
        var command = new MarkServiceOrderAwaitingPaymentCommand(
                ids.get(),
                event.correlationId(),
                event.messageId(),
                event.sagaId(),
                event.serviceOrderId(),
                event.paymentId(),
                event.preferenceId(),
                event.checkoutUrl(),
                event.expiresAt(),
                clock.instant()
        );
        return gateway.handleAtomically(event, command);
    }

    private static void validate(PaymentLinkCreatedEvent event) {
        if (event == null
                || event.messageId() == null
                || event.correlationId() == null
                || event.sagaId() == null
                || event.serviceOrderId() == null
                || event.paymentId() == null
                || event.preferenceId() == null
                || event.preferenceId().isBlank()
                || event.checkoutUrl() == null
                || event.checkoutUrl().isBlank()
                || event.expiresAt() == null
                || event.occurredAt() == null) {
            throw new IllegalArgumentException(
                    "Invalid PaymentLinkCreated event"
            );
        }
    }
}
