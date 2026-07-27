package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.CancelServiceOrderCommand;
import br.com.pitflow.saga.core.event.PaymentRejectedEvent;
import br.com.pitflow.saga.core.gateway.SagaCompensationGateway;

import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

public final class HandlePaymentRejectedImp implements HandlePaymentRejected {
    private final SagaCompensationGateway gateway;
    private final Clock clock;
    private final Supplier<UUID> ids;

    public HandlePaymentRejectedImp(SagaCompensationGateway gateway, Clock clock, Supplier<UUID> ids) {
        this.gateway = gateway; this.clock = clock; this.ids = ids;
    }

    @Override
    public SagaCompensationGateway.HandleResult execute(PaymentRejectedEvent event) {
        if (event == null || event.messageId() == null || event.correlationId() == null
                || event.sagaId() == null || event.serviceOrderId() == null || event.paymentId() == null
                || event.reason() == null || event.reason().isBlank() || event.occurredAt() == null)
            throw new IllegalArgumentException("Invalid PaymentRejected event");
        return gateway.startAtomically(event, new CancelServiceOrderCommand(ids.get(), event.correlationId(),
                event.messageId(), event.sagaId(), event.serviceOrderId(), event.paymentId(),
                event.reason(), clock.instant()));
    }
}
