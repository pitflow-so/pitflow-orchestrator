package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.ServiceOrderAwaitingPaymentEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway;

public final class ConfirmServiceOrderAwaitingPaymentImp
        implements ConfirmServiceOrderAwaitingPayment {
    private final SagaPaymentLinkGateway gateway;

    public ConfirmServiceOrderAwaitingPaymentImp(
            SagaPaymentLinkGateway gateway
    ) {
        this.gateway = gateway;
    }

    @Override
    public SagaPaymentLinkGateway.HandleResult execute(
            ServiceOrderAwaitingPaymentEvent event
    ) {
        if (event == null
                || event.messageId() == null
                || event.correlationId() == null
                || event.causationId() == null
                || event.sagaId() == null
                || event.serviceOrderId() == null
                || event.paymentId() == null
                || event.occurredAt() == null) {
            throw new IllegalArgumentException(
                    "Invalid ServiceOrderAwaitingPayment event"
            );
        }
        return gateway.confirmAwaitingPaymentAtomically(event);
    }
}
