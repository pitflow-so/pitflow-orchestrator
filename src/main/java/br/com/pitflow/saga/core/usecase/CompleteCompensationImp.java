package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.ServiceOrderCancelledEvent;
import br.com.pitflow.saga.core.gateway.SagaCompensationGateway;

public final class CompleteCompensationImp implements CompleteCompensation {
    private final SagaCompensationGateway gateway;
    public CompleteCompensationImp(SagaCompensationGateway gateway) { this.gateway = gateway; }
    @Override
    public SagaCompensationGateway.HandleResult execute(ServiceOrderCancelledEvent event) {
        if (event == null || event.messageId() == null || event.sagaId() == null
                || event.serviceOrderId() == null || event.paymentId() == null || event.occurredAt() == null)
            throw new IllegalArgumentException("Invalid ServiceOrderCancelled event");
        return gateway.failAtomically(event);
    }
}
