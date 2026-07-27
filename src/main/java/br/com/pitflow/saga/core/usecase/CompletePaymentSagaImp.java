package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.ServiceOrderReadyForExecutionEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway;

public final class CompletePaymentSagaImp implements CompletePaymentSaga {
    private final SagaPaymentApprovalGateway gateway;

    public CompletePaymentSagaImp(SagaPaymentApprovalGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public SagaPaymentApprovalGateway.HandleResult execute(
            ServiceOrderReadyForExecutionEvent event
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
                    "Invalid ServiceOrderReadyForExecution event"
            );
        }
        return gateway.completeAtomically(event);
    }
}
