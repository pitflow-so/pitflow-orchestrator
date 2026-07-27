package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.ServiceOrderReadyForExecutionEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway.HandleResult;

public interface CompletePaymentSaga {
    HandleResult execute(ServiceOrderReadyForExecutionEvent event);
}
