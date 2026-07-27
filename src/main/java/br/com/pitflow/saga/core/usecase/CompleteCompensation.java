package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.ServiceOrderCancelledEvent;
import br.com.pitflow.saga.core.gateway.SagaCompensationGateway.HandleResult;

public interface CompleteCompensation {
    HandleResult execute(ServiceOrderCancelledEvent event);
}
