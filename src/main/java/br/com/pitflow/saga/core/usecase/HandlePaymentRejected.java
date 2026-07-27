package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.PaymentRejectedEvent;
import br.com.pitflow.saga.core.gateway.SagaCompensationGateway.HandleResult;

public interface HandlePaymentRejected {
    HandleResult execute(PaymentRejectedEvent event);
}
