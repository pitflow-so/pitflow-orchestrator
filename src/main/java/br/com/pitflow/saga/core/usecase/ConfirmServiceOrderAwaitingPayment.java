package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.ServiceOrderAwaitingPaymentEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway.HandleResult;

public interface ConfirmServiceOrderAwaitingPayment {
    HandleResult execute(ServiceOrderAwaitingPaymentEvent event);
}
