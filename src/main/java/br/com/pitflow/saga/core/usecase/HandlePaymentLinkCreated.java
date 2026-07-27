package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway.HandleResult;

public interface HandlePaymentLinkCreated {
    HandleResult execute(PaymentLinkCreatedEvent event);
}
