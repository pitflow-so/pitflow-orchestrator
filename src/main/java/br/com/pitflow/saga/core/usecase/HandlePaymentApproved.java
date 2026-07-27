package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.PaymentApprovedEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway.HandleResult;

public interface HandlePaymentApproved {
    HandleResult execute(PaymentApprovedEvent event);
}
