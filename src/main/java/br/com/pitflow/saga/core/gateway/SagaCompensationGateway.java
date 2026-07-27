package br.com.pitflow.saga.core.gateway;

import br.com.pitflow.saga.core.event.CancelServiceOrderCommand;
import br.com.pitflow.saga.core.event.PaymentRejectedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderCancelledEvent;

public interface SagaCompensationGateway {
    HandleResult startAtomically(PaymentRejectedEvent source, CancelServiceOrderCommand command);
    HandleResult failAtomically(ServiceOrderCancelledEvent source);
    enum HandleResult { UPDATED, ALREADY_PROCESSED }
}
