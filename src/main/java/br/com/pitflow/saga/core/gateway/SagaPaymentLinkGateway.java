package br.com.pitflow.saga.core.gateway;

import br.com.pitflow.saga.core.event.MarkServiceOrderAwaitingPaymentCommand;
import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderAwaitingPaymentEvent;

public interface SagaPaymentLinkGateway {
    HandleResult handleAtomically(
            PaymentLinkCreatedEvent source,
            MarkServiceOrderAwaitingPaymentCommand command
    );

    HandleResult confirmAwaitingPaymentAtomically(
            ServiceOrderAwaitingPaymentEvent source
    );

    enum HandleResult {
        UPDATED,
        ALREADY_PROCESSED
    }
}
