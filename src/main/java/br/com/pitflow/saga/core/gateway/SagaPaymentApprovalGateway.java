package br.com.pitflow.saga.core.gateway;

import br.com.pitflow.saga.core.event.MarkServiceOrderReadyForExecutionCommand;
import br.com.pitflow.saga.core.event.PaymentApprovedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderReadyForExecutionEvent;

public interface SagaPaymentApprovalGateway {
    HandleResult handleAtomically(
            PaymentApprovedEvent source,
            MarkServiceOrderReadyForExecutionCommand command
    );

    HandleResult completeAtomically(
            ServiceOrderReadyForExecutionEvent source
    );

    enum HandleResult {
        UPDATED,
        ALREADY_PROCESSED
    }
}
