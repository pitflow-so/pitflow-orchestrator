package br.com.pitflow.saga.core.gateway;
import br.com.pitflow.saga.core.entity.PaymentSaga;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.event.CreatePaymentCommand;
public interface SagaStartGateway {
    StartResult startAtomically(BudgetApprovedEvent source, PaymentSaga saga, CreatePaymentCommand command);
    enum StartResult { CREATED, ALREADY_PROCESSED }
}
