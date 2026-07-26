package br.com.pitflow.saga.core.usecase;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.gateway.SagaStartGateway.StartResult;
public interface StartPaymentSaga { StartResult execute(BudgetApprovedEvent event); }
