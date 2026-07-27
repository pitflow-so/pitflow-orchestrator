package br.com.pitflow.saga.controller;

import br.com.pitflow.saga.core.entity.Money;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.gateway.SagaStartGateway.StartResult;
import br.com.pitflow.saga.core.usecase.StartPaymentSaga;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class SagaEventController {
    private final StartPaymentSaga startPaymentSaga;

    public SagaEventController(StartPaymentSaga startPaymentSaga) {
        this.startPaymentSaga = startPaymentSaga;
    }

    public StartResult budgetApproved(BudgetApprovedCommand command) {
        return startPaymentSaga.execute(new BudgetApprovedEvent(
                command.messageId(),
                command.correlationId(),
                command.serviceOrderId(),
                new Money(
                        new BigDecimal(command.amount()),
                        command.currency()
                ),
                command.occurredAt()
        ));
    }

    public record BudgetApprovedCommand(
            UUID messageId,
            UUID correlationId,
            UUID serviceOrderId,
            String amount,
            String currency,
            Instant occurredAt
    ) {
    }
}
