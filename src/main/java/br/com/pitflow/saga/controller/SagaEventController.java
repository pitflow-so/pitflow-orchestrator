package br.com.pitflow.saga.controller;

import br.com.pitflow.saga.core.entity.Money;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderAwaitingPaymentEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway.HandleResult;
import br.com.pitflow.saga.core.gateway.SagaStartGateway.StartResult;
import br.com.pitflow.saga.core.usecase.HandlePaymentLinkCreated;
import br.com.pitflow.saga.core.usecase.ConfirmServiceOrderAwaitingPayment;
import br.com.pitflow.saga.core.usecase.StartPaymentSaga;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class SagaEventController {
    private final StartPaymentSaga startPaymentSaga;
    private final HandlePaymentLinkCreated handlePaymentLinkCreated;
    private final ConfirmServiceOrderAwaitingPayment
            confirmServiceOrderAwaitingPayment;

    public SagaEventController(
            StartPaymentSaga startPaymentSaga,
            HandlePaymentLinkCreated handlePaymentLinkCreated,
            ConfirmServiceOrderAwaitingPayment
                    confirmServiceOrderAwaitingPayment
    ) {
        this.startPaymentSaga = startPaymentSaga;
        this.handlePaymentLinkCreated = handlePaymentLinkCreated;
        this.confirmServiceOrderAwaitingPayment =
                confirmServiceOrderAwaitingPayment;
    }

    public HandleResult serviceOrderAwaitingPayment(
            ServiceOrderAwaitingPaymentCommand command
    ) {
        return confirmServiceOrderAwaitingPayment.execute(
                new ServiceOrderAwaitingPaymentEvent(
                        command.messageId(),
                        command.correlationId(),
                        command.causationId(),
                        command.sagaId(),
                        command.serviceOrderId(),
                        command.paymentId(),
                        command.occurredAt()
                )
        );
    }

    public HandleResult paymentLinkCreated(PaymentLinkCreatedCommand command) {
        return handlePaymentLinkCreated.execute(new PaymentLinkCreatedEvent(
                command.messageId(),
                command.correlationId(),
                command.sagaId(),
                command.serviceOrderId(),
                command.paymentId(),
                command.preferenceId(),
                command.checkoutUrl(),
                command.expiresAt(),
                command.occurredAt()
        ));
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

    public record PaymentLinkCreatedCommand(
            UUID messageId,
            UUID correlationId,
            UUID sagaId,
            UUID serviceOrderId,
            UUID paymentId,
            String preferenceId,
            String checkoutUrl,
            Instant expiresAt,
            Instant occurredAt
    ) {
    }

    public record ServiceOrderAwaitingPaymentCommand(
            UUID messageId,
            UUID correlationId,
            UUID causationId,
            UUID sagaId,
            UUID serviceOrderId,
            UUID paymentId,
            Instant occurredAt
    ) {
    }
}
