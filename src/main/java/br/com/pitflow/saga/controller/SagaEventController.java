package br.com.pitflow.saga.controller;

import br.com.pitflow.saga.core.entity.Money;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.event.PaymentApprovedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderAwaitingPaymentEvent;
import br.com.pitflow.saga.core.event.ServiceOrderReadyForExecutionEvent;
import br.com.pitflow.saga.core.event.PaymentRejectedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderCancelledEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway.HandleResult;
import br.com.pitflow.saga.core.gateway.SagaStartGateway.StartResult;
import br.com.pitflow.saga.core.usecase.HandlePaymentLinkCreated;
import br.com.pitflow.saga.core.usecase.HandlePaymentApproved;
import br.com.pitflow.saga.core.usecase.ConfirmServiceOrderAwaitingPayment;
import br.com.pitflow.saga.core.usecase.CompletePaymentSaga;
import br.com.pitflow.saga.core.usecase.StartPaymentSaga;
import br.com.pitflow.saga.core.usecase.HandlePaymentRejected;
import br.com.pitflow.saga.core.usecase.CompleteCompensation;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class SagaEventController {
    private final StartPaymentSaga startPaymentSaga;
    private final HandlePaymentLinkCreated handlePaymentLinkCreated;
    private final HandlePaymentApproved handlePaymentApproved;
    private final CompletePaymentSaga completePaymentSaga;
    private final ConfirmServiceOrderAwaitingPayment
            confirmServiceOrderAwaitingPayment;
    private final HandlePaymentRejected handlePaymentRejected;
    private final CompleteCompensation completeCompensation;

    public SagaEventController(
            StartPaymentSaga startPaymentSaga,
            HandlePaymentLinkCreated handlePaymentLinkCreated,
            HandlePaymentApproved handlePaymentApproved,
            CompletePaymentSaga completePaymentSaga,
            ConfirmServiceOrderAwaitingPayment
                    confirmServiceOrderAwaitingPayment,
            HandlePaymentRejected handlePaymentRejected,
            CompleteCompensation completeCompensation
    ) {
        this.startPaymentSaga = startPaymentSaga;
        this.handlePaymentLinkCreated = handlePaymentLinkCreated;
        this.handlePaymentApproved = handlePaymentApproved;
        this.completePaymentSaga = completePaymentSaga;
        this.confirmServiceOrderAwaitingPayment =
                confirmServiceOrderAwaitingPayment;
        this.handlePaymentRejected = handlePaymentRejected;
        this.completeCompensation = completeCompensation;
    }

    public br.com.pitflow.saga.core.gateway.SagaCompensationGateway.HandleResult paymentRejected(
            PaymentRejectedCommand command) {
        return handlePaymentRejected.execute(new PaymentRejectedEvent(command.messageId(), command.correlationId(),
                command.sagaId(), command.serviceOrderId(), command.paymentId(), command.reasonCode(),
                command.reason(), command.occurredAt()));
    }

    public br.com.pitflow.saga.core.gateway.SagaCompensationGateway.HandleResult serviceOrderCancelled(
            ServiceOrderCancelledCommand command) {
        return completeCompensation.execute(new ServiceOrderCancelledEvent(command.messageId(),
                command.correlationId(), command.causationId(), command.sagaId(), command.serviceOrderId(),
                command.paymentId(), command.reason(), command.occurredAt()));
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

    public br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway.HandleResult
    paymentApproved(PaymentApprovedCommand command) {
        return handlePaymentApproved.execute(new PaymentApprovedEvent(
                command.messageId(),
                command.correlationId(),
                command.sagaId(),
                command.serviceOrderId(),
                command.paymentId(),
                new Money(
                        new BigDecimal(command.approvedAmount()),
                        command.currency()
                ),
                command.externalPaymentId(),
                command.occurredAt()
        ));
    }

    public br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway.HandleResult
    serviceOrderReadyForExecution(
            ServiceOrderReadyForExecutionCommand command
    ) {
        return completePaymentSaga.execute(
                new ServiceOrderReadyForExecutionEvent(
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

    public record PaymentApprovedCommand(
            UUID messageId,
            UUID correlationId,
            UUID sagaId,
            UUID serviceOrderId,
            UUID paymentId,
            String approvedAmount,
            String currency,
            String externalPaymentId,
            Instant occurredAt
    ) {
    }

    public record ServiceOrderReadyForExecutionCommand(
            UUID messageId,
            UUID correlationId,
            UUID causationId,
            UUID sagaId,
            UUID serviceOrderId,
            UUID paymentId,
            Instant occurredAt
    ) {
    }

    public record PaymentRejectedCommand(UUID messageId, UUID correlationId, UUID sagaId,
                                         UUID serviceOrderId, UUID paymentId, String reasonCode,
                                         String reason, Instant occurredAt) {}

    public record ServiceOrderCancelledCommand(UUID messageId, UUID correlationId, UUID causationId,
                                               UUID sagaId, UUID serviceOrderId, UUID paymentId,
                                               String reason, Instant occurredAt) {}
}
