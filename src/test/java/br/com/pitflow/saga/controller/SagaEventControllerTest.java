package br.com.pitflow.saga.controller;

import br.com.pitflow.saga.core.gateway.SagaCompensationGateway;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway;
import br.com.pitflow.saga.core.gateway.SagaStartGateway;
import br.com.pitflow.saga.core.usecase.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class SagaEventControllerTest {
    private final StartPaymentSaga start = mock(StartPaymentSaga.class);
    private final HandlePaymentLinkCreated link = mock(HandlePaymentLinkCreated.class);
    private final HandlePaymentApproved approved = mock(HandlePaymentApproved.class);
    private final CompletePaymentSaga complete = mock(CompletePaymentSaga.class);
    private final ConfirmServiceOrderAwaitingPayment awaiting = mock(ConfirmServiceOrderAwaitingPayment.class);
    private final HandlePaymentRejected rejected = mock(HandlePaymentRejected.class);
    private final CompleteCompensation compensation = mock(CompleteCompensation.class);
    private final SagaEventController controller = new SagaEventController(
            start, link, approved, complete, awaiting, rejected, compensation);
    private final Instant now = Instant.parse("2026-07-27T15:00:00Z");

    @Test
    void routesHappyPathCommandsToTheirUseCases() {
        UUID message = UUID.randomUUID(), correlation = UUID.randomUUID(), saga = UUID.randomUUID();
        UUID order = UUID.randomUUID(), payment = UUID.randomUUID(), causation = UUID.randomUUID();
        when(start.execute(any())).thenReturn(SagaStartGateway.StartResult.CREATED);
        when(link.execute(any())).thenReturn(SagaPaymentLinkGateway.HandleResult.UPDATED);
        when(awaiting.execute(any())).thenReturn(SagaPaymentLinkGateway.HandleResult.UPDATED);
        when(approved.execute(any())).thenReturn(SagaPaymentApprovalGateway.HandleResult.UPDATED);
        when(complete.execute(any())).thenReturn(SagaPaymentApprovalGateway.HandleResult.UPDATED);

        assertEquals(SagaStartGateway.StartResult.CREATED, controller.budgetApproved(
                new SagaEventController.BudgetApprovedCommand(message, correlation, order, "250.99", "BRL", now)));
        assertEquals(SagaPaymentLinkGateway.HandleResult.UPDATED, controller.paymentLinkCreated(
                new SagaEventController.PaymentLinkCreatedCommand(message, correlation, saga, order, payment,
                        "pref", "https://checkout", now.plusSeconds(900), now)));
        assertEquals(SagaPaymentLinkGateway.HandleResult.UPDATED, controller.serviceOrderAwaitingPayment(
                new SagaEventController.ServiceOrderAwaitingPaymentCommand(
                        message, correlation, causation, saga, order, payment, now)));
        assertEquals(SagaPaymentApprovalGateway.HandleResult.UPDATED, controller.paymentApproved(
                new SagaEventController.PaymentApprovedCommand(message, correlation, saga, order, payment,
                        "250.99", "BRL", "mp-1", now)));
        assertEquals(SagaPaymentApprovalGateway.HandleResult.UPDATED, controller.serviceOrderReadyForExecution(
                new SagaEventController.ServiceOrderReadyForExecutionCommand(
                        message, correlation, causation, saga, order, payment, now)));

        verify(start).execute(argThat(event -> event.amount().amount().toPlainString().equals("250.99")));
        verify(approved).execute(argThat(event -> event.externalPaymentId().equals("mp-1")));
        verify(complete).execute(argThat(event -> event.paymentId().equals(payment)));
    }

    @Test
    void routesCompensationCommandsToTheirUseCases() {
        UUID message = UUID.randomUUID(), correlation = UUID.randomUUID(), saga = UUID.randomUUID();
        UUID order = UUID.randomUUID(), payment = UUID.randomUUID(), causation = UUID.randomUUID();
        when(rejected.execute(any())).thenReturn(SagaCompensationGateway.HandleResult.UPDATED);
        when(compensation.execute(any())).thenReturn(SagaCompensationGateway.HandleResult.UPDATED);

        assertEquals(SagaCompensationGateway.HandleResult.UPDATED, controller.paymentRejected(
                new SagaEventController.PaymentRejectedCommand(message, correlation, saga, order, payment,
                        "DECLINED", "Pagamento recusado", now)));
        assertEquals(SagaCompensationGateway.HandleResult.UPDATED, controller.serviceOrderCancelled(
                new SagaEventController.ServiceOrderCancelledCommand(message, correlation, causation, saga,
                        order, payment, "Pagamento recusado", now)));

        verify(rejected).execute(argThat(event -> event.reasonCode().equals("DECLINED")));
        verify(compensation).execute(argThat(event -> event.causationId().equals(causation)));
    }
}
