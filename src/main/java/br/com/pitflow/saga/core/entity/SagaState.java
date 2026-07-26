package br.com.pitflow.saga.core.entity;
public enum SagaState {
    PAYMENT_CREATION_PENDING, AWAITING_PAYMENT, RELEASING_SERVICE_ORDER,
    COMPLETED, COMPENSATING, FAILED
}
