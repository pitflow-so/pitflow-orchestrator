package br.com.pitflow.saga.infrastructure.configuration;

import br.com.pitflow.saga.controller.SagaEventController;
import br.com.pitflow.saga.core.gateway.SagaStartGateway;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway;
import br.com.pitflow.saga.core.usecase.HandlePaymentLinkCreated;
import br.com.pitflow.saga.core.usecase.HandlePaymentLinkCreatedImp;
import br.com.pitflow.saga.core.usecase.HandlePaymentApproved;
import br.com.pitflow.saga.core.usecase.HandlePaymentApprovedImp;
import br.com.pitflow.saga.core.usecase.ConfirmServiceOrderAwaitingPayment;
import br.com.pitflow.saga.core.usecase.ConfirmServiceOrderAwaitingPaymentImp;
import br.com.pitflow.saga.core.usecase.CompletePaymentSaga;
import br.com.pitflow.saga.core.usecase.CompletePaymentSagaImp;
import br.com.pitflow.saga.core.usecase.StartPaymentSaga;
import br.com.pitflow.saga.core.usecase.StartPaymentSagaImp;
import br.com.pitflow.saga.infrastructure.consumer.sqs.OrchestratorEventConsumer;
import br.com.pitflow.saga.infrastructure.outbox.DynamoDbOutboxRepository;
import br.com.pitflow.saga.infrastructure.outbox.OutboxPublisherScheduler;
import br.com.pitflow.saga.infrastructure.persistence.dynamodb.DynamoDbSagaStartAdapter;
import br.com.pitflow.saga.infrastructure.persistence.dynamodb.DynamoDbSagaPaymentLinkAdapter;
import br.com.pitflow.saga.infrastructure.persistence.dynamodb.DynamoDbSagaPaymentApprovalAdapter;
import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

@Configuration
public class BeanSagaConfig {
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    DynamoDbClient dynamoDbClient(
            @Value("${aws.region}") String region
    ) {
        return DynamoDbClient.builder().region(Region.of(region)).build();
    }

    @Bean
    SqsClient sqsClient(@Value("${aws.region}") String region) {
        return SqsClient.builder().region(Region.of(region)).build();
    }

    @Bean
    SagaStartGateway sagaStartGateway(
            DynamoDbClient client,
            ObjectMapper objectMapper,
            @Value("${aws.dynamodb.table-name}") String tableName,
            @Value("${aws.sqs.payment-command-queue}") String destination
    ) {
        return new DynamoDbSagaStartAdapter(
                client, objectMapper, tableName, destination
        );
    }

    @Bean
    SagaPaymentLinkGateway sagaPaymentLinkGateway(
            DynamoDbClient client,
            ObjectMapper objectMapper,
            @Value("${aws.dynamodb.table-name}") String tableName,
            @Value("${aws.sqs.operation-command-queue}") String destination
    ) {
        return new DynamoDbSagaPaymentLinkAdapter(
                client, objectMapper, tableName, destination
        );
    }

    @Bean
    SagaPaymentApprovalGateway sagaPaymentApprovalGateway(
            DynamoDbClient client,
            ObjectMapper objectMapper,
            @Value("${aws.dynamodb.table-name}") String tableName,
            @Value("${aws.sqs.operation-command-queue}") String destination
    ) {
        return new DynamoDbSagaPaymentApprovalAdapter(
                client, objectMapper, tableName, destination
        );
    }

    @Bean
    HandlePaymentApproved handlePaymentApproved(
            SagaPaymentApprovalGateway gateway,
            Clock clock
    ) {
        return new HandlePaymentApprovedImp(
                gateway, clock, UUID::randomUUID
        );
    }

    @Bean
    CompletePaymentSaga completePaymentSaga(
            SagaPaymentApprovalGateway gateway
    ) {
        return new CompletePaymentSagaImp(gateway);
    }

    @Bean
    HandlePaymentLinkCreated handlePaymentLinkCreated(
            SagaPaymentLinkGateway gateway,
            Clock clock
    ) {
        return new HandlePaymentLinkCreatedImp(
                gateway, clock, UUID::randomUUID
        );
    }

    @Bean
    ConfirmServiceOrderAwaitingPayment confirmServiceOrderAwaitingPayment(
            SagaPaymentLinkGateway gateway
    ) {
        return new ConfirmServiceOrderAwaitingPaymentImp(gateway);
    }

    @Bean
    StartPaymentSaga startPaymentSaga(
            SagaStartGateway gateway,
            Clock clock
    ) {
        return new StartPaymentSagaImp(gateway, clock, UUID::randomUUID);
    }

    @Bean
    SagaEventController sagaEventController(
            StartPaymentSaga startPaymentSaga,
            HandlePaymentLinkCreated handlePaymentLinkCreated,
            HandlePaymentApproved handlePaymentApproved,
            CompletePaymentSaga completePaymentSaga,
            ConfirmServiceOrderAwaitingPayment
                    confirmServiceOrderAwaitingPayment
    ) {
        return new SagaEventController(
                startPaymentSaga,
                handlePaymentLinkCreated,
                handlePaymentApproved,
                completePaymentSaga,
                confirmServiceOrderAwaitingPayment
        );
    }

    @Bean
    @ConditionalOnProperty(
            name = "orchestrator.consumer.enabled",
            havingValue = "true"
    )
    OrchestratorEventConsumer orchestratorEventConsumer(
            SqsClient sqs,
            ObjectMapper objectMapper,
            SagaEventController controller,
            @Value("${aws.sqs.orchestrator-queue}") String queueName,
            @Value("${orchestrator.consumer.wait-time-seconds}") int wait
    ) {
        return new OrchestratorEventConsumer(
                sqs, objectMapper, controller, queueName, wait
        );
    }

    @Bean
    DynamoDbOutboxRepository dynamoDbOutboxRepository(
            DynamoDbClient client,
            @Value("${aws.dynamodb.table-name}") String tableName
    ) {
        return new DynamoDbOutboxRepository(client, tableName);
    }

    @Bean
    @ConditionalOnProperty(
            name = "orchestrator.outbox.enabled",
            havingValue = "true"
    )
    OutboxPublisherScheduler outboxPublisherScheduler(
            DynamoDbOutboxRepository repository,
            SqsClient sqs,
            Clock clock,
            @Value("${orchestrator.outbox.batch-size}") int batchSize,
            @Value("${orchestrator.outbox.lease-seconds}") long lease,
            @Value("${orchestrator.outbox.max-backoff-seconds}") int backoff
    ) {
        return new OutboxPublisherScheduler(
                repository,
                sqs,
                clock,
                batchSize,
                Duration.ofSeconds(lease),
                backoff
        );
    }

    @Bean
    ThreadPoolTaskScheduler taskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("pitflow-scheduler-");
        return scheduler;
    }
}
