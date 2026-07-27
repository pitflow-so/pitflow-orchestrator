package br.com.pitflow;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "orchestrator.consumer.enabled=false",
        "orchestrator.outbox.enabled=false"
})
class PitflowOrchestratorApplicationTest {
    @Test
    void contextLoads() {
    }
}
