package br.com.pitflow;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@EnableScheduling
@SpringBootApplication
public class PitflowOrchestratorApplication {
    public static void main(String[] args) {
        SpringApplication.run(PitflowOrchestratorApplication.class, args);
    }
}
