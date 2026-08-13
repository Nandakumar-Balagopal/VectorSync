package io.vectorsync.embeddingworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EmbeddingWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(EmbeddingWorkerApplication.class, args);
    }
}

// Made with Bob
