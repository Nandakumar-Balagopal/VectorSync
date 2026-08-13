package io.vectorsync.cdcworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CdcWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(CdcWorkerApplication.class, args);
    }
}

// Made with Bob
