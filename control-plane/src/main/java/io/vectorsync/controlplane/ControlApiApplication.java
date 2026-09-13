package io.vectorsync.controlplane;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
// Without this, @Async on table discovery was a no-op: the crawl ran on the request
// thread and its failures became a 500 even though the endpoint returns 202.
@EnableAsync
public class ControlApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ControlApiApplication.class, args);
    }
}
