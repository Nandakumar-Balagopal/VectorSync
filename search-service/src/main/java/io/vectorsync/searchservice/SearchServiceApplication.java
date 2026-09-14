package io.vectorsync.searchservice;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@SpringBootApplication
public class SearchServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SearchServiceApplication.class, args);
    }

    /**
     * Bounded timeouts. {@code new RestTemplate()} has neither a connect nor a read timeout, and
     * {@code embedding.external.timeout-ms} was configured but read by nothing, so a hung embedding
     * service held a request thread indefinitely. Generous rather than tight: the first call after
     * startup also loads the model.
     */
    @Bean
    public RestTemplate restTemplate(
            @Value("${embedding.external.connect-timeout-ms:5000}") long connectTimeoutMs,
            @Value("${embedding.external.timeout-ms:120000}") long readTimeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        factory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return new RestTemplate(factory);
    }
}
