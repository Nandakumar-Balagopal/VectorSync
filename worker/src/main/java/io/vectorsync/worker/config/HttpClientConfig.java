package io.vectorsync.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * HTTP clients with bounded timeouts.
 *
 * <p>Every {@link RestTemplate} in this service used to be constructed with {@code new
 * RestTemplate()}, which has no connect or read timeout at all. {@code embedding.external.timeout-ms}
 * was configured and never read by anything. The consequence was specific: the sync scheduler runs
 * these calls on its own thread, so one unresponsive embedding service or control plane wedged the
 * scheduler permanently -- no timeout, no failure, no next cycle, and nothing in the logs.
 *
 * <p>The read timeout is deliberately generous rather than tight. A warm 64-text batch on
 * all-mpnet-base-v2 measures about 1s, but the first request after startup also loads the model,
 * which is far slower. The purpose here is to guarantee the thread is eventually released, not to
 * enforce a latency budget.
 */
@Configuration
public class HttpClientConfig {

    public static final String EMBEDDING_CLIENT = "embeddingRestTemplate";
    public static final String CONTROL_API_CLIENT = "controlApiRestTemplate";

    @Bean(EMBEDDING_CLIENT)
    public RestTemplate embeddingRestTemplate(
            @Value("${embedding.external.connect-timeout-ms:5000}") long connectTimeoutMs,
            @Value("${embedding.external.timeout-ms:120000}") long readTimeoutMs) {
        return build(connectTimeoutMs, readTimeoutMs);
    }

    /**
     * The control-plane client, carrying the worker's credential when authentication is enabled.
     *
     * <p>Attached here rather than at each call site because there are thirteen outbound calls
     * across two clients, and a credential added per-call is a credential that will be forgotten on
     * the fourteenth. The interceptor is added only when a password is configured, so the
     * unauthenticated default sends no Authorization header at all rather than an empty one.
     */
    @Bean(CONTROL_API_CLIENT)
    public RestTemplate controlApiRestTemplate(
            @Value("${control.api.connect-timeout-ms:5000}") long connectTimeoutMs,
            @Value("${control.api.timeout-ms:15000}") long readTimeoutMs,
            @Value("${vectorsync.auth.worker.username:vectorsync-worker}") String username,
            @Value("${vectorsync.auth.worker.password:}") String password) {
        RestTemplate client = build(connectTimeoutMs, readTimeoutMs);
        if (password != null && !password.isBlank()) {
            client.getInterceptors().add(
                    new org.springframework.http.client.support.BasicAuthenticationInterceptor(
                            username, password));
        }
        return client;
    }

    private static RestTemplate build(long connectTimeoutMs, long readTimeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        factory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return new RestTemplate(factory);
    }
}
