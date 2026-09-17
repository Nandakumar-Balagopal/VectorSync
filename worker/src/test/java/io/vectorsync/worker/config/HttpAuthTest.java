package io.vectorsync.worker.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The HTTP surface's authentication, in both configurations it ships in.
 *
 * <p>Authentication is off by default here, and that is worth testing rather than assuming: an
 * always-on default would break the dashboard proxy, the five scripts under {@code deployment/} and
 * the benchmark clients under {@code bench/} -- the scripts that produced every measurement in the
 * docs. So the disabled case is the shipped case and has to keep working, while the enabled case has
 * to actually deny.
 *
 * <p>The asymmetry in the allowlist is the part most likely to be got wrong later.
 * {@code /actuator/health} must stay anonymous or container orchestration cannot probe the service;
 * {@code /actuator/metrics} must not, because the derivation counters are tagged with source table
 * names and configuration ids.
 */
class HttpAuthTest {

    private static final String[] BASE = {
            "embedding.provider=mock",
            "iceberg.vector.namespace=vector",
            "vectorsync.runner.enabled=false",
            "vectorsync.legacy-sync.enabled=false",
    };

    @SpringBootTest(properties = {
            "embedding.provider=mock",
            "iceberg.vector.namespace=vector",
            "vectorsync.runner.enabled=false",
            "vectorsync.legacy-sync.enabled=false",
    })
    @AutoConfigureMockMvc
    @Nested
    @DisplayName("with the shipped default (authentication disabled)")
    class Disabled {

        @DynamicPropertySource
        static void properties(DynamicPropertyRegistry registry) throws IOException {
            Path warehouse = Files.createTempDirectory("vectorsync-auth-off-");
            registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
        }

        @Autowired
        MockMvc mvc;

        @MockitoBean
        io.vectorsync.worker.client.DerivationControlClient control;

        @Test
        @DisplayName("the API is reachable without a credential")
        void apiIsOpen() throws Exception {
            // Adding spring-boot-starter-security to the classpath secures everything by default,
            // so without the explicit permitAll chain this returns 401 and every script in the
            // repository breaks on upgrade. That is the regression this asserts against.
            mvc.perform(get("/api/vectors/health")).andExpect(status().isOk());
        }

        @Test
        @DisplayName("health is reachable, so orchestration can still probe it")
        void healthIsOpen() throws Exception {
            mvc.perform(get("/actuator/health")).andExpect(status().isOk());
        }
    }

    @SpringBootTest(properties = {
            "embedding.provider=mock",
            "iceberg.vector.namespace=vector",
            "vectorsync.runner.enabled=false",
            "vectorsync.legacy-sync.enabled=false",
            "vectorsync.auth.enabled=true",
            "vectorsync.auth.admin.password=a-real-admin-secret-value",
            "vectorsync.auth.worker.password=a-real-worker-secret-value",
    })
    @AutoConfigureMockMvc
    @Nested
    @DisplayName("with authentication enabled")
    class Enabled {

        @DynamicPropertySource
        static void properties(DynamicPropertyRegistry registry) throws IOException {
            Path warehouse = Files.createTempDirectory("vectorsync-auth-on-");
            registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
        }

        @Autowired
        MockMvc mvc;

        @MockitoBean
        io.vectorsync.worker.client.DerivationControlClient control;

        @Test
        @DisplayName("an unauthenticated API call is refused")
        void apiIsDenied() throws Exception {
            mvc.perform(get("/api/vectors/health")).andExpect(status().isUnauthorized());
        }

        @Test
        @DisplayName("a valid credential is accepted")
        void credentialWorks() throws Exception {
            mvc.perform(get("/api/vectors/health")
                            .with(request -> {
                                request.addHeader("Authorization", "Basic " + java.util.Base64
                                        .getEncoder()
                                        .encodeToString(("vectorsync-admin:a-real-admin-secret-value")
                                                .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                                return request;
                            }))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("health stays anonymous but metrics does not")
        void allowlistIsAsymmetric() throws Exception {
            mvc.perform(get("/actuator/health")).andExpect(status().isOk());
            // Tagged with source table names and config ids, so not anonymous.
            mvc.perform(get("/actuator/metrics")).andExpect(status().isUnauthorized());
        }
    }
}
