package io.vectorsync.controlplane.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;

/**
 * HTTP authentication for this service.
 *
 * <p><b>Off by default</b>, enabled with {@code vectorsync.auth.enabled=true}. That default is a
 * deliberate and temporary compromise, not a recommendation. Turning it on without also updating
 * the dashboard's proxy, the five scripts under {@code deployment/} and the benchmark clients under
 * {@code bench/} breaks every one of them -- and those scripts produced every number in the docs.
 * Shipping the mechanism disabled makes enabling it one property rather than a rewrite, and keeps
 * the repository honest meanwhile: {@code SECURITY.md} states the deployed posture is
 * trusted-network-only.
 *
 * <p>When enabled the shape is deny-by-default with HTTP Basic, and two accounts rather than one.
 * The worker authenticating to the control plane machine-to-machine wants a different credential
 * from a human or a script driving the admin surface; sharing one means rotating the operator
 * password stops derivation.
 *
 * <p>Basic over TLS, chosen over an OAuth2 resource server because a half-built token flow is worse
 * than a simple mechanism that is correct, and over mTLS because nothing here distributes
 * certificates. What it does not protect against, stated plainly: it is one shared secret per role,
 * there is no per-caller attribution beyond the two role names, and over plain HTTP the credential
 * is recoverable from the wire. The last is the deployer's responsibility, which is why TLS
 * termination in front is documented as a requirement rather than an option.
 *
 * <p>The principal is deliberately not the lease owner. Lease ownership identifies a <em>process</em>
 * so the queue fence can tell two workers apart; an auth principal identifies a <em>role</em> and is
 * shared by every replica. Conflating them would make the fence inert as soon as a second worker was
 * deployed -- a bug this project has already had once, when every replica called itself worker-1.
 */
@Configuration
public class HttpAuthConfig {

    /**
     * Paths reachable without a credential.
     *
     * <p>Liveness only. {@code /actuator/metrics} is deliberately absent: the derivation counters
     * are tagged with customer table names and configuration ids, which is not something to serve
     * anonymously.
     */
    private static final String[] ANONYMOUS = {"/actuator/health", "/actuator/health/**"};

    @Value("${vectorsync.auth.enabled:false}")
    private boolean enabled;

    @Value("${vectorsync.auth.admin.username:vectorsync-admin}")
    private String adminUsername;

    @Value("${vectorsync.auth.admin.password:}")
    private String adminPassword;

    @Value("${vectorsync.auth.worker.username:vectorsync-worker}")
    private String workerUsername;

    @Value("${vectorsync.auth.worker.password:}")
    private String workerPassword;

    /**
     * Refuses to start with authentication enabled and no real credential.
     *
     * <p>The failure this prevents is a deployment that believes it is authenticated and is not. A
     * committed default password would be worse than none, because every guard would read as
     * satisfied -- and {@code scripts/start-local.sh} copies {@code .env.host.example} and sources
     * it, so a placeholder there becomes the live credential on a fresh clone without anyone typing
     * it. Hence no default, and known placeholders rejected by name rather than only by length.
     */
    private String require(String role, String value) {
        String upper = role.toUpperCase();
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "vectorsync.auth.enabled is true but no " + role + " password is set. Set "
                            + "VECTORSYNC_AUTH_" + upper + "_PASSWORD to a real secret, or set "
                            + "vectorsync.auth.enabled=false to run on a trusted network.");
        }
        String normalised = value.trim().toLowerCase();
        if (normalised.startsWith("change_me") || normalised.startsWith("change-me")
                || normalised.equals("changeme") || normalised.equals("password")
                || normalised.startsWith("insert")) {
            throw new IllegalStateException(
                    "the " + role + " password is still a placeholder. A committed placeholder is "
                            + "worse than no password, because every other guard reads as satisfied.");
        }
        if (value.length() < 16) {
            throw new IllegalStateException(
                    "the " + role + " password is " + value.length()
                            + " characters; at least 16 are required.");
        }
        return value;
    }

    @Bean
    UserDetailsService vectorsyncUsers() {
        if (!enabled) {
            // No accounts exist when disabled, so a half-finished configuration cannot
            // accidentally authenticate anyone.
            return new InMemoryUserDetailsManager();
        }
        return new InMemoryUserDetailsManager(
                User.withUsername(adminUsername)
                        .password("{noop}" + require("admin", adminPassword))
                        .roles("ADMIN")
                        .build(),
                User.withUsername(workerUsername)
                        .password("{noop}" + require("worker", workerPassword))
                        .roles("WORKER")
                        .build());
    }

    @Bean
    SecurityFilterChain vectorsyncFilterChain(HttpSecurity http) throws Exception {
        if (!enabled) {
            return http.csrf(csrf -> csrf.disable())
                    .authorizeHttpRequests(requests -> requests.anyRequest().permitAll())
                    .build();
        }

        return http
                // Stateless API with no browser session and no login form, so there is no session
                // for a forged cross-site request to ride, and leaving CSRF on would reject every
                // non-GET from the worker and the scripts.
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers(ANONYMOUS).permitAll()
                        .anyRequest().authenticated())
                .httpBasic(basic -> basic
                        // 401 with no WWW-Authenticate. Every caller here sends Basic
                        // preemptively, and advertising the challenge only invites a browser to
                        // cache the credential and replay it cross-site.
                        .authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)))
                .build();
    }
}
