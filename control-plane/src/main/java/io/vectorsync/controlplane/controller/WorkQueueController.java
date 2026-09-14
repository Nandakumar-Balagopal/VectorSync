package io.vectorsync.controlplane.controller;

import io.vectorsync.controlplane.model.WorkItemEntity;
import io.vectorsync.controlplane.service.WorkQueueService;
import io.vectorsync.controlplane.service.WorkQueueService.QueueStats;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Worker-facing endpoints for the materialization queue.
 *
 * <p>Workers hold no queue state of their own: they lease, do the work, and report. Everything that
 * decides what runs next lives in Postgres, so scaling the pool is starting another process and a
 * worker dying is a lease that expires.
 *
 * <p>Enqueue is deliberately absent. Work descriptors come from planning a source snapshot, which
 * happens inside the control plane where the Iceberg metadata already is; exposing enqueue would
 * invite a client to invent work that no snapshot backs.
 *
 * <p>Error mapping is uniform: an unknown or malformed id is a 404 rather than a 500, because a
 * worker retrying a callback for an item that was pruned must be able to tell "gone" from "the
 * control plane is broken" and stop retrying.
 */
@RestController
@RequestMapping("/api/queue")
@Slf4j
public class WorkQueueController {

    private final WorkQueueService workQueueService;

    public WorkQueueController(WorkQueueService workQueueService) {
        this.workQueueService = workQueueService;
    }

    /**
     * Enqueues planned work for a materialization.
     *
     * <p>Planning happens in the worker, because only the data plane talks to the source catalog;
     * the queue is the control plane's, because only it has durable state. So the file list crosses
     * the boundary here. Idempotent on (materializationId, dataFilePath, snapshotId), which is what
     * makes a replan after a crashed planner safe to repeat.
     */
    @PostMapping("/enqueue")
    public ResponseEntity<?> enqueue(@RequestBody EnqueueRequest request) {
        if (request == null || request.getMaterializationId() == null
                || request.getMaterializationId().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "materializationId is required"));
        }

        try {
            int inserted = workQueueService.enqueue(
                    request.getMaterializationId(),
                    request.getDescriptors() == null ? List.of() : request.getDescriptors());
            int submitted = request.getDescriptors() == null ? 0 : request.getDescriptors().size();
            return ResponseEntity.ok(Map.of(
                    "materializationId", request.getMaterializationId(),
                    "submitted", submitted,
                    "inserted", inserted,
                    "alreadyQueued", submitted - inserted));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @lombok.Data
    public static class EnqueueRequest {
        private String materializationId;
        private List<WorkQueueService.WorkDescriptor> descriptors;
    }

    /**
     * Leases up to {@code limit} items to a worker.
     *
     * <p>POST, not GET, because it mutates: each call transfers ownership of rows. A cached or
     * prefetched GET would silently lease work to nobody.
     */
    @PostMapping("/lease")
    public ResponseEntity<?> lease(@RequestBody LeaseRequest request) {
        if (request == null || request.getOwner() == null || request.getOwner().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "owner is required"));
        }

        try {
            Duration leaseDuration = request.getLeaseSeconds() == null || request.getLeaseSeconds() <= 0
                    ? WorkQueueService.DEFAULT_LEASE_DURATION
                    : Duration.ofSeconds(request.getLeaseSeconds());
            int limit = request.getLimit() == null ? 1 : request.getLimit();

            List<WorkItemEntity> leased = workQueueService.lease(
                    request.getOwner(), limit, leaseDuration, request.getMaterializationId());
            return ResponseEntity.ok(leased);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Lease failed for owner {}: {}", request.getOwner(), e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Lease failed", "message", String.valueOf(e.getMessage())));
        }
    }

    @PostMapping("/{id}/complete")
    public ResponseEntity<?> complete(@PathVariable("id") String id) {
        try {
            workQueueService.complete(id);
            return ResponseEntity.ok(Map.of("id", id, "state", WorkItemEntity.State.DONE.name()));
        } catch (IllegalArgumentException e) {
            log.warn("Completion for unknown work item {}", id);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Unknown work item", "id", id));
        } catch (Exception e) {
            log.error("Failed to complete work item {}: {}", id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Complete failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /**
     * Reports a failed attempt. The response carries the resulting state so the worker learns
     * whether the item was requeued or retired without polling for it.
     *
     * <p>A worker should send back the {@code owner} it leased under. Without it the report cannot
     * be tied to a lease generation, so a report that arrives after the lease was reclaimed and the
     * item re-leased spends the new holder's retry budget.
     */
    @PostMapping("/{id}/fail")
    public ResponseEntity<?> fail(@PathVariable("id") String id, @RequestBody(required = false) FailRequest request) {
        try {
            workQueueService.fail(id,
                    request == null ? null : request.getOwner(),
                    request == null ? null : request.getError());
            return workQueueService.find(id)
                    .<ResponseEntity<?>>map(item -> ResponseEntity.ok(Map.of(
                            "id", item.getId(),
                            "state", item.getState().name(),
                            "attempts", item.getAttempts(),
                            "maxAttempts", item.getMaxAttempts())))
                    .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(Map.of("error", "Unknown work item", "id", id)));
        } catch (IllegalArgumentException e) {
            log.warn("Failure report for unknown work item {}", id);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Unknown work item", "id", id));
        } catch (Exception e) {
            log.error("Failed to record failure for work item {}: {}", id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Fail failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /**
     * Returns expired leases to the queue.
     *
     * <p>Exposed as an endpoint as well as being scheduler-driven so an operator can unstick a
     * queue immediately after a worker pool crash instead of waiting for the next tick.
     * Idempotent: a second call with nothing expired reclaims zero.
     */
    @PostMapping("/reclaim")
    public ResponseEntity<?> reclaim() {
        try {
            int reclaimed = workQueueService.reclaimExpiredLeases();
            return ResponseEntity.ok(Map.of("reclaimed", reclaimed));
        } catch (Exception e) {
            log.error("Failed to reclaim expired leases: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Reclaim failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /** Queue depth, scoped to one materialization when asked and global otherwise. */
    @GetMapping("/stats")
    public ResponseEntity<?> stats(
            @RequestParam(value = "materializationId", required = false) String materializationId) {
        try {
            QueueStats stats = workQueueService.stats(materializationId);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Failed to read queue stats for {}: {}", materializationId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Stats failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /**
     * Lease request body.
     *
     * <p>{@code owner} must identify the process uniquely (host plus pid, or the pod name) -- it is
     * the only thing that distinguishes "my lease" from "someone else's" in the logs when a
     * reclaim storm has to be explained. Boxed fields so an omitted value is distinguishable from
     * a deliberate zero and can take the server default.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class LeaseRequest {
        private String owner;
        private Integer limit;
        /** Should exceed the worker's expected time to process {@code limit} files. */
        private Integer leaseSeconds;
        /** Restricts the lease to one materialization. Null leases from the global head. */
        private String materializationId;
    }

    /**
     * Failure report body.
     *
     * <p>{@code owner} is the value the item was leased under; it fences the report against a lease
     * the reporter no longer holds. Optional so an older worker still reports failures, at the cost
     * of the fencing.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class FailRequest {
        private String owner;
        private String error;
    }
}
