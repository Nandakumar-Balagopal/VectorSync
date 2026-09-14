package io.vectorsync.controlplane.controller;

import io.vectorsync.controlplane.model.MaterializationEntity;
import io.vectorsync.controlplane.model.MaterializationEntity.State;
import io.vectorsync.controlplane.service.AdmissionService;
import io.vectorsync.controlplane.service.AdmissionService.AdmissionRequest;
import io.vectorsync.controlplane.service.AdmissionService.AdmissionResult;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * HTTP surface for derived embedding datasets.
 *
 * <p>Creation is an admission decision, not an insert. A rejected spec comes back as 422 with every
 * problem listed, because the caller is usually a form and a single error per round trip is a
 * conversation nobody finishes. A dry run returns the same verdict and cost estimate and writes
 * nothing, so "what would this cost" is answerable without committing to it.
 *
 * <p>All state changes go through {@link AdmissionService} so that they pass the one state machine.
 * There is deliberately no endpoint that sets an arbitrary state: that is how a retired dataset
 * gets pushed back into serving.
 */
@RestController
@RequestMapping("/api/materializations")
@Slf4j
public class MaterializationController {

    private final AdmissionService admissionService;

    public MaterializationController(AdmissionService admissionService) {
        this.admissionService = admissionService;
    }

    /**
     * Admits a materialization, or prices it without admitting when the body sets {@code dryRun}.
     *
     * <p>Returns 200 for a dry run and 201 for a real admission: a dry run created nothing, so
     * reporting 201 would be a lie a client could act on.
     */
    @PostMapping
    public ResponseEntity<?> create(@RequestBody AdmissionRequest request) {
        boolean dryRun = request != null && request.isDryRun();
        try {
            AdmissionResult result = admissionService.admit(request, dryRun);

            if (!result.isAdmitted()) {
                // 422, not 400: the request parsed and was understood, the spec it describes is the
                // thing that does not hold against the catalog.
                return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(result);
            }
            return ResponseEntity.status(dryRun ? HttpStatus.OK : HttpStatus.CREATED).body(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Admission conflict", "message", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Admission failed for {}: {}",
                    request == null ? null : request.getSourceTable(), e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Admission failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /** Lists materializations, optionally filtered to one lifecycle state. */
    @GetMapping
    public ResponseEntity<List<MaterializationEntity>> list(
            @RequestParam(value = "state", required = false) State state) {
        return ResponseEntity.ok(state == null
                ? admissionService.list()
                : admissionService.listByState(state));
    }

    /** State, watermarks, and freshness for one materialization. */
    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable("id") String id) {
        return admissionService.progress(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/pause")
    public ResponseEntity<?> pause(@PathVariable("id") String id) {
        return transition(id, () -> admissionService.pause(id));
    }

    /**
     * Resumes a paused materialization. The state it returns to is derived from how far it actually
     * got, not supplied by the caller.
     */
    @PostMapping("/{id}/resume")
    public ResponseEntity<?> resume(@PathVariable("id") String id) {
        return transition(id, () -> admissionService.resume(id));
    }

    /**
     * Retires a materialization. No vector is ever deleted by this call.
     *
     * <p>The embedding store is content-addressed and deduplicated across every materialization
     * that hashed to the same text, so "delete the vectors for this source table" is not a
     * well-formed operation: the rows it would remove are, by construction, the rows other datasets
     * are being served from. A retire that deleted them would silently corrupt those datasets and
     * the corruption would only show up as empty search results later.
     *
     * <p>{@code purge=true} therefore only marks this configuration's content-map rows as reclaim
     * candidates. Actual reclamation is a separate swept job: deciding that a content hash is
     * referenced by no live entry under any configuration needs a global view and a retention
     * window, which a single request does not have.
     */
    @PostMapping("/{id}/retire")
    public ResponseEntity<?> retire(@PathVariable("id") String id,
                                    @RequestParam(value = "purge", defaultValue = "false") boolean purge) {
        return transition(id, () -> admissionService.retire(id, purge));
    }

    private ResponseEntity<?> transition(String id, Supplier<Optional<MaterializationEntity>> action) {
        try {
            return action.get()
                    .<ResponseEntity<?>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (IllegalStateException e) {
            // An illegal transition is a conflict with the current state, not a malformed request.
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Illegal state transition",
                            "message", String.valueOf(e.getMessage())));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Lifecycle action failed for materialization {}: {}", id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Lifecycle action failed",
                            "message", String.valueOf(e.getMessage())));
        }
    }
}
