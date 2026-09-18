package io.vectorsync.controlplane.repository;

import io.vectorsync.controlplane.model.MaterializationEntity;
import io.vectorsync.controlplane.model.MaterializationEntity.State;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface MaterializationRepository extends JpaRepository<MaterializationEntity, String> {

    /**
     * Identity lookup. A materialization is what its spec produces, so
     * {@code (source_table, config_id)} is the natural key -- the surrogate id exists only to give
     * REST a stable path. Admission uses this to reject a duplicate spec instead of standing up a
     * second scheduler over the same content-map partition.
     */
    Optional<MaterializationEntity> findBySourceTableAndConfigId(String sourceTable, String configId);

    boolean existsBySourceTableAndConfigId(String sourceTable, String configId);

    /**
     * All materializations of one source table, including the candidate configurations of an
     * in-flight model migration, which coexist with the live one by design.
     */
    List<MaterializationEntity> findBySourceTable(String sourceTable);

    List<MaterializationEntity> findByState(State state);

    /**
     * Scheduling order for the worker: highest priority first, then longest-untouched, so a
     * starved dataset eventually wins against a busy same-priority peer.
     */
    List<MaterializationEntity> findByStateInOrderByPriorityDescUpdatedAtAsc(Collection<State> states);

    /** Retired datasets whose content-map rows an operator has marked reclaimable. */
    List<MaterializationEntity> findByStateAndPurgeEligibleTrue(State state);

    /**
     * The materialization currently serving a source table.
     *
     * <p>Returns a list rather than an Optional although a partial unique index allows at most one:
     * a repository method that assumed uniqueness would throw on a database whose index was dropped,
     * and promotion is the one place that should notice and repair that rather than fail.
     */
    List<MaterializationEntity> findBySourceTableAndServingTrue(String sourceTable);
}
