package io.vectorsync.worker.service.derive;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.ContentMapEntry;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import io.vectorsync.worker.service.iceberg.IncrementalChangeDetector;
import io.vectorsync.worker.service.iceberg.SourceFileWork;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Brings the content map back into agreement with a source table that had rows removed.
 *
 * <p>An incremental append scan cannot see a delete, so {@code assess()} refuses a snapshot range
 * containing one and parks the materialization. That refusal is correct -- advancing the watermark
 * past changes that were never materialized serves deleted rows indefinitely -- but it left the
 * capability missing. This is the missing half.
 *
 * <p><b>Why a key-set sweep rather than reading the delete.</b> The obvious approach is to find what
 * was deleted and tombstone exactly that, and it does not work on Iceberg. A positional delete and a
 * v3 deletion vector both carry {@code (file_path, position)} and no key values, so reading the
 * delete file tells you an offset, not a row id; resolving it means reading the data file at that
 * position, and that file may already be gone once {@code expire_snapshots} and
 * {@code remove_orphan_files} have run. Equality deletes do carry keys, but they are being retired
 * from the spec and cannot be relied on. So this asks the opposite question -- which keys does the
 * source still hold? -- which reads only current state and is therefore immune to retention policy
 * entirely.
 *
 * <p><b>Why it subsumes updates as well as deletes.</b> Tested directly in
 * {@code CurrentPipelineEndToEndTest}: the derive path already handles an update correctly, because
 * the content map keys by {@code source_row_id} and collapses by sequence number, so the new version
 * wins and the old one stops being live. What it cannot do is notice a row that vanished. A sweep
 * plus a re-derive therefore covers both: keys still present get a new content-map version from the
 * re-derive, keys absent get a tombstone from here. There is no separate update path and no
 * snapshot-counter heuristic -- counters cannot distinguish a rewrite from a swap, since both leave
 * the removed and added row counts equal.
 *
 * <p><b>Why the sweep is whole-table even when the reconcile is partition-scoped.</b>
 * {@code assess()} reports the partitions a destructive commit touched, and the re-derive half uses
 * them. The sweep cannot: the content map records {@code (source_table, source_row_id,
 * chunk_ordinal, config_id)} and <em>not</em> the source partition a row came from, so there is no
 * way to restrict the live set to the same partitions as the present set. Comparing a whole-table
 * live set against a partition-scoped present set would tombstone every row in every partition the
 * scan skipped. That is the one shape of this mechanism that silently destroys data, so the sweep
 * reads every key. It costs one projected scan of the key columns -- no vectors, no embedding -- and
 * that is the price of not storing the partition.
 *
 * <p><b>Scope safety.</b> The sweep tombstones by row id within {@code (source_table, config_id)},
 * and {@code config_id} deliberately excludes {@code keyColumns} -- so two specs over one table with
 * the same embedding configuration but different key columns would share a partition with two
 * different row-id spaces, and sweeping either would delete the other's rows. That cannot arise
 * through admission: {@code uk_materializations_source_config UNIQUE (source_table, config_id)}
 * forbids it. It can arise through {@code POST /api/derive/run}, which accepts an arbitrary spec,
 * which is why this service is driven from a materialization and is not exposed there.
 */
@Service
@Slf4j
public class ReconcileService {

    private final IcebergCatalogService catalogService;
    private final IncrementalChangeDetector detector;
    private final DeriveService deriveService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public ReconcileService(IcebergCatalogService catalogService,
                            IncrementalChangeDetector detector,
                            DeriveService deriveService) {
        this.catalogService = catalogService;
        this.detector = detector;
        this.deriveService = deriveService;
    }

    /**
     * @param keysPresent  distinct row ids the source still holds at the target snapshot
     * @param liveBefore   row ids the content map listed live before the sweep
     * @param tombstoned   row ids that vanished and were retired
     * @param complete     false when nothing was committed, so a caller must not advance a watermark
     */
    public record SweepResult(long keysPresent,
                              long liveBefore,
                              int tombstoned,
                              boolean complete,
                              String note) {
    }

    /**
     * Tombstones every mapped row the source no longer holds, as at {@code targetSnapshotId}.
     *
     * <p>Pinned to a caller-supplied snapshot rather than reading "current", because the sweep and
     * the re-derive that accompanies it must describe the same version of the source. Reading
     * current here would let a commit landing mid-pass tombstone rows that the re-derive never
     * looked at.
     */
    public SweepResult sweep(MaterializationSpec spec,
                             TableConfig config,
                             long targetSnapshotId,
                             long targetSequenceNumber,
                             long committedAtMillis) {
        Table contentMap = ContentMap.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (contentMap == null) {
            // Nothing has been materialized, so nothing can have vanished. Distinguished from a
            // successful empty sweep because a caller that advances a watermark here would be
            // advancing past a state it never derived.
            return new SweepResult(0, 0, 0, false, "no content map; nothing has been derived yet");
        }

        Set<String> liveRowIds = new LinkedHashSet<>();
        for (ContentMapEntry entry :
                ContentMap.liveEntries(contentMap, spec.getSourceTable(), spec.configId())) {
            liveRowIds.add(entry.getSourceRowId());
        }
        if (liveRowIds.isEmpty()) {
            return new SweepResult(0, 0, 0, true, "nothing mapped for this scope");
        }

        Set<String> present = presentRowIds(spec, config, targetSnapshotId);

        List<String> vanished = new ArrayList<>();
        for (String rowId : liveRowIds) {
            if (!present.contains(rowId)) {
                vanished.add(rowId);
            }
        }

        if (vanished.isEmpty()) {
            return new SweepResult(present.size(), liveRowIds.size(), 0, true,
                    "every mapped row is still present in the source");
        }

        // Refusing to tombstone everything. An empty present-set means the key projection read
        // nothing -- a renamed key column, an unreadable snapshot, a spec pointed at the wrong
        // table -- far more often than it means a genuinely emptied table, and the cost of being
        // wrong is retiring an entire materialization's serving data. An operator who really did
        // empty the table can retire the materialization explicitly.
        if (present.isEmpty()) {
            return new SweepResult(0, liveRowIds.size(), 0, false,
                    "the source returned no keys at all, which would retire every mapped row; "
                            + "refusing. Check the key columns resolve at this snapshot, or retire "
                            + "the materialization explicitly if the table really is empty.");
        }

        DeriveResult result = deriveService.tombstone(
                spec, vanished, targetSnapshotId, targetSequenceNumber, committedAtMillis);
        if (!result.complete()) {
            return new SweepResult(present.size(), liveRowIds.size(), 0, false,
                    "tombstoning failed; the content map is unchanged");
        }

        log.info("Reconciled {} / {}: {} of {} mapped rows no longer exist in the source and were "
                        + "tombstoned at sequence {}",
                spec.getSourceTable(), spec.configId(), vanished.size(), liveRowIds.size(),
                targetSequenceNumber);
        return new SweepResult(present.size(), liveRowIds.size(), vanished.size(), true,
                vanished.size() + " rows tombstoned");
    }

    /**
     * Distinct row ids the source holds at a snapshot, read from the key columns only.
     *
     * <p>Projected to the key columns, so this reads identity and never an embedded value: the cost
     * is one narrow scan of the table rather than anything proportional to the text being embedded.
     * That is what makes a whole-table sweep affordable.
     */
    private Set<String> presentRowIds(MaterializationSpec spec, TableConfig config, long snapshotId) {
        Set<String> present = new LinkedHashSet<>();
        List<String> keyColumns = spec.getKeyColumns();

        for (SourceFileWork file : detector.backfillWork(config, snapshotId)) {
            List<Record> rows = detector.readFile(config, file, keyColumns);
            for (Record row : rows) {
                present.add(deriveService.rowIdOf(spec, row));
            }
        }
        return present;
    }
}
