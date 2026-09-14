package io.vectorsync.searchservice.service.iceberg;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.index.IndexVector;
import io.vectorsync.format.vector.VectorRecordCodec;
import io.vectorsync.format.vector.VectorResolution;
import io.vectorsync.format.vector.VectorTableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Reads the vector table.
 *
 * <p>Every query here pushes its predicate into Iceberg rather than filtering in Java afterwards.
 * That is the entire reason the table is partitioned by {@code (source_table, model_version)}: with
 * an equality predicate on those columns Iceberg prunes whole files using partition metadata and
 * never opens them. Filtering after a full scan makes the partitioning decorative, and on a table
 * holding every version of every embedding it is the difference between reading one partition and
 * reading the warehouse.
 *
 * <p>Queries that only need scalar metadata also project their columns. A vector row is dominated by
 * the embedding array, so asking for one {@code long} per row without projection deserializes
 * hundreds of floats per row to discard them.
 */
@Service
@Slf4j
public class VectorSyncReader {

    /**
     * Columns {@link VectorResolution} needs to decide which rows are live: the resolution key, the
     * ordering fields, and the tombstone flag. Deliberately excludes the embedding and the text,
     * which is the point.
     */
    private static final String[] RESOLUTION_COLUMNS = {
            Constants.SOURCE_TABLE_COLUMN,
            Constants.SOURCE_ROW_ID_COLUMN,
            Constants.CHUNK_ORDINAL_COLUMN,
            Constants.EMBEDDING_MODEL_COLUMN,
            Constants.EMBEDDING_VERSION_COLUMN,
            Constants.MODEL_VERSION_COLUMN,
            Constants.DELETED_COLUMN,
            Constants.SOURCE_SEQUENCE_NUMBER_COLUMN,
            Constants.SOURCE_COMMITTED_AT_COLUMN,
            Constants.CREATED_AT_COLUMN,
    };

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public VectorSyncReader(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public List<VectorRecord> readAllVectors() {
        return VectorResolution.latestLiveVectors(readRaw());
    }

    /**
     * The live vectors an index should cover: one source table and one model version.
     *
     * <p>Scoping an index this way removes the need to filter search results by source table,
     * which the previous implementation did after top-k and so could under-return.
     */
    public List<VectorRecord> readForIndex(String sourceTable, String modelVersion) {
        // Resolution happens after the pushdown, which is safe because the resolution key includes
        // both partition columns: every tombstone that could retire a row in this partition also
        // lives in this partition.
        return VectorResolution.latestLiveVectors(
                        read(partition(sourceTable, modelVersion), VectorRecordCodec::fromIcebergRecord))
                .stream()
                .filter(vector -> vector.getEmbedding() != null && !vector.getEmbedding().isEmpty())
                .toList();
    }

    /**
     * The same live set as {@link #readForIndex}, in the compact form an index build consumes.
     *
     * <p>Resolution runs first over projected metadata, so the embedding of a superseded or
     * tombstoned row is never read at all. The surviving rows are then converted to {@code float[]}
     * as each Iceberg row is decoded, which lets the boxed values be collected per row instead of
     * being retained for the entire build -- retention, not allocation, is what capped index size.
     */
    public List<IndexVector> readForIndexBuild(String sourceTable, String modelVersion) {
        Set<String> live = liveVectorIds(sourceTable, modelVersion);
        if (live.isEmpty()) {
            return List.of();
        }

        return read(partition(sourceTable, modelVersion), record -> toIndexVector(record, live)).stream()
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    /**
     * A deterministic sample of live vectors, read without materializing the population.
     *
     * <p>Index-recall probing needs a handful of vectors out of a partition that may hold millions.
     * Resolution runs over projected metadata to find the live ids, the sample is taken from that id
     * list, and only the sampled rows have their embeddings read. Sampling is evenly spaced over
     * sorted ids rather than random so the same index yields the same probes on every run, which is
     * what lets the resulting score be compared against a threshold.
     */
    public List<VectorRecord> sampleLiveVectors(String sourceTable, String modelVersion, int count) {
        List<String> ids = new ArrayList<>(liveVectorIds(sourceTable, modelVersion));
        if (ids.isEmpty() || count <= 0) {
            return List.of();
        }
        Collections.sort(ids);

        List<String> sampled;
        if (ids.size() <= count) {
            sampled = ids;
        } else {
            sampled = new ArrayList<>(count);
            double stride = (double) ids.size() / count;
            for (int i = 0; i < count; i++) {
                sampled.add(ids.get((int) (i * stride)));
            }
        }

        List<VectorRecord> probes = read(
                Expressions.and(
                        partition(sourceTable, modelVersion),
                        Expressions.in(Constants.VECTOR_ID_COLUMN, sampled)),
                VectorRecordCodec::fromIcebergRecord);

        // Restore the sampled order; the scan returns rows in file order.
        Map<String, VectorRecord> byId = new java.util.HashMap<>();
        probes.forEach(probe -> byId.put(probe.getVectorId(), probe));
        return sampled.stream().map(byId::get).filter(java.util.Objects::nonNull).toList();
    }

    /** As {@link #readForIndexBuild}, restricted to the state at a source sequence number. */
    public List<IndexVector> readForIndexBuildAsOf(String sourceTable,
                                                   String modelVersion,
                                                   long sourceSequenceNumber) {
        Set<String> live = liveVectorIdsAsOf(sourceTable, modelVersion, sourceSequenceNumber);
        if (live.isEmpty()) {
            return List.of();
        }

        return read(partition(sourceTable, modelVersion), record -> toIndexVector(record, live)).stream()
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    private Set<String> liveVectorIdsAsOf(String sourceTable, String modelVersion, long asOf) {
        return VectorResolution.liveVectorsAsOf(resolvableRows(sourceTable, modelVersion), asOf).stream()
                .map(VectorRecord::getVectorId)
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * Vector ids that survive resolution, read without touching the embedding column.
     *
     * <p>Resolution needs the full history for a key to decide which row wins, so the metadata pass
     * cannot be avoided -- but it can be made to cost metadata rather than vectors.
     */
    private Set<String> liveVectorIds(String sourceTable, String modelVersion) {
        return VectorResolution.latestLiveVectors(resolvableRows(sourceTable, modelVersion)).stream()
                .map(VectorRecord::getVectorId)
                .collect(java.util.stream.Collectors.toSet());
    }

    private List<VectorRecord> resolvableRows(String sourceTable, String modelVersion) {
        return readProjected(
                partition(sourceTable, modelVersion),
                record -> {
                    VectorRecord vector = toResolutionRecord(record);
                    vector.setVectorId(asString(record, Constants.VECTOR_ID_COLUMN));
                    return vector;
                },
                withVectorId(RESOLUTION_COLUMNS));
    }

    private static IndexVector toIndexVector(Record record, Set<String> live) {
        String vectorId = asString(record, Constants.VECTOR_ID_COLUMN);
        if (vectorId == null || !live.contains(vectorId)) {
            return null;
        }

        float[] embedding = toFloatArray(field(record, Constants.EMBEDDING_COLUMN));
        if (embedding.length == 0) {
            return null;
        }

        return new IndexVector(
                vectorId,
                asString(record, Constants.SOURCE_TABLE_COLUMN),
                asString(record, Constants.SOURCE_ROW_ID_COLUMN),
                asString(record, Constants.TEXT_COLUMN),
                asString(record, Constants.MODEL_VERSION_COLUMN),
                asLong(record, Constants.SOURCE_SNAPSHOT_ID_COLUMN),
                asLong(record, Constants.SOURCE_SEQUENCE_NUMBER_COLUMN),
                embedding);
    }

    /** The Iceberg column is float32, so this narrows nothing that was ever meaningful. */
    private static float[] toFloatArray(Object value) {
        if (!(value instanceof List<?> list)) {
            return new float[0];
        }

        float[] embedding = new float[list.size()];
        int position = 0;
        for (Object element : list) {
            embedding[position++] = element instanceof Number number ? number.floatValue() : 0f;
        }
        return embedding;
    }

    private static String[] withVectorId(String[] columns) {
        String[] extended = java.util.Arrays.copyOf(columns, columns.length + 1);
        extended[columns.length] = Constants.VECTOR_ID_COLUMN;
        return extended;
    }

    /** Live vectors as they stood at a source sequence number, for a reproducible rebuild. */
    public List<VectorRecord> readForIndexAsOf(String sourceTable, String modelVersion, long sourceSequenceNumber) {
        return VectorResolution.liveVectorsAsOf(
                        read(partition(sourceTable, modelVersion), VectorRecordCodec::fromIcebergRecord),
                        sourceSequenceNumber)
                .stream()
                .filter(vector -> vector.getEmbedding() != null && !vector.getEmbedding().isEmpty())
                .toList();
    }

    /** Highest source snapshot observed for a table, i.e. how current its embeddings are. */
    public long latestSourceSnapshot(String sourceTable) {
        return readProjected(
                        Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable),
                        record -> new long[]{
                                asLong(record, Constants.SOURCE_SEQUENCE_NUMBER_COLUMN),
                                asLong(record, Constants.SOURCE_SNAPSHOT_ID_COLUMN)},
                        Constants.SOURCE_SEQUENCE_NUMBER_COLUMN,
                        Constants.SOURCE_SNAPSHOT_ID_COLUMN)
                .stream()
                .max(Comparator.comparingLong(pair -> pair[0]))
                .map(pair -> pair[1])
                .orElse(0L);
    }

    /**
     * Highest source sequence number observed for a table. Orders history; snapshot ids do not.
     *
     * <p>Called on every index-status request to decide whether an index is stale, so it reads one
     * projected column out of one partition set rather than the whole table.
     */
    public long latestSourceSequenceNumber(String sourceTable) {
        return readProjected(
                        Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable),
                        record -> asLong(record, Constants.SOURCE_SEQUENCE_NUMBER_COLUMN),
                        Constants.SOURCE_SEQUENCE_NUMBER_COLUMN)
                .stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
    }

    /** Distinct model versions materialized for a table, excluding versions fully tombstoned. */
    public List<String> modelVersionsFor(String sourceTable) {
        List<VectorRecord> resolvable = readProjected(
                Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable),
                VectorSyncReader::toResolutionRecord,
                RESOLUTION_COLUMNS);

        return VectorResolution.latestLiveVectors(resolvable).stream()
                .map(VectorRecord::modelVersion)
                .distinct()
                .sorted()
                .toList();
    }

    /** Full append-only history, before resolution. Unfiltered; prefer a scoped read. */
    public List<VectorRecord> readRaw() {
        return read(null, VectorRecordCodec::fromIcebergRecord);
    }

    /** Full history for one source table, before resolution. */
    public List<VectorRecord> readRawForTable(String sourceTable) {
        return read(Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable),
                VectorRecordCodec::fromIcebergRecord);
    }

    /** Full history for one vector id. Used by provenance, which knows exactly what it wants. */
    public List<VectorRecord> readRawForVectorId(String vectorId) {
        return read(Expressions.equal(Constants.VECTOR_ID_COLUMN, vectorId),
                VectorRecordCodec::fromIcebergRecord);
    }

    private static Expression partition(String sourceTable, String modelVersion) {
        return Expressions.and(
                Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable),
                Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion));
    }

    private <T> List<T> read(Expression filter, Function<Record, T> mapper) {
        return readProjected(filter, mapper);
    }

    private <T> List<T> readProjected(Expression filter, Function<Record, T> mapper, String... columns) {
        Table table = VectorTableSchema.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (table == null) {
            return List.of();
        }

        IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(table);
        if (filter != null) {
            scan = scan.where(filter);
        }
        if (columns.length > 0) {
            scan = scan.select(columns);
        }

        List<T> results = new ArrayList<>();
        try (CloseableIterable<Record> rows = scan.build()) {
            for (Record row : rows) {
                try {
                    results.add(mapper.apply(row));
                } catch (Exception e) {
                    log.warn("Skipping unreadable vector row: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read vectors from Iceberg", e);
        }

        return results;
    }

    /**
     * Builds a {@link VectorRecord} carrying only {@link #RESOLUTION_COLUMNS}. The embedding and
     * text are left null on purpose: callers of this path are deciding which rows are live, not
     * reading their contents.
     */
    private static VectorRecord toResolutionRecord(Record record) {
        VectorRecord vector = VectorRecord.builder()
                .sourceTable(asString(record, Constants.SOURCE_TABLE_COLUMN))
                .sourceRowId(asString(record, Constants.SOURCE_ROW_ID_COLUMN))
                .chunkOrdinal((int) asLong(record, Constants.CHUNK_ORDINAL_COLUMN))
                .embeddingModel(asString(record, Constants.EMBEDDING_MODEL_COLUMN))
                .embeddingVersion(asString(record, Constants.EMBEDDING_VERSION_COLUMN))
                .sourceSequenceNumber(asLong(record, Constants.SOURCE_SEQUENCE_NUMBER_COLUMN))
                .sourceCommittedAtMillis(asLong(record, Constants.SOURCE_COMMITTED_AT_COLUMN))
                .deleted(Boolean.TRUE.equals(field(record, Constants.DELETED_COLUMN)))
                .build();

        Object createdAt = field(record, Constants.CREATED_AT_COLUMN);
        if (createdAt instanceof java.time.OffsetDateTime offset) {
            vector.setCreatedAt(offset.toInstant());
        } else if (createdAt instanceof Instant instant) {
            vector.setCreatedAt(instant);
        }
        return vector;
    }

    /** Null rather than an exception when a column was not projected. */
    private static Object field(Record record, String name) {
        Types.NestedField declared = record.struct().field(name);
        return declared == null ? null : record.getField(name);
    }

    private static String asString(Record record, String name) {
        Object value = field(record, name);
        return value == null ? null : String.valueOf(value);
    }

    private static long asLong(Record record, String name) {
        Object value = field(record, name);
        return value instanceof Number number ? number.longValue() : 0L;
    }
}
