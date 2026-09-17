package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ProjectionBuilder;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a few rows of a serving projection, for inspection rather than for serving.
 *
 * <p>Exists because the projection is the one artifact a query engine touches, and until now there
 * was no way to look at it without attaching an engine. Its value is demonstrative: each row carries
 * the lineage that makes the vector reproducible, and showing that is more convincing than asserting
 * it.
 *
 * <p>Not a query path. The limit is small and enforced, the embedding column is never projected, and
 * nothing here is on the hot path -- an engine reads the Iceberg table directly.
 */
@Service
@Slf4j
public class ProjectionReader {

    private static final List<String> LINEAGE_COLUMNS = List.of(
            Constants.SOURCE_ROW_ID_COLUMN,
            Constants.CHUNK_ORDINAL_COLUMN,
            Constants.CONTENT_HASH_COLUMN,
            Constants.MODEL_VERSION_COLUMN,
            Constants.CONFIG_ID_COLUMN,
            Constants.EMBEDDING_DIM_COLUMN,
            Constants.TEXT_COLUMN,
            Constants.SOURCE_SNAPSHOT_ID_COLUMN,
            Constants.SOURCE_SEQUENCE_NUMBER_COLUMN);

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public ProjectionReader(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public List<Map<String, Object>> sample(String sourceTable, String configId, int limit) {
        // Addressed by (source table, config id) directly. Fabricating a spec to obtain the name
        // cannot work: the name embeds the config id, which is a hash of the spec's fields, so a
        // placeholder spec addresses a table that was never written.
        org.apache.iceberg.catalog.TableIdentifier identifier =
                ProjectionBuilder.identifier(vectorNamespace, sourceTable, configId);

        Table table = catalogService.getCatalog().tableExists(identifier)
                ? catalogService.getCatalog().loadTable(identifier)
                : null;
        if (table == null) {
            throw new IllegalStateException(
                    "No projection for " + sourceTable + "; it is published once a pass completes");
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .select(LINEAGE_COLUMNS.toArray(new String[0]))
                .build()) {

            for (Record record : scan) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (String column : LINEAGE_COLUMNS) {
                    Object value = record.getField(column);
                    row.put(column, value == null ? null : String.valueOf(value));
                }
                rows.add(row);
                if (rows.size() >= limit) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read projection for " + sourceTable, e);
        }
        return rows;
    }

    /**
     * The vector width of a published projection, or 0 when it has not been built.
     *
     * <p>Separate from {@link #sample} rather than read out of it, for two reasons that both bite.
     * {@code sample} stringifies every value for display, so the width would arrive as
     * {@code "384"} and any numeric use of it is a parse waiting to be forgotten; and it throws when
     * the projection is absent, which is right for a debugging endpoint and wrong for a caller whose
     * job is to answer "is there one yet".
     *
     * <p>Read from one projected row rather than from the snapshot summary: the summary key is
     * private to {@code ProjectionBuilder}, and a row is the value actually on disk rather than what
     * the last commit claimed.
     */
    public int dimension(String sourceTable, String configId) {
        org.apache.iceberg.catalog.TableIdentifier identifier =
                ProjectionBuilder.identifier(vectorNamespace, sourceTable, configId);
        if (!catalogService.getCatalog().tableExists(identifier)) {
            return 0;
        }

        Table table = catalogService.getCatalog().loadTable(identifier);
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .select(Constants.EMBEDDING_DIM_COLUMN, Constants.CONFIG_ID_COLUMN)
                .build()) {
            for (Record record : scan) {
                Object value = record.getField(Constants.EMBEDDING_DIM_COLUMN);
                if (value instanceof Number width) {
                    return width.intValue();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Could not read the embedding dimension for " + sourceTable, e);
        }
        return 0;
    }

    /**
     * Exact cosine top-k over a scope's projection.
     *
     * <p>The same arithmetic {@code SqlViewGenerator} emits for Trino, computed here so a
     * measurement can separate data cost from engine cost. No pruning and no index: it scans the
     * scope, because its purpose is to be the obviously-correct baseline that an approximate path
     * is compared against.
     *
     * <p>float32 on disk, double in the comparison. The projection stores float32 because every
     * embedding model emits it, and widening per element here rather than storing doubles keeps the
     * table half the size for an identical ranking.
     */
    public List<Map<String, Object>> topK(String sourceTable,
                                          String configId,
                                          List<Double> query,
                                          int k) {
        org.apache.iceberg.catalog.TableIdentifier identifier =
                ProjectionBuilder.identifier(vectorNamespace, sourceTable, configId);
        if (!catalogService.getCatalog().tableExists(identifier)) {
            throw new IllegalStateException(
                    "No projection for " + sourceTable + "; it is published once a pass completes");
        }
        Table table = catalogService.getCatalog().loadTable(identifier);

        double queryNorm = 0;
        for (Double value : query) {
            queryNorm += value * value;
        }
        queryNorm = Math.sqrt(queryNorm);

        List<Map<String, Object>> scored = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .select(Constants.SOURCE_ROW_ID_COLUMN, Constants.CHUNK_ORDINAL_COLUMN,
                        Constants.CONFIG_ID_COLUMN, Constants.EMBEDDING_COLUMN,
                        Constants.TEXT_COLUMN)
                .build()) {

            for (Record record : rows) {
                Object raw = record.getField(Constants.EMBEDDING_COLUMN);
                if (!(raw instanceof List<?> embedding) || embedding.size() != query.size()) {
                    // A width mismatch means two embedding spaces in one scope. Skipping rather
                    // than scoring: array arithmetic would pad the shorter side and return a
                    // plausible number for an incomparable vector.
                    continue;
                }
                double dot = 0;
                double norm = 0;
                for (int i = 0; i < embedding.size(); i++) {
                    double a = ((Number) embedding.get(i)).doubleValue();
                    dot += a * query.get(i);
                    norm += a * a;
                }
                double similarity = dot / (Math.sqrt(norm) * queryNorm + 1e-12);

                Map<String, Object> hit = new LinkedHashMap<>();
                hit.put("sourceRowId", String.valueOf(record.getField(Constants.SOURCE_ROW_ID_COLUMN)));
                hit.put("chunkOrdinal", record.getField(Constants.CHUNK_ORDINAL_COLUMN));
                hit.put("similarity", similarity);
                hit.put("text", record.getField(Constants.TEXT_COLUMN));
                scored.add(hit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not search the projection for " + sourceTable, e);
        }

        scored.sort((left, right) -> Double.compare(
                (Double) right.get("similarity"), (Double) left.get("similarity")));
        return scored.subList(0, Math.min(k, scored.size()));
    }
}
