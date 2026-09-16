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
}
