package io.vectorsync.format.vector;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.io.IcebergAppender;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips vectors through a real Iceberg table on the local filesystem.
 *
 * <p>Covers the insert / update / delete lifecycle that the Docker E2E asserts, without needing
 * Docker or an embedding service. It also guards the identity-partition regression: writing a
 * DataFile without its partition tuple makes every partition column read back null, which the
 * previous code masked by duplicating source_table into the metadata map.
 */
class VectorTableIntegrationTest {

    private static final String NAMESPACE = "vector";
    private static final String SOURCE_TABLE = "default.products";
    private static final String MODEL = "all-MiniLM-L6-v2";

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private VectorRecord vector(String rowId, long snapshotId, String version, boolean deleted) {
        VectorRecord record = VectorRecord.builder()
                .sourceTable(SOURCE_TABLE)
                .sourceRowId(rowId)
                .sourceSnapshotId(snapshotId)
                .sourceSequenceNumber(snapshotId / 100)
                .chunkOrdinal(0)
                .embeddingModel(MODEL)
                .embeddingVersion(version)
                .embeddingDim(deleted ? 0 : 3)
                .preprocessingId("pp-1")
                .embedding(deleted ? List.of() : List.of(0.1, 0.2, 0.3))
                .text(deleted ? null : "text for " + rowId)
                .deleted(deleted)
                .createdAt(Instant.now())
                .build();
        record.setVectorId(VectorIds.vectorId(record));
        return record;
    }

    private void write(List<VectorRecord> records) {
        Table table = VectorTableSchema.loadOrCreate(catalog, NAMESPACE);
        IcebergAppender.append(table, records.stream()
                .map(record -> VectorRecordCodec.toIcebergRecord(table.schema(), record))
                .toList());
    }

    private List<VectorRecord> readRaw() {
        Table table = VectorTableSchema.loadOrCreate(catalog, NAMESPACE);
        List<VectorRecord> records = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                records.add(VectorRecordCodec.fromIcebergRecord(row));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return records;
    }

    @Test
    @DisplayName("identity-partition columns survive the round trip")
    void partitionColumnsSurvive() {
        write(List.of(vector("p-100", 100L, "v1", false)));

        VectorRecord stored = readRaw().get(0);

        assertEquals(SOURCE_TABLE, stored.getSourceTable(), "source_table is an identity partition column");
        assertEquals(MODEL, stored.getEmbeddingModel());
        assertEquals("v1", stored.getEmbeddingVersion());
        assertEquals(MODEL + ":v1", stored.modelVersion());
        assertEquals(100L, stored.getSourceSnapshotId());
        assertEquals(1L, stored.getSourceSequenceNumber());
        assertEquals("pp-1", stored.getPreprocessingId());
        assertEquals(List.of(0.1, 0.2, 0.3), roundedEmbedding(stored));
        assertNotNull(stored.getVectorId());
    }

    private List<Double> roundedEmbedding(VectorRecord record) {
        // float32 storage means exact double equality does not hold for 0.1/0.2/0.3.
        return record.getEmbedding().stream()
                .map(value -> Math.round(value * 10.0) / 10.0)
                .toList();
    }

    @Test
    @DisplayName("insert then update resolves to the newer snapshot")
    void insertThenUpdate() {
        write(List.of(vector("p-100", 100L, "v1", false)));
        write(List.of(vector("p-100", 200L, "v1", false)));

        List<VectorRecord> live = VectorResolution.latestLiveVectors(readRaw());

        assertEquals(1, live.size());
        assertEquals(200L, live.get(0).getSourceSnapshotId());
    }

    @Test
    @DisplayName("a delete tombstone removes the row from the live set")
    void deleteTombstone() {
        write(List.of(vector("p-100", 100L, "v1", false)));
        write(List.of(vector("p-100", 200L, "v1", true)));

        assertTrue(VectorResolution.latestLiveVectors(readRaw()).isEmpty());
        assertEquals(2, readRaw().size(), "history is retained; only resolution hides the row");
    }

    @Test
    @DisplayName("re-writing the same snapshot is idempotent for the live set")
    void rewriteIsIdempotent() {
        VectorRecord record = vector("p-100", 100L, "v1", false);
        write(List.of(record));
        write(List.of(vector("p-100", 100L, "v1", false)));

        List<VectorRecord> raw = readRaw();
        assertEquals(2, raw.size(), "append-only, so both physical rows exist");
        assertEquals(record.getVectorId(), raw.get(0).getVectorId());
        assertEquals(record.getVectorId(), raw.get(1).getVectorId(), "identity is stable across retries");
        assertEquals(1, VectorResolution.latestLiveVectors(raw).size(), "but only one is live");
    }

    @Test
    @DisplayName("two embedding versions coexist in one table")
    void versionsCoexist() {
        write(List.of(
                vector("p-100", 100L, "v1", false),
                vector("p-100", 100L, "v2", false)));

        List<VectorRecord> live = VectorResolution.latestLiveVectors(readRaw());

        assertEquals(2, live.size());
        assertEquals(
                List.of(MODEL + ":v1", MODEL + ":v2"),
                live.stream().map(VectorRecord::modelVersion).sorted().toList());
    }

    @Test
    @DisplayName("asOf reproduces the live set at an earlier source snapshot")
    void asOfIsReproducible() {
        write(List.of(vector("p-100", 100L, "v1", false)));
        write(List.of(vector("p-200", 200L, "v1", false)));
        write(List.of(vector("p-100", 300L, "v1", true)));

        List<VectorRecord> raw = readRaw();

        assertEquals(1, VectorResolution.liveVectorsAsOf(raw, 1L).size());
        assertEquals(2, VectorResolution.liveVectorsAsOf(raw, 2L).size());
        assertEquals(1, VectorResolution.liveVectorsAsOf(raw, 3L).size());
    }

    @Test
    @DisplayName("a table from an older format version is refused rather than dropped")
    void refusesForeignFormatVersion() {
        Table table = VectorTableSchema.loadOrCreate(catalog, NAMESPACE);
        table.updateProperties()
                .set(Constants.FORMAT_VERSION_PROPERTY, "1")
                .commit();

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> VectorTableSchema.loadOrCreate(catalog, NAMESPACE));

        assertTrue(error.getMessage().contains("format version 1"));
        assertTrue(error.getMessage().contains("rebuild"));
        assertTrue(catalog.tableExists(VectorTableSchema.identifier(NAMESPACE)),
                "the table must still exist; dropping it must be an explicit operator action");
    }
}
