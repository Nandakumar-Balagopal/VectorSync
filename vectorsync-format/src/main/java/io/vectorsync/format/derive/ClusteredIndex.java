package io.vectorsync.format.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.catalog.Namespaces;
import io.vectorsync.format.io.IcebergAppender;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A serving table partitioned by cluster id, so that an ordinary query engine performs the candidate
 * reduction an ANN index would otherwise perform.
 *
 * <p>This is the answer to a specific constraint. An Iceberg-native ANN index is not readable by any
 * engine today: Iceberg classifies vector indexing as early-stage discussion, Puffin's established
 * index use is Bloom filters and deletion vectors, and the two 2026 proposals for ANN blobs are
 * unstandardised -- so writing one would require forking every engine that is supposed to read it.
 * Partitioning, by contrast, is implemented everywhere. Assign each vector to its nearest centroid,
 * partition by that id, and a query that probes the nearest few clusters reads only those partitions.
 * The approximation lives in the partition predicate rather than in a graph, and Spark, Trino,
 * Databricks and Snowflake all prune it without knowing anything about vectors.
 *
 * <p>The cost model is explicit and is the point: probing {@code p} of {@code k} clusters reads
 * roughly {@code p/k} of the table, and recall is whatever that fraction captures. Both numbers are
 * measurable, which is what makes the trade defensible rather than hopeful.
 *
 * <p>Centroids live in their own table beside this one. They are small, read once per query, and
 * meaningless without the assignment they produced -- so they share its lifecycle rather than being
 * recomputed by each reader.
 */
@Slf4j
public final class ClusteredIndex {

    public static final String TABLE_NAME_PREFIX = "clustered_";
    public static final String CENTROIDS_TABLE_NAME = "vector_centroids";
    public static final String CLUSTER_ID_COLUMN = "cluster_id";
    public static final String CENTROID_COLUMN = "centroid";

    private ClusteredIndex() {
    }

    /** One vector as the clustered table stores it. */
    public record Entry(String contentHash,
                        int clusterId,
                        String modelVersion,
                        String configId,
                        int embeddingDim,
                        float[] embedding,
                        String text) {
    }

    /** A candidate returned by a probe, with the score the engine would have computed. */
    public record Candidate(String contentHash, int clusterId, double similarity, String text) {
    }

    /**
     * @param rowsScanned rows actually read, which is the number the pruning claim rests on
     * @param totalRows   rows in the whole scope, for the fraction
     */
    public record ProbeResult(List<Candidate> candidates,
                              List<Integer> probedClusters,
                              long rowsScanned,
                              long totalRows) {

        public double fractionScanned() {
            return totalRows == 0 ? 0.0 : (double) rowsScanned / totalRows;
        }
    }

    // ---------------------------------------------------------------- schemas

    public static Schema schema() {
        return new Schema(
                Types.NestedField.required(1, Constants.CONTENT_HASH_COLUMN, Types.StringType.get()),
                Types.NestedField.required(2, CLUSTER_ID_COLUMN, Types.IntegerType.get()),
                Types.NestedField.required(3, Constants.MODEL_VERSION_COLUMN, Types.StringType.get()),
                Types.NestedField.required(4, Constants.CONFIG_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(5, Constants.EMBEDDING_DIM_COLUMN, Types.IntegerType.get()),
                Types.NestedField.required(6, Constants.EMBEDDING_COLUMN,
                        Types.ListType.ofRequired(7, Types.FloatType.get())),
                Types.NestedField.optional(8, Constants.TEXT_COLUMN, Types.StringType.get()));
    }

    /**
     * Partitioned by scope and then cluster.
     *
     * <p>{@code cluster_id} must be an identity partition rather than a plain column: a predicate on
     * a non-partition column filters rows after reading them, which would make the probe honest but
     * pointless. Scope leads so that two configurations do not share partitions.
     */
    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(Constants.MODEL_VERSION_COLUMN)
                .identity(Constants.CONFIG_ID_COLUMN)
                .identity(CLUSTER_ID_COLUMN)
                .build();
    }

    public static Schema centroidsSchema() {
        return new Schema(
                Types.NestedField.required(1, Constants.MODEL_VERSION_COLUMN, Types.StringType.get()),
                Types.NestedField.required(2, Constants.CONFIG_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(3, CLUSTER_ID_COLUMN, Types.IntegerType.get()),
                Types.NestedField.required(4, CENTROID_COLUMN,
                        Types.ListType.ofRequired(5, Types.FloatType.get())));
    }

    public static PartitionSpec centroidsPartitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(Constants.MODEL_VERSION_COLUMN)
                .identity(Constants.CONFIG_ID_COLUMN)
                .build();
    }

    // ---------------------------------------------------------------- tables

    public static String tableName(String sourceTable) {
        return TABLE_NAME_PREFIX + sourceTable.replaceAll("[^A-Za-z0-9_]", "_").toLowerCase();
    }

    public static TableIdentifier identifier(String namespace, String sourceTable) {
        return TableIdentifier.of(Namespace.of(namespace), tableName(sourceTable));
    }

    public static Table loadOrCreate(Catalog catalog, String namespace, String sourceTable) {
        return loadOrCreate(catalog, identifier(namespace, sourceTable), schema(),
                ClusteredIndex::partitionSpec);
    }

    public static Table loadCentroidsOrCreate(Catalog catalog, String namespace) {
        return loadOrCreate(catalog,
                TableIdentifier.of(Namespace.of(namespace), CENTROIDS_TABLE_NAME),
                centroidsSchema(), ClusteredIndex::centroidsPartitionSpec);
    }

    private static Table loadOrCreate(Catalog catalog,
                                      TableIdentifier identifier,
                                      Schema schema,
                                      java.util.function.Function<Schema, PartitionSpec> spec) {
        if (catalog.tableExists(identifier)) {
            return catalog.loadTable(identifier);
        }
        Namespaces.ensureExists(catalog, identifier);
        try {
            return catalog.createTable(identifier, schema, spec.apply(schema),
                    Map.of(Constants.FORMAT_VERSION_PROPERTY,
                            String.valueOf(Constants.VECTOR_FORMAT_VERSION)));
        } catch (AlreadyExistsException e) {
            // Two builders racing is a healthy outcome, not an error.
            return catalog.loadTable(identifier);
        }
    }

    /**
     * Drops the whole clustered table, purging its data files.
     *
     * <p>An operator primitive, not a rebuild primitive, and the distinction cost this index its
     * data. {@code purge = true} physically deletes every data file, and the table holds every
     * scope: one table serves all {@code (model_version, config_id)} pairs derived from a source
     * table, because scope is a partition and not a table boundary. A rebuild that reached for this
     * therefore deleted the rows of every other scope as a side effect, and deleted them before the
     * replacement rows had been written. {@link #replaceScope} is what a rebuild wants.
     */
    public static boolean drop(Catalog catalog, String namespace, String sourceTable) {
        return catalog.dropTable(identifier(namespace, sourceTable), true);
    }

    // ---------------------------------------------------------------- writes

    /**
     * Appends entries in their own commit, removing nothing.
     *
     * <p>Add-only, and so not usable for a rebuild: refitting centroids relabels every vector, so a
     * second generation appended over the first leaves each vector present under both its old and
     * its new cluster id and the probe scores a mixture of two assignments. {@link #replaceScope}
     * exists for that case.
     */
    public static void append(Table table, Collection<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        IcebergAppender.append(table, toRecords(table.schema(), entries));
    }

    /**
     * Replaces one scope's rows in a single snapshot: the row filter deletes what
     * {@code (model_version, config_id)} held and the new data files land in the same commit, so a
     * reader sees the old generation or the new one and never a half-built mixture.
     *
     * <p>This replaces a drop-and-append that destroyed data two different ways. The drop was
     * {@code dropTable(purge = true)}, issued after the centroids had been committed and before the
     * new rows were written, so any failure in that window had already physically deleted every
     * data file of the index with nothing left to recover from. And the drop was per <em>table</em>
     * while the contents are per <em>scope</em>: rebuilding {@code (m1, c1)} purged the rows of
     * {@code (m2, c2)} in the same clustered table while their centroids survived in
     * {@code vector_centroids}, so a probe against the purged scope ranked the query against live
     * centroids, read partitions that no longer had files, and returned nothing without erroring.
     *
     * <p>The filter names only {@code model_version} and {@code config_id}, which is load-bearing
     * rather than incidental. Iceberg deletes whole files by row filter and refuses -- "Cannot
     * delete file where some, but not all, rows match the filter" -- when it cannot prove a filter
     * covers an entire file, and both of these are identity partition fields so the projection is
     * provable. {@code cluster_id} is an identity field too, but it cannot be used to narrow this:
     * a filter on the clusters the new assignment happens to produce would leave every other
     * cluster of the old generation in place.
     *
     * <p>The conflict validation below is not optional bookkeeping. Iceberg's overwrite conflict
     * checks are opt-in, and without them a concurrent append into this scope is deleted by this
     * overwrite silently -- the commit succeeds, no exception is raised, and the rows are gone.
     *
     * <p>Passing no entries retires the scope: the delete still happens and the scope is left
     * empty. The guard against emptying a scope that is serving lives where the vectors are read,
     * in {@code ClusterIndexService.build}, because only the caller can tell an empty source from
     * an unreadable one.
     */
    public static void replaceScope(Table table,
                                    String modelVersion,
                                    String configId,
                                    Collection<Entry> entries) {
        // Read before the write, so conflict detection covers exactly the commits that landed while
        // this rebuild was running. Leaving the starting snapshot unset makes Iceberg validate every
        // ancestor instead, which reports this scope's own previous generation as a conflict and
        // fails every rebuild after the first.
        Snapshot base = table.currentSnapshot();
        List<DataFile> dataFiles =
                IcebergAppender.writeFiles(table, toRecords(table.schema(), entries));

        commitScopeReplacement(table, modelVersion, configId, dataFiles, base);
        log.info("Replaced scope {} / {} in {}: {} vectors in {} data files",
                modelVersion, configId, table.name(), entries.size(), dataFiles.size());
    }

    /**
     * Replaces one scope's centroids in a single snapshot, so at most one centroid generation per
     * scope exists on disk.
     *
     * <p>The predecessor only ever appended, and nothing deleted, so after one rebuild the table
     * held two fitted generations for the same scope. That did not surface as an error:
     * {@link #readCentroids} keys by cluster id, so the two generations collapsed into one list of
     * {@code k} centroids drawn arbitrarily from two different k-means solutions. The probe then
     * ranked the query against centroids that did not describe the assignment on disk and selected
     * cluster ids accordingly -- wrong neighbours, exactly scored, with no signal that anything had
     * happened.
     *
     * <p>The filter is the same {@code (model_version, config_id)} pair as for the data table, and
     * for the same reason: {@link #centroidsPartitionSpec} makes both identity partition fields, so
     * the delete is provably whole-file. {@code cluster_id} is a plain column here, not a partition
     * field, so naming it in the filter would fail at commit time.
     */
    public static void replaceCentroids(Table table,
                                        String modelVersion,
                                        String configId,
                                        List<float[]> centroids) {
        Snapshot base = table.currentSnapshot();
        Schema schema = table.schema();
        List<Record> records = new ArrayList<>(centroids.size());
        for (int cluster = 0; cluster < centroids.size(); cluster++) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField(Constants.MODEL_VERSION_COLUMN, modelVersion);
            record.setField(Constants.CONFIG_ID_COLUMN, configId);
            record.setField(CLUSTER_ID_COLUMN, cluster);
            record.setField(CENTROID_COLUMN, toList(centroids.get(cluster)));
            records.add(record);
        }

        List<DataFile> dataFiles = IcebergAppender.writeFiles(table, records);
        commitScopeReplacement(table, modelVersion, configId, dataFiles, base);
        log.info("Replaced {} centroids for {} / {}", centroids.size(), modelVersion, configId);
    }

    /**
     * The one commit both replacements make.
     *
     * <p>Modelled on {@code ProjectionBuilder.commitReplacement}, with conflict validation added.
     * Iceberg's commit retry resolves two writers in <em>different</em> scopes on its own, because
     * their row filters are disjoint. What it cannot resolve is a writer in the <em>same</em> scope:
     * a concurrent {@link #append} or a second rebuild lands files this overwrite's filter covers,
     * and Iceberg's default behaviour is to delete them and report success. So the commit fails
     * instead, and the caller rebuilds against the state that actually won.
     */
    private static void commitScopeReplacement(Table table,
                                               String modelVersion,
                                               String configId,
                                               List<DataFile> dataFiles,
                                               Snapshot base) {
        Expression scope = scopeFilter(modelVersion, configId);

        OverwriteFiles overwrite = table.newOverwrite().overwriteByRowFilter(scope);
        dataFiles.forEach(overwrite::addFile);
        if (!dataFiles.isEmpty()) {
            // Catches a row written under the wrong partition values before it becomes a file that
            // the next rebuild's filter cannot delete. Enabled only when files were added: the
            // validation reads the spec of the added files, so on a pure delete (retiring a scope)
            // it fails with "Cannot determine partition spec".
            overwrite.validateAddedFilesMatchOverwriteFilter();
        }

        // validateNoConflictingData is what turns the check on; setting the filter alone validates
        // nothing. The filter is passed explicitly rather than left to default to the row filter so
        // that the two cannot drift apart later, since the set of rows this commit removes and the
        // set a concurrent writer must not have added are the same set by construction.
        overwrite.conflictDetectionFilter(scope);
        overwrite.validateNoConflictingData();
        if (base != null) {
            overwrite.validateFromSnapshot(base.snapshotId());
        }

        overwrite.commit();
    }

    /**
     * The scope predicate. Identity partition fields only, in both tables, which is what makes the
     * delete whole-file provable in one and the same expression usable for the other.
     */
    private static Expression scopeFilter(String modelVersion, String configId) {
        return Expressions.and(
                Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion),
                Expressions.equal(Constants.CONFIG_ID_COLUMN, configId));
    }

    private static List<Record> toRecords(Schema schema, Collection<Entry> entries) {
        List<Record> records = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField(Constants.CONTENT_HASH_COLUMN, entry.contentHash());
            record.setField(CLUSTER_ID_COLUMN, entry.clusterId());
            record.setField(Constants.MODEL_VERSION_COLUMN, entry.modelVersion());
            record.setField(Constants.CONFIG_ID_COLUMN, entry.configId());
            record.setField(Constants.EMBEDDING_DIM_COLUMN, entry.embeddingDim());
            record.setField(Constants.EMBEDDING_COLUMN, toList(entry.embedding()));
            record.setField(Constants.TEXT_COLUMN, entry.text());
            records.add(record);
        }
        return records;
    }

    // ---------------------------------------------------------------- reads

    /**
     * Centroids for one scope, indexed by cluster id.
     *
     * <p>Keying by cluster id assumes one generation per scope, which is what
     * {@link #replaceCentroids} guarantees. It is also why the two-generation bug it fixed was
     * invisible: with two fits on disk this returns {@code k} centroids either way, just half of
     * them from a solution the rows were never assigned against.
     */
    public static List<float[]> readCentroids(Table table, String modelVersion, String configId) {
        Map<Integer, float[]> byCluster = new HashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .build()) {
            for (Record row : rows) {
                Object cluster = row.getField(CLUSTER_ID_COLUMN);
                byCluster.put(((Number) cluster).intValue(),
                        toFloats(row.getField(CENTROID_COLUMN)));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read centroids for " + configId, e);
        }

        List<float[]> centroids = new ArrayList<>(byCluster.size());
        for (int cluster = 0; cluster < byCluster.size(); cluster++) {
            float[] centroid = byCluster.get(cluster);
            if (centroid == null) {
                throw new IllegalStateException(
                        "Centroid set for " + configId + " is missing cluster " + cluster);
            }
            centroids.add(centroid);
        }
        return centroids;
    }

    /**
     * Scores the probed partitions exactly and returns the top {@code k}.
     *
     * <p>The predicate is an {@code IN} on the partition column, so Iceberg prunes to those
     * partitions and {@code rowsScanned} reflects what was really read. Scoring is exact within the
     * candidate set -- the approximation is entirely in which partitions were chosen, which is what
     * makes the result explainable: a missed neighbour was in a cluster that was not probed.
     */
    public static ProbeResult probe(Table table,
                                    String modelVersion,
                                    String configId,
                                    float[] query,
                                    List<Integer> clusters,
                                    int k,
                                    long totalRows) {
        List<Candidate> scored = new ArrayList<>();
        long scanned = 0;

        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .where(Expressions.in(CLUSTER_ID_COLUMN, clusters))
                .build()) {

            for (Record row : rows) {
                scanned++;
                float[] embedding = toFloats(row.getField(Constants.EMBEDDING_COLUMN));
                scored.add(new Candidate(
                        String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN)),
                        ((Number) row.getField(CLUSTER_ID_COLUMN)).intValue(),
                        VectorClustering.cosine(query, embedding),
                        row.getField(Constants.TEXT_COLUMN) == null
                                ? null : String.valueOf(row.getField(Constants.TEXT_COLUMN))));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Probe failed for " + configId, e);
        }

        scored.sort((a, b) -> Double.compare(b.similarity(), a.similarity()));
        return new ProbeResult(
                scored.subList(0, Math.min(k, scored.size())), clusters, scanned, totalRows);
    }

    /** Exhaustive scan of a scope, which is the ground truth recall is measured against. */
    public static List<Candidate> exact(Table table,
                                        String modelVersion,
                                        String configId,
                                        float[] query,
                                        int k) {
        List<Candidate> scored = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .build()) {
            for (Record row : rows) {
                scored.add(new Candidate(
                        String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN)),
                        ((Number) row.getField(CLUSTER_ID_COLUMN)).intValue(),
                        VectorClustering.cosine(query, toFloats(row.getField(Constants.EMBEDDING_COLUMN))),
                        null));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Exact scan failed for " + configId, e);
        }
        scored.sort((a, b) -> Double.compare(b.similarity(), a.similarity()));
        return scored.subList(0, Math.min(k, scored.size()));
    }

    // ---------------------------------------------------------------- conversion

    private static List<Float> toList(float[] values) {
        List<Float> list = new ArrayList<>(values.length);
        for (float value : values) {
            list.add(value);
        }
        return list;
    }

    private static float[] toFloats(Object value) {
        if (!(value instanceof List<?> list)) {
            return new float[0];
        }
        float[] values = new float[list.size()];
        int position = 0;
        for (Object element : list) {
            values[position++] = element instanceof Number number ? number.floatValue() : 0f;
        }
        return values;
    }
}
