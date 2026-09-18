package io.vectorsync.worker.service.iceberg;

import io.vectorsync.format.catalog.IcebergCatalogConfig;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Detects a half-migrated JDBC catalog, because the failure it causes is otherwise undiagnosable.
 *
 * <p>Iceberg's JDBC catalog stores one row per table in {@code iceberg_tables} and commits by
 * compare-and-set: {@code UPDATE ... WHERE metadata_location = <what the writer last read>}. When
 * support for views was added, the V1 schema gained an {@code iceberg_type} column and that column
 * became part of the predicate. A catalog created under the older schema and later upgraded keeps
 * its existing rows with {@code iceberg_type} left NULL -- and those rows can then never satisfy a
 * V1 predicate, so <b>every commit to those specific tables fails forever</b> while creates, loads
 * and reads all continue to work perfectly.
 *
 * <p>What that looks like from inside this system is worth spelling out, because it cost hours to
 * find. Derivation runs, the model is called, vectors are written to Parquet, and then the append
 * raises {@code CommitFailedException}. The derive path correctly treats that as "nothing landed"
 * and retries; the control plane correctly gives a work item three attempts; three deterministic
 * failures later the materialization is DEGRADED with "rows did not materialize". Every layer
 * behaves correctly and the reported cause -- a lost commit race -- is the one thing that is not
 * happening. Meanwhile newer tables in the same catalog commit without trouble, which makes it look
 * like contention rather than a catalog defect.
 *
 * <p>So this runs once at startup, reads nothing but a count, and says so plainly with the repair.
 * It never fails startup: the condition is per-table, a warehouse may have a mix, and refusing to
 * boot would take away the very deployment an operator needs in order to run the fix.
 */
@Component
@Slf4j
public class JdbcCatalogHealthCheck {

    private final IcebergCatalogService catalogService;

    /**
     * Takes the catalog service rather than the configuration directly: the configuration is not a
     * Spring bean here, it is assembled by {@link IcebergCatalogService} from its own property
     * source, and that service is the single place in the worker that knows how the catalog is
     * wired.
     */
    public JdbcCatalogHealthCheck(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    private IcebergCatalogConfig config() {
        return catalogService.config();
    }

    /** @param affectedTables qualified names whose every commit will fail until repaired */
    public record Finding(boolean checked,
                          boolean healthy,
                          List<String> affectedTables,
                          String note,
                          String repairSql) {

        static Finding notApplicable(String note) {
            return new Finding(false, true, List.of(), note, null);
        }
    }

    @PostConstruct
    void reportAtStartup() {
        Finding finding = inspect();
        if (!finding.checked() || finding.healthy()) {
            return;
        }
        log.error("""
                        The Iceberg JDBC catalog has {} table(s) with a NULL iceberg_type, and \
                        EVERY COMMIT to them will fail with CommitFailedException while reads keep \
                        working. This is a half-migrated catalog, not contention. Affected: {}. \
                        Repair with: {}""",
                finding.affectedTables().size(), finding.affectedTables(), finding.repairSql());
    }

    /**
     * Reads the catalog's own table registry. Cheap enough to expose on an endpoint.
     *
     * <p>Opens its own short-lived connection rather than borrowing Iceberg's pool, because the
     * question is about the catalog's storage rather than about any table, and because a diagnostic
     * that needs the thing it is diagnosing to be healthy is not much of a diagnostic.
     */
    public Finding inspect() {
        IcebergCatalogConfig config = config();
        if (!"jdbc".equals(config.type())) {
            return Finding.notApplicable("catalog type is " + config.type() + ", not jdbc");
        }
        String uri = config.getCatalogUri();
        if (uri == null || uri.isBlank()) {
            return Finding.notApplicable("no catalog uri configured");
        }

        List<String> affected = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(
                uri, config.getJdbcUser(), config.getJdbcPassword())) {

            // information_schema first: on a catalog still genuinely at V0 the column does not
            // exist, and there is nothing wrong with that -- Iceberg then uses V0 predicates and
            // commits fine. The broken state is specifically column-present-but-value-NULL.
            boolean hasColumn;
            try (PreparedStatement statement = connection.prepareStatement(
                    "select 1 from information_schema.columns "
                            + "where table_name = 'iceberg_tables' and column_name = 'iceberg_type'");
                 ResultSet rows = statement.executeQuery()) {
                hasColumn = rows.next();
            }
            if (!hasColumn) {
                return Finding.notApplicable(
                        "catalog is on the pre-view schema, where iceberg_type is not used");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "select table_namespace, table_name from iceberg_tables "
                            + "where iceberg_type is null order by 1, 2");
                 ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    affected.add(rows.getString(1) + "." + rows.getString(2));
                }
            }
        } catch (Exception e) {
            // A diagnostic must not become a startup dependency of its own.
            return Finding.notApplicable("could not inspect the catalog: " + e.getMessage());
        }

        if (affected.isEmpty()) {
            return new Finding(true, true, List.of(),
                    "every catalog row has an iceberg_type, so commits can succeed", null);
        }
        return new Finding(true, false, affected,
                affected.size() + " table(s) cannot be committed to: their catalog row has a NULL "
                        + "iceberg_type, which no V1 compare-and-set predicate can match",
                "UPDATE iceberg_tables SET iceberg_type = 'TABLE' WHERE iceberg_type IS NULL;");
    }

    /** Endpoint-friendly view. */
    public Map<String, Object> asMap() {
        Finding finding = inspect();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("checked", finding.checked());
        out.put("healthy", finding.healthy());
        out.put("note", finding.note());
        out.put("affectedTables", finding.affectedTables());
        if (finding.repairSql() != null) {
            out.put("repairSql", finding.repairSql());
        }
        return out;
    }
}
