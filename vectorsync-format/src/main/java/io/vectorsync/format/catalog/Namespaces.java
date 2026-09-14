package io.vectorsync.format.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;

import java.util.Map;

/**
 * Creates a namespace before the first table is created in it.
 *
 * <p>HadoopCatalog treats a namespace as a directory prefix and materializes it implicitly, so
 * nothing in this project ever needed to create one. Every other catalog requires the namespace to
 * exist first: REST, Hive, Glue and JDBC all reject {@code createTable} into a missing namespace
 * with {@code NoSuchNamespaceException}. The result was that supporting those catalog types got as
 * far as constructing the catalog and then failed on the first write -- and those are precisely the
 * catalog types Trino and Databricks can attach to, so the engines this project targets could not
 * be reached at all.
 *
 * <p>Idempotent and best-effort by design. A concurrent creator winning the race is success, not
 * failure, and a catalog that forbids namespace creation to this principal but already has the
 * namespace provisioned must still work -- so a failure here is logged and the subsequent
 * {@code createTable} is allowed to produce the real error.
 */
@Slf4j
public final class Namespaces {

    private Namespaces() {
    }

    /** Ensures the namespace of {@code identifier} exists, when the catalog has namespaces at all. */
    public static void ensureExists(Catalog catalog, TableIdentifier identifier) {
        if (catalog == null || identifier == null) {
            return;
        }
        ensureExists(catalog, identifier.namespace());
    }

    public static void ensureExists(Catalog catalog, Namespace namespace) {
        if (!(catalog instanceof SupportsNamespaces namespaces)
                || namespace == null || namespace.isEmpty()) {
            return;
        }

        try {
            if (namespaces.namespaceExists(namespace)) {
                return;
            }
            namespaces.createNamespace(namespace, Map.of());
            log.info("Created namespace {}", namespace);
        } catch (AlreadyExistsException e) {
            // Another writer got there first, which is the outcome we wanted.
        } catch (Exception e) {
            // Deliberately not fatal: the namespace may exist but be unlistable to this principal,
            // and createTable will report the real problem with better context than this can.
            log.warn("Could not ensure namespace {} exists ({}); continuing to table creation",
                    namespace, e.getMessage());
        }
    }
}
