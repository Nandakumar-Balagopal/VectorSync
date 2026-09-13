package io.vectorsync.searchservice.service.index;

import io.vectorsync.common.Constants;
import io.vectorsync.format.index.IndexAliasEntry;
import io.vectorsync.format.index.IndexAliasStore;
import io.vectorsync.format.index.IndexArtifactStore;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.format.index.IndexManifestStore;
import io.vectorsync.format.vector.VectorTableSchema;
import io.vectorsync.searchservice.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Spring-facing access to the manifest, the alias log, and artifact storage.
 *
 * <p>Everything durable lives in Iceberg or object storage, so this holds no state of its own and
 * two search replicas always agree about which index is promoted.
 */
@Service
@Slf4j
public class IndexRegistry {

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    /** Where artifacts are written. Defaults to an {@code indexes/} prefix in the warehouse. */
    @Value("${vectorsync.index.base-uri:}")
    private String configuredBaseUri;

    public IndexRegistry(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public IndexManifestStore manifest() {
        return new IndexManifestStore(catalog(), vectorNamespace);
    }

    public IndexAliasStore aliases() {
        return new IndexAliasStore(catalog(), vectorNamespace);
    }

    public IndexArtifactStore artifacts() {
        return new IndexArtifactStore(vectorTable().io());
    }

    public Table vectorTable() {
        Table table = VectorTableSchema.loadIfExists(catalog(), vectorNamespace);
        if (table == null) {
            throw new IllegalStateException(
                    "Vector table does not exist yet; run a sync before building an index");
        }
        return table;
    }

    /** Artifact location for one index, derived from its id so it is content-addressed. */
    public String artifactUri(String indexId) {
        return baseUri() + "/" + indexId;
    }

    public String baseUri() {
        if (configuredBaseUri != null && !configuredBaseUri.isBlank()) {
            return trimTrailingSlash(configuredBaseUri);
        }

        String warehouse = catalogService.config().getWarehousePath();
        if (warehouse == null || warehouse.isBlank()) {
            throw new IllegalStateException("Cannot derive an index base URI: warehouse is not configured");
        }
        return trimTrailingSlash(warehouse) + "/indexes";
    }

    // --- convenience lookups ---

    public Optional<IndexManifestEntry> promotedIndex(String sourceTable) {
        return aliases().resolveProduction(sourceTable)
                .flatMap(alias -> manifest().findById(alias.getIndexId()));
    }

    public Optional<IndexAliasEntry> promotedAlias(String sourceTable) {
        return aliases().resolveProduction(sourceTable);
    }

    public List<IndexManifestEntry> indexesFor(String sourceTable) {
        return manifest().findForTable(sourceTable);
    }

    public int formatVersion() {
        return Constants.VECTOR_FORMAT_VERSION;
    }

    private Catalog catalog() {
        return catalogService.getCatalog();
    }

    private static String trimTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }
}
