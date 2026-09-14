package io.vectorsync.format.catalog;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Immutable catalog settings, decoupled from any configuration framework.
 *
 * <p>Spring services build this from {@code @Value}-injected properties; a Spark job or CLI can
 * build it from its own configuration source. That separation is why this type exists.
 *
 * <p>Value equality is required, not cosmetic: {@link IcebergCatalogFactory} caches catalogs keyed
 * by this type, and identity equality would make every call a cache miss.
 */
@Getter
@Builder
@EqualsAndHashCode
public class IcebergCatalogConfig {

    private static final String HADOOP_CATALOG_IMPL = "org.apache.iceberg.hadoop.HadoopCatalog";

    /**
     * Catalog types Iceberg resolves by name through {@code CatalogUtil.buildIcebergCatalog}.
     * Anything outside this set is treated as a fully-qualified implementation class.
     */
    private static final Set<String> NAMED_TYPES =
            Set.of("hadoop", "hive", "rest", "glue", "nessie", "jdbc");

    /** Types whose catalog lives at a URI rather than on the warehouse filesystem. */
    private static final Set<String> URI_TYPES = Set.of("rest", "hive", "nessie", "jdbc");

    private final String catalogType;
    private final String warehousePath;

    /**
     * Catalog endpoint. Required for rest, hive, nessie and jdbc; meaningless for hadoop and glue.
     */
    private final String catalogUri;

    /** Bearer token or {@code id:secret} credential for a REST catalog. */
    private final String catalogCredential;

    /** Catalog name as the server knows it, when it differs from the warehouse path. */
    private final String catalogWarehouse;

    /**
     * Escape hatch for properties this type does not model -- {@code glue.id}, {@code nessie.ref},
     * {@code rest.scope}, JDBC credentials. Passed through untouched.
     *
     * <p>Necessary rather than lazy: catalog implementations each accept their own property keys and
     * enumerating them here would mean a code change every time a user runs a catalog variant this
     * project has not seen.
     */
    @Builder.Default
    private final Map<String, String> extraProperties = Map.of();
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3Region;

    @Builder.Default
    private final boolean pathStyleAccess = true;

    /**
     * Permits HadoopCatalog on object storage despite its lack of atomic commits. Off by default so
     * the unsafe configuration has to be chosen rather than inherited.
     */
    @Builder.Default
    private final boolean allowUnsafeHadoopCatalog = false;

    /** Normalized type, defaulting to hadoop. */
    public String type() {
        return catalogType == null || catalogType.isBlank() ? "hadoop" : catalogType.trim().toLowerCase();
    }

    /** True when Iceberg can resolve this type by name instead of by class. */
    public boolean isNamedType() {
        return NAMED_TYPES.contains(type());
    }

    /** True when the type needs {@link #getCatalogUri()}. */
    public boolean requiresUri() {
        return URI_TYPES.contains(type());
    }

    /**
     * Resolves the catalog implementation class for a custom type.
     *
     * <p>Only meaningful when {@link #isNamedType()} is false. Named types are built by Iceberg from
     * the {@code type} property, which is how a REST or Glue catalog gets its own configuration
     * handling rather than being force-fitted through a class name.
     */
    public String catalogImpl() {
        return isNamedType() ? HADOOP_CATALOG_IMPL : catalogType;
    }

    /**
     * Catalog properties in the form Iceberg expects.
     *
     * <p>Built here rather than in the factory so a non-Spring caller gets identical behavior, and
     * so the S3 settings cannot drift between the two.
     */
    public Map<String, String> catalogProperties() {
        Map<String, String> properties = new LinkedHashMap<>();

        if (isNamedType()) {
            properties.put("type", type());
        } else {
            properties.put("catalog-impl", catalogType);
        }

        if (!isBlank(warehousePath)) {
            properties.put("warehouse", warehousePath);
        }
        if (!isBlank(catalogWarehouse)) {
            // A server-side catalog name, which for REST is distinct from the physical warehouse
            // location: the server resolves storage, the client only names the catalog.
            properties.put("warehouse", catalogWarehouse);
        }
        if (!isBlank(catalogUri)) {
            properties.put("uri", catalogUri);
        }
        if (!isBlank(catalogCredential)) {
            properties.put("credential", catalogCredential);
        }

        if (usesNativeS3FileIO()) {
            properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            properties.put("s3.endpoint", s3Endpoint);
            properties.put("s3.access-key-id", s3AccessKey);
            properties.put("s3.secret-access-key", s3SecretKey);
            properties.put("s3.path-style-access", String.valueOf(pathStyleAccess));
            properties.put("s3.region", s3Region);
            properties.put("client.region", s3Region);
        }

        // Last, so an operator can override anything above for a catalog variant we have not seen.
        properties.putAll(extraProperties);
        return properties;
    }

    /** True when the warehouse lives on object storage under either the s3 or s3a scheme. */
    public boolean usesObjectStore() {
        return warehousePath != null
                && (warehousePath.startsWith("s3://") || warehousePath.startsWith("s3a://"));
    }

    /**
     * True only for the {@code s3://} scheme, which selects Iceberg's native S3FileIO. An
     * {@code s3a://} warehouse is read and written through Hadoop's S3AFileSystem instead.
     *
     * <p>All components must agree on this, or writers and readers end up using different FileIO
     * implementations against the same warehouse.
     */
    public boolean usesNativeS3FileIO() {
        return warehousePath != null && warehousePath.startsWith("s3://");
    }

    /**
     * Validates lazily, at point of use rather than at construction, so a service that never
     * touches Iceberg can start without object-store credentials configured.
     */
    public void validate() {
        if (requiresUri()) {
            if (isBlank(catalogUri)) {
                throw new IllegalStateException(
                        "A " + type() + " catalog requires iceberg.catalog.uri");
            }
        } else if (isBlank(warehousePath)) {
            throw new IllegalStateException(
                    "ICEBERG_CATALOG_WAREHOUSE is required for a " + type() + " catalog");
        }

        if (usesObjectStore()
                && (isBlank(s3Endpoint) || isBlank(s3AccessKey) || isBlank(s3SecretKey) || isBlank(s3Region))) {
            throw new IllegalStateException("AWS S3 settings are required for s3/s3a warehouses");
        }

        // Refused rather than warned about. HadoopCatalog resolves the current metadata pointer by
        // listing the table directory and relies on an atomic rename to commit; object stores
        // provide neither, so two concurrent commits can both succeed and one silently wins. It is
        // adequate for a single-writer demo and unsafe for the shared, multi-writer deployment this
        // service is meant to be, which is exactly the configuration that looks like it works.
        if ("hadoop".equals(type()) && usesObjectStore() && !allowUnsafeHadoopCatalog) {
            throw new IllegalStateException(
                    "HadoopCatalog on an object store cannot commit atomically and will corrupt "
                            + "metadata under concurrent writers. Use a rest, hive, glue or nessie "
                            + "catalog, or set iceberg.catalog.allow-unsafe-hadoop=true to accept "
                            + "the risk for single-writer local use.");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
