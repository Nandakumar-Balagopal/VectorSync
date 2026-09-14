package io.vectorsync.format.catalog;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

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

    private final String catalogType;
    private final String warehousePath;
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3Region;

    @Builder.Default
    private final boolean pathStyleAccess = true;

    /**
     * Resolves the catalog implementation class. A blank or {@code hadoop} type means
     * HadoopCatalog; anything else is treated as a fully-qualified class name.
     */
    public String catalogImpl() {
        return catalogType == null || catalogType.isBlank() || "hadoop".equalsIgnoreCase(catalogType)
                ? HADOOP_CATALOG_IMPL
                : catalogType;
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
        if (isBlank(warehousePath)) {
            throw new IllegalStateException("ICEBERG_CATALOG_WAREHOUSE is required");
        }

        if (usesObjectStore()
                && (isBlank(s3Endpoint) || isBlank(s3AccessKey) || isBlank(s3SecretKey) || isBlank(s3Region))) {
            throw new IllegalStateException("AWS S3 settings are required for s3/s3a warehouses");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
