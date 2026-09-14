package io.vectorsync.format.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single definition of how VectorSync builds an Iceberg catalog.
 *
 * <p>Previously duplicated across worker, search-service, and control-plane, which had drifted:
 * only the worker configured a region or auto-created the warehouse bucket, and the other two set
 * S3FileIO unconditionally even for {@code s3a://} warehouses. That meant the writer and the
 * readers reached the same warehouse through different FileIO implementations.
 */
@Slf4j
public final class IcebergCatalogFactory {

    private static final String CATALOG_NAME = "iceberg-catalog";
    private static final String S3_FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";

    /**
     * Catalogs are cached per configuration. Callers reach for a catalog on every read and write,
     * and building one is not cheap: it reloads the catalog implementation, rebuilds a Hadoop
     * Configuration, and performs an S3 HeadBucket round trip. Iceberg catalogs are designed to be
     * long-lived and are safe to share, so the per-call construction was pure overhead charged to
     * every query.
     */
    private static final Map<IcebergCatalogConfig, Catalog> CACHE = new ConcurrentHashMap<>();

    private IcebergCatalogFactory() {
    }

    public static Catalog load(IcebergCatalogConfig config) {
        config.validate();
        return CACHE.computeIfAbsent(config, IcebergCatalogFactory::build);
    }

    /** Drops cached catalogs. For tests that repoint the warehouse within one JVM. */
    public static void clearCache() {
        CACHE.clear();
    }

    private static Catalog build(IcebergCatalogConfig config) {
        log.info("Creating {} Iceberg catalog (warehouse {}, uri {})",
                config.type(), config.getWarehousePath(),
                config.getCatalogUri() == null ? "-" : config.getCatalogUri());

        ensureWarehouseBucket(config);

        Map<String, String> properties = config.catalogProperties();

        // buildIcebergCatalog resolves rest, hive, glue, nessie, jdbc and hadoop from the "type"
        // property, each with its own configuration handling. The previous implementation called
        // loadCatalog with a class name, which meant only HadoopCatalog could ever be constructed:
        // a REST catalog needs uri and credential, Glue needs a region, Nessie needs a ref, and
        // none of those were ever placed in the property map. Every engine deployment worth
        // integrating with -- Spark on Glue, Trino on REST or Hive, Databricks on Unity -- was
        // therefore unreachable.
        return CatalogUtil.buildIcebergCatalog(CATALOG_NAME, properties, hadoopConfiguration(config));
    }

    /**
     * Hadoop settings are applied unconditionally because an {@code s3a://} warehouse is served by
     * S3AFileSystem regardless of which Iceberg FileIO is selected.
     */
    private static Configuration hadoopConfiguration(IcebergCatalogConfig config) {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", config.getS3Endpoint());
        hadoopConf.set("fs.s3a.endpoint.region", config.getS3Region());
        hadoopConf.set("fs.s3a.aws.region", config.getS3Region());
        hadoopConf.set("fs.s3a.access.key", config.getS3AccessKey());
        hadoopConf.set("fs.s3a.secret.key", config.getS3SecretKey());
        hadoopConf.set("fs.s3a.path.style.access", String.valueOf(config.isPathStyleAccess()));
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return hadoopConf;
    }

    /**
     * Creates the warehouse bucket when missing. Best-effort: MinIO and S3 both reject table
     * creation against a nonexistent bucket, but a permissions failure here should not prevent
     * startup, since the bucket may already exist and simply not be head-able.
     */
    private static void ensureWarehouseBucket(IcebergCatalogConfig config) {
        String warehousePath = config.getWarehousePath();
        if (warehousePath == null || warehousePath.isBlank()) {
            return;
        }

        URI uri = URI.create(warehousePath);
        if (!"s3a".equalsIgnoreCase(uri.getScheme()) && !"s3".equalsIgnoreCase(uri.getScheme())) {
            return;
        }

        String bucket = uri.getHost();
        if (bucket == null || bucket.isBlank()) {
            return;
        }

        S3Configuration s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(config.isPathStyleAccess())
                .build();

        try (S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(config.getS3Endpoint()))
                .region(Region.of(config.getS3Region()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.getS3AccessKey(), config.getS3SecretKey())))
                .serviceConfiguration(s3Config)
                .build()) {
            try {
                s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            } catch (NoSuchBucketException e) {
                s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            } catch (S3Exception e) {
                if (e.statusCode() == 404) {
                    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
                } else {
                    log.warn("Failed to check/create bucket {}: {}", bucket, e.awsErrorDetails().errorMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to ensure warehouse bucket: {}", e.getMessage());
        }
    }
}
