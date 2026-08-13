package io.vectorsync.worker.service.iceberg;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;

@Service
@Slf4j
public class IcebergCatalogService {

    @Value("${iceberg.catalog.type:hadoop}")
    private String catalogType;

    @Value("${iceberg.catalog.warehouse}")
    private String warehousePath;

    @Value("${aws.s3.endpoint}")
    private String s3Endpoint;

    @Value("${aws.s3.access-key}")
    private String s3AccessKey;

    @Value("${aws.s3.secret-key}")
    private String s3SecretKey;

    @Value("${aws.s3.region}")
    private String s3Region;

    @Value("${aws.s3.path-style-access:true}")
    private boolean pathStyleAccess;

    public Catalog getCatalog() {
        validateConfig();
        log.info("Creating Iceberg catalog with warehouse: {}", warehousePath);

        ensureWarehouseBucket();

        String catalogImpl = catalogType == null || catalogType.isBlank() || "hadoop".equalsIgnoreCase(catalogType)
            ? "org.apache.iceberg.hadoop.HadoopCatalog"
            : catalogType;

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, catalogImpl);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
        boolean useS3FileIO = warehousePath != null && warehousePath.startsWith("s3://");
        if (useS3FileIO) {
            properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
            properties.put("s3.endpoint", s3Endpoint);
            properties.put("s3.access-key-id", s3AccessKey);
            properties.put("s3.secret-access-key", s3SecretKey);
            properties.put("s3.path-style-access", String.valueOf(pathStyleAccess));
            properties.put("s3.region", s3Region);
            properties.put("aws.region", s3Region);
        }

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", s3Endpoint);
        hadoopConf.set("fs.s3a.endpoint.region", s3Region);
        hadoopConf.set("fs.s3a.aws.region", s3Region);
        hadoopConf.set("fs.s3a.access.key", s3AccessKey);
        hadoopConf.set("fs.s3a.secret.key", s3SecretKey);
        hadoopConf.set("fs.s3a.path.style.access", String.valueOf(pathStyleAccess));
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

        return CatalogUtil.loadCatalog(
            catalogImpl,
            "iceberg-catalog",
            properties,
            hadoopConf
        );
    }

    private void ensureWarehouseBucket() {
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
                .pathStyleAccessEnabled(pathStyleAccess)
                .build();

        try (S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(s3Endpoint))
                .region(Region.of(s3Region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3AccessKey, s3SecretKey)))
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

    private void validateConfig() {
        if (isBlank(warehousePath)) {
            throw new IllegalStateException("ICEBERG_CATALOG_WAREHOUSE is required");
        }

        boolean useS3 = warehousePath.startsWith("s3://") || warehousePath.startsWith("s3a://");
        if (useS3 && (isBlank(s3Endpoint) || isBlank(s3AccessKey) || isBlank(s3SecretKey) || isBlank(s3Region))) {
            throw new IllegalStateException("AWS S3 settings are required for s3/s3a warehouses");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
