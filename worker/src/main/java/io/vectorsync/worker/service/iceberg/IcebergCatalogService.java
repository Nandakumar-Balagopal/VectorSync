package io.vectorsync.worker.service.iceberg;

import io.vectorsync.format.catalog.IcebergCatalogConfig;
import io.vectorsync.format.catalog.IcebergCatalogFactory;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Spring adapter over {@link IcebergCatalogFactory}. Holds configuration binding only; all catalog
 * construction lives in vectorsync-format so that non-Spring consumers share the same definition.
 */
@Service
public class IcebergCatalogService {

    @Value("${iceberg.catalog.type:hadoop}")
    private String catalogType;

    @Value("${iceberg.catalog.warehouse:}")
    private String warehousePath;

    @Value("${aws.s3.endpoint:}")
    private String s3Endpoint;

    @Value("${aws.s3.access-key:}")
    private String s3AccessKey;

    @Value("${aws.s3.secret-key:}")
    private String s3SecretKey;

    @Value("${aws.s3.region:}")
    private String s3Region;

    @Value("${aws.s3.path-style-access:true}")
    private boolean pathStyleAccess;

    public Catalog getCatalog() {
        return IcebergCatalogFactory.load(config());
    }

    public IcebergCatalogConfig config() {
        return IcebergCatalogConfig.builder()
                .catalogType(catalogType)
                .warehousePath(warehousePath)
                .s3Endpoint(s3Endpoint)
                .s3AccessKey(s3AccessKey)
                .s3SecretKey(s3SecretKey)
                .s3Region(s3Region)
                .pathStyleAccess(pathStyleAccess)
                .build();
    }
}
