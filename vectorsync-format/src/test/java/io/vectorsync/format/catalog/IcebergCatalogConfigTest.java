package io.vectorsync.format.catalog;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergCatalogConfigTest {

    private static IcebergCatalogConfig.IcebergCatalogConfigBuilder s3aConfig() {
        return IcebergCatalogConfig.builder()
                .warehousePath("s3a://vectorsync/warehouse")
                .s3Endpoint("http://localhost:9000")
                .s3AccessKey("key")
                .s3SecretKey("secret")
                .s3Region("us-east-1");
    }

    @Test
    @DisplayName("blank or hadoop catalog type resolves to HadoopCatalog")
    void defaultsToHadoopCatalog() {
        String expected = "org.apache.iceberg.hadoop.HadoopCatalog";

        assertEquals(expected, s3aConfig().catalogType(null).build().catalogImpl());
        assertEquals(expected, s3aConfig().catalogType("").build().catalogImpl());
        assertEquals(expected, s3aConfig().catalogType("hadoop").build().catalogImpl());
        assertEquals(expected, s3aConfig().catalogType("HADOOP").build().catalogImpl());
    }

    @Test
    @DisplayName("a custom catalog type is passed through as a class name")
    void customCatalogImpl() {
        assertEquals(
                "org.apache.iceberg.rest.RESTCatalog",
                s3aConfig().catalogType("org.apache.iceberg.rest.RESTCatalog").build().catalogImpl());
    }

    @Test
    @DisplayName("s3 and s3a are object stores; file is not")
    void detectsObjectStore() {
        assertTrue(s3aConfig().build().usesObjectStore());
        assertTrue(s3aConfig().warehousePath("s3://bucket/wh").build().usesObjectStore());
        assertFalse(s3aConfig().warehousePath("file:///tmp/wh").build().usesObjectStore());
    }

    @Test
    @DisplayName("native S3FileIO is selected only for the s3 scheme, not s3a")
    void nativeFileIoOnlyForS3Scheme() {
        assertTrue(s3aConfig().warehousePath("s3://bucket/wh").build().usesNativeS3FileIO());
        assertFalse(s3aConfig().warehousePath("s3a://bucket/wh").build().usesNativeS3FileIO());
        assertFalse(s3aConfig().warehousePath("file:///tmp/wh").build().usesNativeS3FileIO());
    }

    @Test
    @DisplayName("a blank warehouse always fails validation")
    void rejectsBlankWarehouse() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> IcebergCatalogConfig.builder().warehousePath("").build().validate());

        assertTrue(error.getMessage().contains("ICEBERG_CATALOG_WAREHOUSE"));
    }

    @Test
    @DisplayName("an object-store warehouse requires a region")
    void requiresRegionForObjectStore() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> s3aConfig().s3Region("").build().validate());

        assertTrue(error.getMessage().contains("AWS S3 settings"));
    }

    @Test
    @DisplayName("an object-store warehouse requires endpoint and credentials")
    void requiresCredentialsForObjectStore() {
        assertThrows(IllegalStateException.class, () -> s3aConfig().s3Endpoint("").build().validate());
        assertThrows(IllegalStateException.class, () -> s3aConfig().s3AccessKey("").build().validate());
        assertThrows(IllegalStateException.class, () -> s3aConfig().s3SecretKey("").build().validate());
    }

    @Test
    @DisplayName("a non-object-store warehouse needs no S3 settings")
    void allowsLocalWarehouseWithoutS3Settings() {
        IcebergCatalogConfig.builder()
                .warehousePath("file:///tmp/warehouse")
                .build()
                .validate();
    }

    @Test
    @DisplayName("path-style access defaults to true for MinIO compatibility")
    void pathStyleAccessDefaultsTrue() {
        assertTrue(IcebergCatalogConfig.builder().warehousePath("file:///tmp/wh").build().isPathStyleAccess());
    }
}
