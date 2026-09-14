package io.vectorsync.format.catalog;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @Test
    @DisplayName("HadoopCatalog on object storage is refused unless explicitly accepted")
    void hadoopOnObjectStoreIsRefused() {
        // HadoopCatalog finds the current metadata pointer by listing the table directory and
        // commits with an atomic rename. Object stores provide neither, so two concurrent commits
        // can both appear to succeed. It is the default configuration and it looks like it works,
        // which is why this has to fail loudly rather than warn.
        IcebergCatalogConfig unsafe = IcebergCatalogConfig.builder()
                .catalogType("hadoop")
                .warehousePath("s3a://bucket/warehouse")
                .s3Endpoint("http://minio:9000")
                .s3AccessKey("key")
                .s3SecretKey("secret")
                .s3Region("us-east-1")
                .build();

        IllegalStateException thrown = assertThrows(IllegalStateException.class, unsafe::validate);
        assertTrue(thrown.getMessage().contains("atomically"), thrown.getMessage());

        IcebergCatalogConfig accepted = IcebergCatalogConfig.builder()
                .catalogType("hadoop")
                .warehousePath("s3a://bucket/warehouse")
                .s3Endpoint("http://minio:9000")
                .s3AccessKey("key")
                .s3SecretKey("secret")
                .s3Region("us-east-1")
                .allowUnsafeHadoopCatalog(true)
                .build();
        accepted.validate();
    }

    @Test
    @DisplayName("a local filesystem warehouse with HadoopCatalog is fine")
    void hadoopOnLocalFilesystemIsAllowed() {
        // The rename that HadoopCatalog needs is atomic on a real filesystem, so the guard must not
        // fire here or every test and local run would break.
        IcebergCatalogConfig.builder()
                .catalogType("hadoop")
                .warehousePath("file:///tmp/warehouse")
                .build()
                .validate();
    }

    @Test
    @DisplayName("a REST catalog requires a uri and is built by type, not by class name")
    void restCatalogRequiresUri() {
        IcebergCatalogConfig missingUri = IcebergCatalogConfig.builder()
                .catalogType("rest")
                .build();
        assertThrows(IllegalStateException.class, missingUri::validate);

        IcebergCatalogConfig rest = IcebergCatalogConfig.builder()
                .catalogType("rest")
                .catalogUri("https://catalog.example.com/iceberg")
                .catalogCredential("id:secret")
                .catalogWarehouse("prod")
                .build();
        rest.validate();

        assertTrue(rest.isNamedType());
        Map<String, String> properties = rest.catalogProperties();
        assertEquals("rest", properties.get("type"));
        assertEquals("https://catalog.example.com/iceberg", properties.get("uri"));
        assertEquals("id:secret", properties.get("credential"));
        // The server names the catalog; it resolves storage itself, so the client's warehouse
        // property is the catalog name rather than a path.
        assertEquals("prod", properties.get("warehouse"));
        assertNull(properties.get("catalog-impl"),
                "a named type must not also be pinned to an implementation class");
    }

    @Test
    @DisplayName("glue and hive resolve by name; an unknown type falls back to a class name")
    void namedAndCustomTypes() {
        assertTrue(IcebergCatalogConfig.builder().catalogType("glue")
                .warehousePath("s3://bucket/wh").build().isNamedType());
        assertTrue(IcebergCatalogConfig.builder().catalogType("hive")
                .catalogUri("thrift://metastore:9083").build().isNamedType());

        IcebergCatalogConfig custom = IcebergCatalogConfig.builder()
                .catalogType("com.example.MyCatalog")
                .warehousePath("file:///tmp/wh")
                .build();
        assertFalse(custom.isNamedType());
        assertEquals("com.example.MyCatalog", custom.catalogProperties().get("catalog-impl"));
    }

    @Test
    @DisplayName("extra properties override the defaults, for catalog variants not modelled here")
    void extraPropertiesWin() {
        IcebergCatalogConfig config = IcebergCatalogConfig.builder()
                .catalogType("nessie")
                .catalogUri("http://nessie:19120/api/v2")
                .extraProperties(Map.of("nessie.ref", "main", "warehouse", "s3://override/wh"))
                .build();

        Map<String, String> properties = config.catalogProperties();
        assertEquals("main", properties.get("nessie.ref"));
        assertEquals("s3://override/wh", properties.get("warehouse"),
                "an operator must be able to correct anything this class computed");
    }
}
