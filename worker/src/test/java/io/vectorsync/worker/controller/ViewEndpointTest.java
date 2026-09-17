package io.vectorsync.worker.controller;

import io.vectorsync.format.derive.ProjectionBuilder;
import io.vectorsync.format.derive.SqlViewGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The generated engine-side SQL, which until now nothing in the system could reach.
 *
 * <p>{@code SqlViewGenerator} had zero callers. The project's thesis is that vectors are an open
 * table queried by an engine you already run with no VectorSync process in the query path, and the
 * DDL that makes that true existed only in the codebase -- every benchmark and every document
 * showing a Trino query was pasting SQL by hand. {@code GET /api/derive/view} now serves it, so the
 * claim and the artifact are the same thing.
 *
 * <p>Tested at the generator rather than over HTTP because the properties worth pinning are
 * properties of the emitted SQL, and the endpoint is a lookup of one integer plus a name. The
 * integer is the part that matters and it is covered by
 * {@code ProjectionReader.dimension}'s own callers.
 */
class ViewEndpointTest {

    private static final String TABLE = "vectors_default_products_0123456789abcdef";

    @Test
    @DisplayName("the projection name is derivable from source table and config id alone")
    void tableNameIsAddressable() {
        // The endpoint cannot fabricate a spec to get this name: the name embeds the config id,
        // which is a hash of the spec's fields, so a placeholder spec addresses a table that was
        // never written. This overload is what makes the endpoint possible at all.
        String name = ProjectionBuilder.tableName("default.products", "0123456789abcdef");
        assertEquals(TABLE, name);
    }

    @Test
    @DisplayName("the Trino view guards the query vector width")
    void trinoViewGuardsWidth() {
        String ddl = SqlViewGenerator.trino("iceberg.vectorsync." + TABLE, 384);

        // The guard is the reason the endpoint reads the dimension from the projection instead of
        // accepting it as a parameter. Trino's cosine_similarity returns NULL rather than raising on
        // a length mismatch, so without this a wrong width produces an all-NULL ranking -- a query
        // that looks like it worked and returns nonsense.
        assertTrue(ddl.contains("cardinality"),
                "the emitted view has no width guard, so a wrong-dimension query would silently "
                        + "rank every row NULL:\n" + ddl);
        assertTrue(ddl.contains("384"), "the dimension did not reach the emitted SQL");
        assertTrue(ddl.contains("cosine_similarity"),
                "the view does not use Trino's built-in similarity function");
        assertTrue(ddl.contains("iceberg.vectorsync." + TABLE),
                "the qualified table name did not reach the emitted SQL");
    }

    @Test
    @DisplayName("the Spark view targets the same projection")
    void sparkViewIsGenerated() {
        String ddl = SqlViewGenerator.spark("my_catalog.vectorsync." + TABLE, 384);

        assertTrue(ddl.contains(TABLE), "the table name did not reach the emitted Spark SQL");
        assertTrue(ddl.toLowerCase().contains("create") && ddl.toLowerCase().contains("view"),
                "the Spark output is not view DDL:\n" + ddl);
    }

    @Test
    @DisplayName("a zero or negative dimension is refused rather than emitted")
    void refusesAnImpossibleWidth() {
        // The endpoint maps dimension 0 to a 409 rather than calling through, but the generator
        // refusing as well means a future caller cannot emit a view whose guard can never pass.
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> SqlViewGenerator.trino(TABLE, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> SqlViewGenerator.spark(TABLE, -1));
    }
}
