package io.vectorsync.format.derive;

import io.vectorsync.common.Constants;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Generates the SQL view that turns a serving projection into a searchable table for engines that
 * have no vector support at all.
 *
 * <p>This is the zero-install serving path. A Trino or Spark user who already has the Iceberg
 * catalog configured can rank rows by cosine similarity without an index build, a sidecar service,
 * or a new dependency -- the projection is an ordinary Iceberg table and the arithmetic is ordinary
 * SQL. The reason to generate it rather than document it is that neither engine has a dense-vector
 * cosine function suited to an {@code array<float>} column -- Trino does, from release 465, so its
 * view is a single function call; Spark does not, so its body is a {@code reduce} over a
 * {@code zip_with} written three times (dot product and both norms), and no
 * analyst should be asked to type that correctly per query.
 *
 * <p>The view is a convenience, not the fast path: it reads every vector in the partition. It is
 * the right tool for a few million rows, an ad-hoc query, or validating an index's recall against
 * exact results; past that, use a built index.
 *
 * <h2>The placeholder is a template, not a bound parameter</h2>
 * <p>No SQL engine supports a parameterized view -- a view body is fixed at creation time -- so
 * {@value #QUERY_VECTOR_PARAMETER} is a named placeholder in generated text. Either substitute a
 * literal with {@link #bind} before executing the DDL, which pins the view to one query vector, or
 * take the {@code SELECT} body and execute it directly with the vector bound by the client (Spark
 * 3.4+ supports named parameter markers in {@code spark.sql(query, args)}). Pretending a view can
 * take a runtime argument would be the dishonest option.
 */
public final class SqlViewGenerator {

    /** Named placeholder for the query vector. Substituted by {@link #bind}. */
    public static final String QUERY_VECTOR_PARAMETER = ":query_vector";

    /** Appended to the projection's name to name its view. */
    public static final String VIEW_NAME_SUFFIX = "_cosine";

    /** Alias of the similarity column, so callers can {@code ORDER BY} it by name. */
    /**
     * Output column name. Deliberately not "cosine_similarity": Trino's built-in has that name, and
     * aliasing a column to it inside the same SELECT is legal but reads as a shadowing bug.
     */
    public static final String SIMILARITY_COLUMN = "similarity";

    /**
     * Columns the view exposes. The embedding itself is deliberately absent: shipping the vector
     * back to the client costs {@code dimension x 4} bytes per row for data nobody reads, and the
     * content hash already identifies which vector produced a hit.
     */
    private static final List<String> VIEW_COLUMNS = List.of(
            Constants.VECTOR_ID_COLUMN,
            Constants.SOURCE_TABLE_COLUMN,
            Constants.SOURCE_ROW_ID_COLUMN,
            Constants.CHUNK_ORDINAL_COLUMN,
            Constants.CONTENT_HASH_COLUMN,
            Constants.MODEL_VERSION_COLUMN,
            Constants.CONFIG_ID_COLUMN,
            Constants.TEXT_COLUMN,
            Constants.SOURCE_SNAPSHOT_ID_COLUMN,
            Constants.SOURCE_SEQUENCE_NUMBER_COLUMN,
            Constants.CREATED_AT_COLUMN);

    /**
     * One segment of a qualified name. Restricted rather than escaped: these names are concatenated
     * into SQL text, so anything that could close a quote and continue the statement is refused at
     * the boundary instead of being trusted to an escaping routine.
     */
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z0-9_$]+");

    private SqlViewGenerator() {
    }

    /** Default view name for a projection: the projection's own name plus a suffix. */
    public static String viewName(String projectionTable) {
        return projectionTable + VIEW_NAME_SUFFIX;
    }

    /**
     * Trino view ranking the projection by cosine similarity to {@value #QUERY_VECTOR_PARAMETER}.
     *
     * @param projectionTable the projection, qualified as far as the session requires
     *                        ({@code table}, {@code schema.table}, or {@code catalog.schema.table})
     * @param dimension       the projection's vector width, from
     *                        {@link ProjectionBuilder.Projection#embeddingDim()}
     */
    public static String trino(String projectionTable, int dimension) {
        return trino(projectionTable, viewName(projectionTable), dimension);
    }

    /** As {@link #trino(String, int)}, with the view placed under a name of your choosing. */
    public static String trino(String projectionTable, String viewName, int dimension) {
        String table = quote(projectionTable, '"');
        String view = quote(viewName, '"');
        requirePositiveDimension(dimension);

        StringBuilder sql = new StringBuilder();
        sql.append("-- Exact cosine similarity over ").append(projectionTable)
                .append(" (dimension ").append(dimension).append(").\n");
        sql.append("-- cosine_similarity(array(double), array(double)) is built in from Trino 465\n");
        sql.append("-- (Nov 2024); cosine_distance is available alongside it. Earlier releases only\n");
        sql.append("-- had the sparse map-based overload, which does not apply to an array column.\n");
        sql.append("-- CREATE OR REPLACE VIEW requires Trino 431 or newer; on older releases DROP\n");
        sql.append("-- VIEW first and use CREATE VIEW.\n");
        sql.append("-- ").append(QUERY_VECTOR_PARAMETER)
                .append(" is a template placeholder: substitute an ARRAY[...] literal before running\n");
        sql.append("-- this DDL, because a Trino view body cannot take a runtime parameter.\n");
        sql.append("CREATE OR REPLACE VIEW ").append(view).append(" AS\n");
        // The query vector is materialized once in a CTE rather than repeated, so substituting the
        // placeholder rewrites one occurrence.
        sql.append("WITH query AS (\n");
        sql.append("    SELECT CAST(").append(QUERY_VECTOR_PARAMETER)
                .append(" AS array(double)) AS qv\n");
        sql.append(")\n");
        sql.append("SELECT\n");
        appendColumns(sql);
        sql.append("    cosine_similarity(CAST(v.").append(Constants.EMBEDDING_COLUMN)
                .append(" AS array(double)), q.qv) AS ").append(SIMILARITY_COLUMN).append("\n");
        sql.append("FROM ").append(table).append(" v\n");
        sql.append("CROSS JOIN query q\n");
        // The built-in returns NULL on a length mismatch rather than raising, so a wrong-dimension
        // query would silently produce an all-NULL ranking. The explicit width check turns that
        // into an empty result, which is a visible failure instead of a plausible one.
        sql.append("WHERE cardinality(q.qv) = ").append(dimension).append("\n");
        return sql.toString();
    }

    /**
     * Spark SQL view ranking the projection by cosine similarity to
     * {@value #QUERY_VECTOR_PARAMETER}.
     *
     * <p>The view must be created in a catalog that can hold views. Iceberg 1.5.0's Spark
     * integration does not implement views for the Iceberg catalogs, so create it in the session
     * catalog (Hive metastore) and reference the projection by its fully qualified name --
     * {@code my_catalog.my_namespace.vectors_...} -- which is what {@code projectionTable} is for.
     */
    public static String spark(String projectionTable, int dimension) {
        return spark(projectionTable, viewName(projectionTable), dimension);
    }

    /** As {@link #spark(String, int)}, with the view placed under a name of your choosing. */
    public static String spark(String projectionTable, String viewName, int dimension) {
        String table = quote(projectionTable, '`');
        String view = quote(viewName, '`');
        requirePositiveDimension(dimension);

        StringBuilder sql = new StringBuilder();
        sql.append("-- Exact cosine similarity over ").append(projectionTable)
                .append(" (dimension ").append(dimension).append(").\n");
        sql.append("-- Requires Spark 2.4 or newer for the higher-order functions transform,\n");
        sql.append("-- zip_with and aggregate. Create this in a catalog that supports views: as of\n");
        sql.append("-- Iceberg 1.5.0 the Spark Iceberg catalogs cannot hold one, so use the session\n");
        sql.append("-- catalog and keep the projection's name fully qualified.\n");
        sql.append("-- ").append(QUERY_VECTOR_PARAMETER)
                .append(" is a template placeholder: substitute an array(...) literal before running\n");
        sql.append("-- this DDL, or run the SELECT body with spark.sql(body, args) on Spark 3.4+.\n");
        sql.append("CREATE OR REPLACE VIEW ").append(view).append(" AS\n");
        sql.append("WITH query AS (\n");
        sql.append("    SELECT CAST(").append(QUERY_VECTOR_PARAMETER)
                .append(" AS array<double>) AS qv\n");
        sql.append(")\n");
        sql.append("SELECT\n");
        appendColumns(sql);
        String embedding = "CAST(v." + Constants.EMBEDDING_COLUMN + " AS array<double>)";
        sql.append("    ").append(sparkDotProduct(embedding, "q.qv")).append("\n");
        // NULLIF keeps the result the same under both ANSI modes: without it a zero-norm vector
        // divides by zero, which is NULL with spark.sql.ansi.enabled=false and an error with it on.
        sql.append("        / NULLIF(\n");
        sql.append("            ").append(sparkNorm(embedding)).append("\n");
        sql.append("          * ").append(sparkNorm("q.qv")).append(",\n");
        sql.append("            0.0D) AS ").append(SIMILARITY_COLUMN).append("\n");
        sql.append("FROM ").append(table).append(" v\n");
        sql.append("CROSS JOIN query q\n");
        // zip_with pads the shorter array with NULL in Spark too, so the same width guard applies.
        sql.append("WHERE size(q.qv) = ").append(dimension).append("\n");
        return sql.toString();
    }

    /**
     * Substitutes a query vector literal for {@value #QUERY_VECTOR_PARAMETER}.
     *
     * @throws IllegalArgumentException if the statement has no placeholder left to fill, which
     *         otherwise yields a view that silently scores against whatever was bound last
     */
    public static String bind(String statement, String queryVectorLiteral) {
        if (!statement.contains(QUERY_VECTOR_PARAMETER)) {
            throw new IllegalArgumentException(
                    "Statement contains no " + QUERY_VECTOR_PARAMETER + " placeholder to bind");
        }
        return statement.replace(QUERY_VECTOR_PARAMETER, queryVectorLiteral);
    }

    /** {@code ARRAY[0.1, 0.2, ...]}, for {@link #bind} into a Trino statement. */
    public static String trinoVectorLiteral(float[] queryVector) {
        return vectorLiteral(queryVector, "ARRAY[", "]");
    }

    /** {@code array(0.1, 0.2, ...)}, for {@link #bind} into a Spark statement. */
    public static String sparkVectorLiteral(float[] queryVector) {
        return vectorLiteral(queryVector, "array(", ")");
    }

    private static void appendColumns(StringBuilder sql) {
        for (String column : VIEW_COLUMNS) {
            sql.append("    v.").append(column).append(",\n");
        }
    }

    /**
     * {@code reduce(zip_with(a, b, multiply), zero, add, identity)} -- Trino's reduce takes an
     * output function as its fourth argument even when the state is already the result.
     *
     * <p>Both operands must already be {@code array(double)}: the stored column is
     * {@code array(real)}, and folding in float32 accumulates visible error over hundreds of terms.
     *
     * <p>The initial state is {@code CAST(0 AS double)} rather than {@code 0.0} because an
     * unsuffixed decimal literal is {@code DECIMAL(1,1)} in Trino and {@code reduce} takes its
     * accumulator type from that literal. The combining lambda would then have to fold a double
     * sum back into one decimal digit, for which there is no implicit coercion, so the whole
     * function fails to resolve and the DDL is rejected at view creation.
     */


    /** Spark spells the same fold {@code aggregate}, with no output function. */
    private static String sparkDotProduct(String left, String right) {
        return "aggregate(zip_with(" + left + ", " + right
                + ", (e, p) -> e * p), CAST(0 AS double), (s, x) -> s + x)";
    }

    private static String sparkNorm(String vector) {
        return "sqrt(aggregate(transform(" + vector + ", e -> e * e),"
                + " CAST(0 AS double), (s, x) -> s + x))";
    }

    private static String vectorLiteral(float[] queryVector, String open, String close) {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("Query vector is empty");
        }

        StringBuilder literal = new StringBuilder(open);
        for (int i = 0; i < queryVector.length; i++) {
            float value = queryVector[i];
            // A NaN or an infinity here would not be a literal in either dialect and would break
            // the statement at parse time, a long way from the code that produced the value.
            if (!Float.isFinite(value)) {
                throw new IllegalArgumentException(
                        "Query vector element " + i + " is not finite: " + value);
            }
            if (i > 0) {
                literal.append(", ");
            }
            // Float.toString, not the raw float: it emits the shortest decimal that round-trips,
            // so the literal the engine parses is the value that was intended.
            literal.append(Float.toString(value));
        }
        return literal.append(close).toString();
    }

    /**
     * Quotes a possibly qualified name segment by segment. Quoting the whole string would make
     * {@code catalog.schema.table} one identifier containing dots rather than three names.
     */
    private static String quote(String name, char quoteChar) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Table or view name is required");
        }

        String[] segments = name.split("\\.", -1);
        StringBuilder quoted = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (!SAFE_IDENTIFIER.matcher(segments[i]).matches()) {
                throw new IllegalArgumentException(
                        "Unsafe SQL identifier segment in \"" + name + "\": " + segments[i]);
            }
            if (i > 0) {
                quoted.append('.');
            }
            quoted.append(quoteChar).append(segments[i]).append(quoteChar);
        }
        return quoted.toString();
    }

    private static void requirePositiveDimension(int dimension) {
        if (dimension <= 0) {
            // A dimension of zero means the projection is empty, and a view generated from it would
            // have a width guard no query could satisfy.
            throw new IllegalArgumentException(
                    "Vector dimension must be positive, got " + dimension);
        }
    }
}
