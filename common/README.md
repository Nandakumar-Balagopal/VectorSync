# Common Module

Shared library module containing DTOs, models, utilities, and constants used across all VectorSync services.

## Purpose

The `common` module provides a centralized location for:
- Data Transfer Objects (DTOs)
- Shared domain models
- Event models
- Constants and enums
- Utility classes
- Vector similarity helpers
- Common exceptions
- Shared configuration objects

**IMPORTANT**: This module contains **NO business logic**. It is purely for shared code that multiple services need to use.

## Module Structure

```
common/
├── dto/              # Data Transfer Objects
├── model/            # Domain models
├── event/            # Event models (for future Kafka integration)
├── constant/         # Constants and enums
├── util/             # Utility classes
├── exception/        # Common exceptions
└── config/           # Shared configuration objects
```

## Contents

### DTOs (Data Transfer Objects)

DTOs for API requests and responses:

```java
// Table registration
public class TableRegistrationRequest {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private List<String> textColumns;
    private Integer pollInterval;
}

// Search request
public class SearchRequest {
    private String query;
    private String sourceTable;
    private Integer limit;
    private Double minSimilarity;
}

// Search result
public class SearchResult {
    private String id;
    private String sourceTable;
    private Double similarity;
    private String textContent;
    private Map<String, String> metadata;
}
```

### Domain Models

Shared domain models:

```java
// Table configuration
public class TableConfig {
    private Long id;
    private String catalogName;
    private String schemaName;
    private String tableName;
    private List<String> textColumns;
    private Integer pollInterval;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}

// Sync state
public class SyncState {
    private Long id;
    private Long tableId;
    private Long lastSnapshotId;
    private LocalDateTime lastSyncTimestamp;
    private SyncStatus status;
    private String errorMessage;
}

// Worker registration
public class WorkerRegistration {
    private String workerId;
    private WorkerType workerType;
    private String host;
    private Integer port;
    private WorkerStatus status;
    private LocalDateTime lastHeartbeat;
}
```

### Event Models (TODO: Kafka Integration)

Event models for future event-driven architecture:

```java
// CDC event
public class CdcEvent {
    private String eventId;
    private String sourceTable;
    private String catalogName;
    private String schemaName;
    private Long snapshotId;
    private List<Map<String, Object>> records;
    private LocalDateTime timestamp;
}

// Embedding event
public class EmbeddingEvent {
    private String eventId;
    private String sourceTable;
    private String recordId;
    private String textContent;
    private List<Double> embedding;
    private LocalDateTime timestamp;
}

// Worker event
public class WorkerEvent {
    private String workerId;
    private WorkerEventType eventType;  // REGISTERED, HEARTBEAT, FAILED
    private LocalDateTime timestamp;
}
```

### Constants

Shared constants and enums:

```java
public class Constants {
    // Iceberg
    public static final String VECTOR_NAMESPACE = "vector";
    public static final String VECTOR_TABLE_SUFFIX = "_vectors";
    
    // Embedding
    public static final int DEFAULT_EMBEDDING_DIMENSION = 384;
    public static final int DEFAULT_BATCH_SIZE = 100;
    
    // Polling
    public static final int DEFAULT_POLL_INTERVAL_MS = 30000;
    public static final int DEFAULT_LEASE_TTL_MS = 60000;
}

public enum SyncStatus {
    PENDING,
    SYNCING,
    SYNCED,
    FAILED
}

public enum WorkerType {
    CDC,
    EMBEDDING
}

public enum WorkerStatus {
    ACTIVE,
    INACTIVE,
    FAILED
}
```

### Utilities

Utility classes for common operations:

```java
// Vector similarity
public class VectorUtils {
    public static double cosineSimilarity(List<Double> a, List<Double> b) {
        // Calculate cosine similarity
    }
    
    public static List<Double> normalize(List<Double> vector) {
        // Normalize vector to unit length
    }
    
    public static double euclideanDistance(List<Double> a, List<Double> b) {
        // Calculate Euclidean distance
    }
}

// JSON utilities
public class JsonUtils {
    public static String toJson(Object obj) {
        // Convert object to JSON
    }
    
    public static <T> T fromJson(String json, Class<T> clazz) {
        // Parse JSON to object
    }
}

// Validation utilities
public class ValidationUtils {
    public static boolean isValidTableName(String name) {
        // Validate table name format
    }
    
    public static boolean isValidEmbedding(List<Double> embedding, int expectedDim) {
        // Validate embedding dimensions
    }
}
```

### Exceptions

Common exception classes:

```java
// Base exception
public class VectorSyncException extends RuntimeException {
    public VectorSyncException(String message) {
        super(message);
    }
    
    public VectorSyncException(String message, Throwable cause) {
        super(message, cause);
    }
}

// Specific exceptions
public class TableNotFoundException extends VectorSyncException {
    public TableNotFoundException(String tableName) {
        super("Table not found: " + tableName);
    }
}

public class EmbeddingException extends VectorSyncException {
    public EmbeddingException(String message) {
        super(message);
    }
}

public class IcebergException extends VectorSyncException {
    public IcebergException(String message, Throwable cause) {
        super(message, cause);
    }
}

public class WorkerException extends VectorSyncException {
    public WorkerException(String message) {
        super(message);
    }
}
```

### Configuration Objects

Shared configuration classes:

```java
// Iceberg configuration
public class IcebergConfig {
    private String warehouse;
    private String vectorNamespace;
    private Map<String, String> catalogProperties;
}

// Embedding configuration
public class EmbeddingConfig {
    private String providerType;
    private String externalUrl;
    private Integer timeout;
    private Integer batchSize;
}

// Search configuration
public class SearchConfig {
    private Integer defaultLimit;
    private Integer maxLimit;
    private Double minSimilarity;
}
```

## Usage

### Adding as Dependency

In other modules' `pom.xml`:

```xml
<dependency>
    <groupId>io.vectorsync</groupId>
    <artifactId>vectorsync-common</artifactId>
    <version>${project.version}</version>
</dependency>
```

### Using DTOs

```java
// In control-plane
@PostMapping("/api/tables")
public ResponseEntity<TableConfig> registerTable(
    @RequestBody TableRegistrationRequest request) {
    // Use DTO from common module
}

// In search-service
@PostMapping("/api/search")
public ResponseEntity<List<SearchResult>> search(
    @RequestBody SearchRequest request) {
    // Use DTOs from common module
}
```

### Using Utilities

```java
// In embedding-worker
import io.vectorsync.common.util.VectorUtils;

double similarity = VectorUtils.cosineSimilarity(embedding1, embedding2);
List<Double> normalized = VectorUtils.normalize(embedding);
```

### Using Constants

```java
// In cdc-worker
import io.vectorsync.common.constant.Constants;

String vectorTable = tableName + Constants.VECTOR_TABLE_SUFFIX;
int pollInterval = Constants.DEFAULT_POLL_INTERVAL_MS;
```

## Design Principles

### 1. No Business Logic
The common module should contain **only** shared code. Business logic belongs in the service modules.

**Good:**
```java
// Utility for vector operations
public class VectorUtils {
    public static double cosineSimilarity(List<Double> a, List<Double> b) {
        // Pure calculation, no business logic
    }
}
```

**Bad:**
```java
// Business logic doesn't belong here
public class VectorService {
    public void generateAndStoreEmbeddings(String text) {
        // This is business logic - belongs in embedding-worker
    }
}
```

### 2. Immutable DTOs
Use immutable DTOs with builders:

```java
@Value
@Builder
public class SearchRequest {
    String query;
    String sourceTable;
    Integer limit;
    Double minSimilarity;
}
```

### 3. Clear Naming
Use descriptive names that indicate purpose:
- `*Request` - API request DTOs
- `*Response` - API response DTOs
- `*Event` - Event models for messaging
- `*Config` - Configuration objects
- `*Exception` - Exception classes
- `*Utils` - Utility classes

### 4. Minimal Dependencies
Keep dependencies minimal. Only include libraries that are truly shared across all services.

Current dependencies:
- Lombok (for reducing boilerplate)
- Jackson (for JSON serialization)
- SLF4J (for logging interfaces)

### 5. Versioning
The common module version should match the parent project version. All services should use the same version.

## Building

```bash
# Build common module
mvn clean install

# This installs the JAR to local Maven repository
# Other modules can then depend on it
```

## Testing

```bash
# Run tests
mvn test

# Generate coverage report
mvn verify
```

## Future Enhancements

### Event Models (TODO: Kafka Integration)
When Kafka is integrated, add comprehensive event models:
- CDC events for change notifications
- Embedding events for vector generation
- Worker events for coordination
- Search events for analytics

### Validation Annotations (TODO)
Add JSR-303 validation annotations to DTOs:
```java
public class TableRegistrationRequest {
    @NotBlank
    private String catalogName;
    
    @NotBlank
    private String schemaName;
    
    @NotBlank
    private String tableName;
    
    @NotEmpty
    private List<String> textColumns;
    
    @Min(1000)
    private Integer pollInterval;
}
```

### Metrics Models (TODO)
Add models for metrics and monitoring:
```java
public class WorkerMetrics {
    private String workerId;
    private Long tablesAssigned;
    private Long eventsProcessed;
    private Double avgProcessingTime;
    private LocalDateTime timestamp;
}
```

## Best Practices

1. **Keep it simple** - Don't add unnecessary abstractions
2. **Document everything** - Clear javadocs for all public APIs
3. **Test thoroughly** - High test coverage for utilities
4. **Version carefully** - Breaking changes require major version bump
5. **Review dependencies** - Minimize transitive dependencies
6. **Use interfaces** - Define contracts, not implementations
7. **Avoid coupling** - Don't reference service-specific code

## Troubleshooting

### Dependency conflicts
If you see dependency conflicts, check:
- All services use the same common module version
- No circular dependencies between modules
- Transitive dependencies are properly managed

### ClassNotFoundException
If classes from common are not found:
- Ensure `mvn install` was run on common module
- Check the dependency is correctly declared in pom.xml
- Verify the version matches

### Serialization issues
If JSON serialization fails:
- Ensure Jackson annotations are correct
- Check for circular references in models
- Verify all fields are serializable