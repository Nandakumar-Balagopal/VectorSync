package io.vectorsync.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ChangeEvent {
    public static final String OPERATION_INSERT = "INSERT";
    public static final String OPERATION_UPDATE = "UPDATE";
    public static final String OPERATION_DELETE = "DELETE";

    private String tableId;
    private long snapshotId;
    private long previousSnapshotId;
    private String operation;
    private Map<String, Object> rowData;
    private Map<String, Object> previousRowData;
    private Instant detectedAt;
}
