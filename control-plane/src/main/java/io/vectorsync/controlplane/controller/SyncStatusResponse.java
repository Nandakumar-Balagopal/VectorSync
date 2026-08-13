package io.vectorsync.controlplane.controller;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SyncStatusResponse {
    private String tableId;
    private String tableName;
    private boolean enabled;
    private Long lastSnapshotId;
    private Instant lastSyncAt;
}
