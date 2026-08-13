package io.vectorsync.worker.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SyncStateResponse {
    private String tableId;
    private Long lastSnapshotId;
    private Instant lastSyncAt;
}
