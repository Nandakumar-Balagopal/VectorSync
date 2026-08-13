package io.vectorsync.controlplane.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Entity
@Table(name = "sync_state")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SyncStateEntity {
    @Id
    @Column(name = "table_id")
    private String tableId;

    @Column(name = "last_snapshot_id")
    private Long lastSnapshotId;

    @Column(name = "last_sync_at")
    private Instant lastSyncAt;
}
