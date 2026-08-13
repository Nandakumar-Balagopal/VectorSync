package io.vectorsync.controlplane.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "sync_jobs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncJobEntity {
    
    @Id
    private String jobId;
    
    @Column(nullable = false)
    private String catalogName;
    
    @Column(name = "s3_path", nullable = false, length = 1000)
    private String s3Path;
    
    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private SyncStatus status;
    
    private boolean syncExistingTables;
    
    private boolean registerNewTables;
    
    @Column(nullable = false)
    private LocalDateTime startedAt;
    
    private LocalDateTime completedAt;
    
    private Integer tablesDiscovered;
    
    private Integer tablesRegistered;
    
    private Integer tablesUpdated;
    
    private Integer tablesFailed;
    
    @Column(columnDefinition = "TEXT")
    private String errorMessage;
    
    @Column(columnDefinition = "TEXT")
    private String errorDetails;
    
    private String createdBy;
    
    @PrePersist
    protected void onCreate() {
        if (startedAt == null) {
            startedAt = LocalDateTime.now();
        }
        if (status == null) {
            status = SyncStatus.RUNNING;
        }
    }
    
    public enum SyncStatus {
        RUNNING,
        COMPLETED,
        FAILED,
        CANCELLED
    }
}

// Made with Bob
