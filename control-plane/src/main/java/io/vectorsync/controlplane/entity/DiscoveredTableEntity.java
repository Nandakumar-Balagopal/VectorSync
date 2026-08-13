package io.vectorsync.controlplane.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "discovered_tables")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DiscoveredTableEntity {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String tableName;
    
    @Column(nullable = false)
    private String schemaName;
    
    @Column(nullable = false, length = 1000)
    private String location;
    
    @Column(length = 1000)
    private String warehouseUrl;
    
    @Column(nullable = false, length = 1000)
    private String metadataLocation;
    
    @Column(unique = true)
    private String uuid;
    
    @Column(columnDefinition = "TEXT")
    private String schemaJson;
    
    @Column(columnDefinition = "TEXT")
    private String partitionSpecJson;
    
    @Column(columnDefinition = "TEXT")
    private String propertiesJson;
    
    private Long totalRecords;
    
    private Long totalFiles;
    
    private Long totalSize;
    
    @Column(nullable = false)
    private LocalDateTime discoveredAt;
    
    @Column(nullable = false)
    private boolean registered;
    
    private LocalDateTime registeredAt;
    
    @Column(length = 500)
    private String syncJobId;
    
    @Column(length = 500)
    private String catalogName;
    
    @PrePersist
    protected void onCreate() {
        discoveredAt = LocalDateTime.now();
        if (registered == false) {
            registered = false;
        }
    }
}

// Made with Bob
