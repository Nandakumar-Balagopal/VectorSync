package io.vectorsync.controlplane.repository;

import io.vectorsync.controlplane.entity.SyncJobEntity;
import io.vectorsync.controlplane.entity.SyncJobEntity.SyncStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface SyncJobRepository extends JpaRepository<SyncJobEntity, String> {
    
    List<SyncJobEntity> findByStatus(SyncStatus status);
    
    List<SyncJobEntity> findByCatalogName(String catalogName);
    
    List<SyncJobEntity> findByStartedAtAfter(LocalDateTime startedAt);
    
    List<SyncJobEntity> findByStatusOrderByStartedAtDesc(SyncStatus status);
    
    List<SyncJobEntity> findAllByOrderByStartedAtDesc();
}

// Made with Bob
