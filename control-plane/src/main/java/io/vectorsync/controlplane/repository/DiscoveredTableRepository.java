package io.vectorsync.controlplane.repository;

import io.vectorsync.controlplane.entity.DiscoveredTableEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DiscoveredTableRepository extends JpaRepository<DiscoveredTableEntity, Long> {
    
    Optional<DiscoveredTableEntity> findByUuid(String uuid);
    
    List<DiscoveredTableEntity> findBySyncJobId(String syncJobId);
    
    List<DiscoveredTableEntity> findByRegistered(boolean registered);
    
    List<DiscoveredTableEntity> findByCatalogName(String catalogName);
    
    List<DiscoveredTableEntity> findBySchemaNameAndTableName(String schemaName, String tableName);
    
    boolean existsByUuid(String uuid);
    
    long countByRegistered(boolean registered);
    
    long countBySyncJobId(String syncJobId);
}

// Made with Bob
