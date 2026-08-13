package io.vectorsync.controlplane.repository;

import io.vectorsync.controlplane.entity.SyncStateEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SyncStateRepository extends JpaRepository<SyncStateEntity, String> {
}
