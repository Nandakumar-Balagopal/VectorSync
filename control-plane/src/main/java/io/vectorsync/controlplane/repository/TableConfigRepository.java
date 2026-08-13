package io.vectorsync.controlplane.repository;

import io.vectorsync.controlplane.entity.TableConfigEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TableConfigRepository extends JpaRepository<TableConfigEntity, String> {
}
