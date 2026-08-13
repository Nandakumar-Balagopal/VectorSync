package io.vectorsync.cdcworker.snapshot;

import io.vectorsync.common.dto.ChangeEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CdcResult {
    private List<ChangeEvent> changeEvents;
    private Long currentSnapshotId;
    private Long previousSnapshotId;
}
