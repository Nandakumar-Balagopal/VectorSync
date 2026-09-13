package io.vectorsync.format.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * One entry in the alias log: at this moment, this alias pointed at this index.
 *
 * <p>The log is append-only, so promotion history is auditable and a rollback is just another
 * append naming the earlier index.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexAliasEntry {

    /** Logical serving name, e.g. {@code production}. */
    private String aliasName;

    private String sourceTable;

    /** The index this alias resolved to. */
    private String indexId;

    private Instant updatedAt;
    private String updatedBy;

    /** Free-text reason, e.g. "promote v2 after recall@10 81% vs 72%". */
    private String note;
}
