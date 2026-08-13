# VectorSync Alert Types - Comprehensive Guide

## Overview

The VectorSync Alerts system monitors the entire vector synchronization pipeline and generates alerts for operational issues, performance degradation, and informational events.

## Alert Severity Levels

### 🔴 Critical (Red)
System-breaking issues that require immediate attention. These alerts indicate failures that prevent normal operation.

### 🟡 Warning (Yellow)
Performance degradation or potential issues that should be addressed soon but don't prevent operation.

### 🔵 Info (Blue)
Informational notifications about system events, completions, and state changes.

---

## Alert Categories

### 1. **Embedding Service Alerts**

#### Critical
- **Embedding service unreachable** - Multiple consecutive connection failures
  - Example: "Embedding service unreachable - 3 consecutive failures"
  - Action: Check embedding service health, network connectivity, API keys

- **Embedding API rate limit exceeded** - Too many requests to external API
  - Example: "OpenAI API rate limit exceeded - 429 errors"
  - Action: Reduce request rate, upgrade API tier, implement backoff

- **Embedding dimension mismatch** - Model returns wrong vector size
  - Example: "Expected 384 dimensions, got 768 from model"
  - Action: Verify model configuration, update table schema

#### Warning
- **High embedding failure rate** - Percentage of failed embeddings exceeds threshold
  - Example: "High embedding failure rate (12%) for table: products"
  - Action: Check data quality, model compatibility, API quotas

- **Embedding latency spike** - P95 latency exceeds normal range
  - Example: "Embedding P95 latency: 2.5s (normal: 450ms)"
  - Action: Check embedding service load, network latency

#### Info
- **Embedding model updated** - New model deployed
  - Example: "Embedding model updated: sentence-transformers/all-mpnet-base-v2"
  - Action: Monitor for compatibility issues

- **Batch embedding completed** - Large batch job finished
  - Example: "Batch embedding job completed: 10,000 rows in 45 seconds"
  - Action: None - informational only

---

### 2. **CDC (Change Data Capture) Alerts**

#### Critical
- **CDC processing stopped** - No new snapshots detected
  - Example: "No CDC activity for 30 minutes on table: orders"
  - Action: Check Iceberg catalog connectivity, snapshot generation

- **Iceberg metadata corruption** - Unable to read table metadata
  - Example: "Failed to read Iceberg metadata for table: products"
  - Action: Verify S3/storage access, check Iceberg catalog

#### Warning
- **Table sync lag exceeds threshold** - Delay between source and vectors
  - Example: "Table sync lag exceeds 15 minutes: reviews"
  - Action: Increase worker capacity, check CDC processing speed

- **CDC processing backlog** - Large number of pending changes
  - Example: "CDC processing backlog: 5,000 pending rows across 3 tables"
  - Action: Scale workers, optimize processing pipeline

- **Snapshot retention policy triggered** - Old snapshots cleaned up
  - Example: "Iceberg snapshot retention policy triggered - 50 old snapshots deleted"
  - Action: None - normal maintenance

#### Info
- **New snapshot detected** - CDC captured new changes
  - Example: "New snapshot 12345 detected for table: products (500 rows changed)"
  - Action: None - normal operation

---

### 3. **Index (HNSW) Alerts**

#### Critical
- **Index corrupted** - HNSW index file damaged or unreadable
  - Example: "HNSW index corrupted for table: products"
  - Action: Rebuild index immediately, check storage integrity

- **Index build failed** - Unable to create new index
  - Example: "Index build failed for table: reviews - out of memory"
  - Action: Increase memory, reduce index parameters (M, efConstruction)

#### Warning
- **Index freshness below threshold** - Index out of sync with data
  - Example: "Index freshness below 85% for table: orders"
  - Action: Trigger index rebuild, check sync frequency

- **Index query performance degraded** - Search latency increased
  - Example: "Index query P95 latency: 850ms (normal: 280ms)"
  - Action: Rebuild index, check index parameters

#### Info
- **Index rebuild completed** - Scheduled maintenance finished
  - Example: "Scheduled index rebuild completed for table: products"
  - Action: None - normal maintenance

- **Index parameters updated** - HNSW configuration changed
  - Example: "Index M parameter updated: 16 → 32 for table: products"
  - Action: Monitor query performance

---

### 4. **Worker & Resource Alerts**

#### Critical
- **Worker pool exhausted** - No available workers
  - Example: "Worker pool exhausted - 0 available workers"
  - Action: Scale worker pool, check for stuck jobs

- **Worker crashed** - Worker process terminated unexpectedly
  - Example: "Worker-2 crashed with exit code 137 (OOM)"
  - Action: Increase worker memory, investigate memory leaks

- **Storage connection lost** - Unable to access S3/object storage
  - Example: "S3 connection timeout - unable to read Iceberg metadata"
  - Action: Check network, verify credentials, check S3 service status

#### Warning
- **High memory usage** - Worker memory above threshold
  - Example: "Memory usage above 80% on worker-2"
  - Action: Monitor for memory leaks, consider scaling

- **High CPU usage** - Worker CPU above threshold
  - Example: "CPU usage above 90% on worker-1 for 5 minutes"
  - Action: Check for inefficient queries, scale workers

- **Worker heartbeat missed** - Worker not responding
  - Example: "Worker-3 missed 2 consecutive heartbeats"
  - Action: Check worker health, restart if necessary

#### Info
- **Worker recovered** - Worker returned to healthy state
  - Example: "Worker-3 successfully recovered from idle state"
  - Action: None - informational only

- **Worker scaled** - Worker pool size changed
  - Example: "Worker pool scaled: 3 → 5 workers"
  - Action: None - normal scaling operation

---

### 5. **Table Management Alerts**

#### Critical
- **Table registration failed** - Unable to register new table
  - Example: "Failed to register table: customer_feedback - schema mismatch"
  - Action: Verify table schema, check catalog connectivity

#### Warning
- **Table disabled** - Table sync disabled by user or system
  - Example: "Table sync disabled for: products (too many failures)"
  - Action: Investigate failures, re-enable when resolved

#### Info
- **New table registered** - Table added to VectorSync
  - Example: "New table registered: customer_feedback"
  - Action: None - informational only

- **Table configuration updated** - Settings changed
  - Example: "Table configuration updated: products (model changed)"
  - Action: Monitor for compatibility issues

---

### 6. **Job Queue Alerts**

#### Critical
- **Job queue deadlock** - Jobs stuck in queue
  - Example: "15 jobs stuck in queue for > 30 minutes"
  - Action: Clear stuck jobs, restart workers

#### Warning
- **High job failure rate** - Many jobs failing
  - Example: "Job failure rate: 25% (15 failed in last hour)"
  - Action: Investigate common failure causes

- **Job retry limit exceeded** - Job failed too many times
  - Example: "Job job-123 exceeded retry limit (5 attempts)"
  - Action: Manual intervention required, check job logs

#### Info
- **Job completed** - Job finished successfully
  - Example: "Embedding job job-456 completed (1,000 rows)"
  - Action: None - informational only

---

## Alert Actions

### Acknowledge
Mark alert as seen/reviewed. Doesn't dismiss the alert but indicates it's being handled.

### Dismiss
Remove alert from the list. Use for resolved issues or false positives.

### View Related
Navigate to the related table or resource for more context.

---

## Alert Thresholds (Configurable)

### Sync Lag
- **Warning**: > 10 minutes
- **Critical**: > 30 minutes

### Embedding Failure Rate
- **Warning**: > 5%
- **Critical**: > 15%

### Index Freshness
- **Warning**: < 90%
- **Critical**: < 75%

### Memory Usage
- **Warning**: > 80%
- **Critical**: > 95%

### CPU Usage
- **Warning**: > 85%
- **Critical**: > 95%

### Backlog Size
- **Warning**: > 1,000 rows
- **Critical**: > 10,000 rows

---

## Alert Notification Channels (Future)

### Planned Integrations
- **Email**: Send alerts to operations team
- **Slack**: Post to monitoring channel
- **PagerDuty**: Critical alerts trigger pages
- **Webhook**: Custom integrations
- **SMS**: Critical alerts only

---

## Alert Retention

- **Critical**: 30 days
- **Warning**: 14 days
- **Info**: 7 days

Acknowledged alerts are retained for audit purposes.

---

## Mock Alert Examples (Current Dashboard)

The dashboard currently shows 15 mock alerts covering all categories:

1. **3 Critical** (unacknowledged):
   - Embedding service unreachable
   - HNSW index corrupted
   - Worker pool exhausted

2. **5 Warning** (3 unacknowledged, 2 acknowledged):
   - Table sync lag
   - High embedding failure rate
   - CDC processing backlog
   - Index freshness low
   - High memory usage

3. **7 Info** (all acknowledged):
   - Index rebuild completed
   - New table registered
   - Embedding model updated
   - Worker recovered
   - Snapshot retention triggered
   - S3 connection timeout (resolved)
   - Batch embedding completed

---

## Best Practices

### For Operators
1. **Acknowledge critical alerts immediately** to show they're being handled
2. **Investigate warnings** before they become critical
3. **Review info alerts** for operational insights
4. **Set up alert rules** based on your SLAs
5. **Use "View Related"** to get context quickly

### For Developers
1. **Generate alerts early** - don't wait for catastrophic failure
2. **Include actionable information** in alert messages
3. **Link alerts to tables** when applicable
4. **Use appropriate severity levels**
5. **Test alert generation** in staging

---

*Made with Bob - VectorSync Alert System Documentation*