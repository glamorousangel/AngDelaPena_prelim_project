# DataOps Report — Project 4

## 1. Reliability Architecture

The pipeline separates extraction, validation, quarantine, transformation, and analytical storage. This creates a clear fault-isolation boundary: malformed sensor records are captured in the quarantine stream while valid records continue through the analytical path.

The pipeline processes the 4,000-row SQLite source and produces separate clean and quarantine outputs so that invalid sensor records do not terminate the complete analytical workflow.

## 2. Validation Rules

The pipeline applies the following validation rules:

- `camera_node_id` must be present and non-blank.
- `intersection_zone` is trimmed and normalized to the `Zone X` form.
- `vehicle_count` must not be negative.
- Extreme positive sensor glitches at `vehicle_count >= 5,000` are quarantined.
- `order_date` must be parseable as a timestamp.

The validated pipeline produced:

- 3,600 clean records
- 400 quarantined records

The 400 quarantined records were classified as:

| Validation failure | Records |
|---|---:|
| NULL or blank `camera_node_id` | 200 |
| Negative `vehicle_count` | 120 |
| Extreme `vehicle_count` outlier (>= 5,000) | 80 |
| **Total** | **400** |

## 3. Quarantine Strategy

Each rejected record is written to:

`quarantine/null_sensor_nodes.csv`

The quarantine file preserves the original source fields and adds a `quarantine_reason` field describing the validation failure.

The quarantine CSV is overwritten on every run so that repeated executions against the same source do not accumulate duplicate quarantine records.

## 4. Analytical Transformation and Storage

Valid records are transformed into hourly traffic-throughput aggregates.

The analytical dataset contains:

- `intersection_zone`
- `year`
- `month`
- `day`
- `hour`
- `total_vehicle_throughput`
- `sensor_readings`

The result is stored as Snappy-compressed Apache Parquet.

The Parquet dataset uses Hive-style `key=value/` partitions for:

- `intersection_zone`
- `year`
- `month`

The resulting analytical dataset contains 3,600 hourly aggregation rows.

## 5. Idempotency

The analytical Parquet directory is deleted and recreated before each successful write, while the quarantine CSV is overwritten.

Therefore, running the orchestrator multiple times against the same source dataset does not append duplicate analytical rows or quarantine records.

Idempotency was verified by executing the pipeline twice. Both executions produced:

```text
source=4000
clean=3600
quarantine=400
hourly_rows=3600