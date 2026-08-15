# Project 4 — Smart City IoT Traffic Grid Optimizer

## Overview

This project implements a fault-tolerant data pipeline for the Smart City IoT Traffic Grid Optimizer scenario. The pipeline extracts traffic-camera records from the SQLite OLTP source, validates and sanitizes the records, quarantines corrupted sensor rows, and produces an hourly traffic-throughput analytical dataset in Snappy-compressed Apache Parquet format using Hive-style partitions.

## Source

- Database: `shop_oltp_p4.db`
- Table: `orders`
- Expected source rows: 4,000

## Pipeline

1. Extract raw traffic-camera records from SQLite using `sqlite3` and pandas.
2. Validate `camera_node_id`, `intersection_zone`, `vehicle_count`, and `order_date`.
3. Quarantine rows with missing/blank sensor identifiers, negative vehicle counts, extreme positive sensor outliers, or malformed structural values.
4. Normalize intersection labels by trimming whitespace and standardizing casing.
5. Aggregate total vehicle throughput and sensor-reading counts by intersection zone and hour.
6. Serialize the analytical result as Snappy-compressed Apache Parquet.
7. Write the analytical dataset using Hive-style partitions by intersection zone, year, and month.
8. Replace the previous analytical dataset on each run to prevent duplicate output files or rows.
9. Overwrite the quarantine CSV with the current run's rejected records.
10. Record timestamped execution events in `logs/traffic_grid.log`.

## Validation and Quarantine

The generated source dataset contains 4,000 records, including approximately 10% intentionally corrupted records.

The validated pipeline currently produces:

- 3,600 clean records
- 400 quarantined records

Quarantine breakdown:

| Validation failure | Records |
|---|---:|
| NULL or blank `camera_node_id` | 200 |
| Negative `vehicle_count` | 120 |
| Extreme `vehicle_count` outlier (>= 5,000) | 80 |
| **Total** | **400** |

Rejected records are preserved in:

`quarantine/null_sensor_nodes.csv`

Each quarantined record includes a `quarantine_reason` field explaining why the record was rejected.

## Analytical Output

The pipeline produces:

`data/analytics/hourly_grid_throughput.parquet/`

The Parquet dataset uses:

- Apache Parquet format
- Snappy compression
- Hive-style partitioning:
  - `intersection_zone`
  - `year`
  - `month`

The analytical dataset currently contains 3,600 hourly aggregation rows.

## Logging

Pipeline execution events are written to:

`logs/traffic_grid.log`

The log records major pipeline stages, validation results, quarantine output, Parquet generation, and final pipeline status.

## Idempotency

The analytical Parquet output is cleared and rebuilt on each execution. The quarantine CSV is overwritten with the current run's rejected records.

Repeated executions therefore do not accumulate duplicate analytical records or quarantine rows.

A repeated pipeline execution was tested successfully:

```text
source=4000
clean=3600
quarantine=400
hourly_rows=3600