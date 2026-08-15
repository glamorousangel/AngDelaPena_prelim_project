from __future__ import annotations

import hashlib
import logging
import shutil
import sqlite3
import time
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
SOURCE_DB = PROJECT_ROOT / "shop_oltp_p4.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "analytics" / "hourly_grid_throughput.parquet"
QUARANTINE_FILE = PROJECT_ROOT / "quarantine" / "null_sensor_nodes.csv"
LOG_FILE = PROJECT_ROOT / "logs" / "traffic_grid.log"

# The generated Project 4 dataset defines the extreme positive sensor-glitch
# records at vehicle_count >= 5,000. This captures the 80 known outlier rows.
EXTREME_VEHICLE_COUNT_THRESHOLD = 5_000
EXPECTED_SOURCE_ROWS = 4_000


logger = logging.getLogger("traffic_grid")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    logger.addHandler(file_handler)


def sha256_file(path: Path) -> str:
    """Return a SHA-256 fingerprint of the source database for lineage logging."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_source() -> pd.DataFrame:
    """Extract the raw Project 4 orders table from SQLite."""
    if not SOURCE_DB.exists():
        raise FileNotFoundError(f"Source database not found: {SOURCE_DB}")

    query = """
        SELECT
            record_id,
            camera_node_id,
            intersection_zone,
            vehicle_count,
            status,
            order_date
        FROM orders
        ORDER BY record_id
    """

    with sqlite3.connect(SOURCE_DB) as connection:
        frame = pd.read_sql_query(query, connection)

    logger.info("EXTRACT | rows=%d | source_sha256=%s", len(frame), sha256_file(SOURCE_DB))
    return frame


def normalize_zone(value: object) -> str:
    """Normalize intersection labels such as ' zone a ' and 'ZONE A'."""
    if pd.isna(value):
        raise ValueError("intersection_zone is NULL")

    cleaned = str(value).strip().upper()
    if not cleaned.startswith("ZONE "):
        raise ValueError(f"Malformed intersection_zone: {value!r}")

    suffix = cleaned.removeprefix("ZONE ").strip()
    if len(suffix) != 1 or not suffix.isalpha():
        raise ValueError(f"Malformed intersection_zone: {value!r}")

    return f"Zone {suffix}"


def validate_and_clean(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate and clean source rows using vectorized pandas operations."""

    working = frame.copy()

    # Validate camera_node_id
    camera_clean = working["camera_node_id"].astype("string").str.strip()

    invalid_camera = (
        working["camera_node_id"].isna()
        | camera_clean.eq("")
    )

    # Validate vehicle_count
    vehicle_numeric = pd.to_numeric(
        working["vehicle_count"],
        errors="coerce",
    )

    invalid_vehicle = (
        vehicle_numeric.isna()
        | (vehicle_numeric < 0)
        | (vehicle_numeric >= EXTREME_VEHICLE_COUNT_THRESHOLD)
    )

    # Validate intersection_zone
    zone_clean = working["intersection_zone"].astype("string").str.strip().str.upper()

    valid_zone_format = zone_clean.str.match(
        r"^ZONE\s+[A-Z]$",
        na=False,
    )

    invalid_zone = ~valid_zone_format

    # Validate order_date
    timestamp = pd.to_datetime(
        working["order_date"],
        errors="coerce",
    )

    invalid_timestamp = timestamp.isna()

    # Build quarantine mask
    invalid_mask = (
        invalid_camera
        | invalid_vehicle
        | invalid_zone
        | invalid_timestamp
    )

    # Create clean records
    clean = working.loc[~invalid_mask].copy()

    clean["camera_node_id"] = camera_clean.loc[~invalid_mask]
    clean["intersection_zone"] = (
        zone_clean.loc[~invalid_mask]
        .str.replace(
            r"^ZONE\s+",
            "Zone ",
            regex=True,
        )
        .str.strip()
    )
    clean["vehicle_count"] = vehicle_numeric.loc[~invalid_mask].astype("int64")
    clean["order_date"] = timestamp.loc[~invalid_mask]

    # Create quarantine records
    quarantine = working.loc[invalid_mask].copy()

    def determine_reason(index: int) -> str:
        """Return the first applicable validation failure reason."""

        if invalid_camera.loc[index]:
            return "NULL or blank camera_node_id"

        if invalid_vehicle.loc[index]:
            value = vehicle_numeric.loc[index]

            if pd.isna(value):
                return "invalid vehicle_count"

            if value < 0:
                return "negative vehicle_count"

            if value >= EXTREME_VEHICLE_COUNT_THRESHOLD:
                return (
                    f"extreme vehicle_count outlier "
                    f"(>= {EXTREME_VEHICLE_COUNT_THRESHOLD})"
                )

        if invalid_zone.loc[index]:
            return "Malformed intersection_zone"

        if invalid_timestamp.loc[index]:
            return "Invalid order_date"

        return "Validation failure"

    quarantine["quarantine_reason"] = [
        determine_reason(index)
        for index in quarantine.index
    ]

    # Log quarantine summary
    reason_counts = quarantine["quarantine_reason"].value_counts()

    for reason, count in reason_counts.items():
        logger.warning(
            "QUARANTINE_SUMMARY | reason=%s | rows=%d",
            reason,
            count,
        )

    logger.info(
        "VALIDATION | valid_rows=%d | quarantined_rows=%d",
        len(clean),
        len(quarantine),
    )

    return clean.reset_index(drop=True), quarantine.reset_index(drop=True)


def write_quarantine(quarantine: pd.DataFrame) -> None:
    """Replace the previous quarantine file so repeated runs are idempotent."""
    QUARANTINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    quarantine.to_csv(QUARANTINE_FILE, index=False)
    logger.info("QUARANTINE_WRITE | path=%s | rows=%d", QUARANTINE_FILE, len(quarantine))


def aggregate_hourly(clean: pd.DataFrame) -> pd.DataFrame:
    """Aggregate total hourly vehicle throughput per intersection zone."""
    clean = clean.copy()
    clean["year"] = clean["order_date"].dt.year.astype("int16")
    clean["month"] = clean["order_date"].dt.month.astype("int8")
    clean["day"] = clean["order_date"].dt.day.astype("int8")
    clean["hour"] = clean["order_date"].dt.hour.astype("int8")

    result = (
        clean.groupby(
            ["intersection_zone", "year", "month", "day", "hour"],
            as_index=False,
            sort=True,
        )
        .agg(
            total_vehicle_throughput=("vehicle_count", "sum"),
            sensor_readings=("record_id", "count"),
        )
    )

    result["total_vehicle_throughput"] = result["total_vehicle_throughput"].astype("int64")
    result["sensor_readings"] = result["sensor_readings"].astype("int32")
    return result


def write_parquet(aggregated: pd.DataFrame) -> None:
    """Write a compressed Hive-style Parquet dataset after clearing old output."""
    OUTPUT_DIR.parent.mkdir(parents=True, exist_ok=True)

    # Remove the previous analytical dataset to prevent duplicate partitions/files.
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    aggregated.to_parquet(
        OUTPUT_DIR,
        engine="pyarrow",
        compression="snappy",
        index=False,
        partition_cols=["intersection_zone", "year", "month"],
    )

    logger.info(
        "PARQUET_WRITE | path=%s | rows=%d | "
        "compression=snappy | partitions=intersection_zone/year/month",
        OUTPUT_DIR,
        len(aggregated),
    )


def run_pipeline() -> None:
    started = time.perf_counter()
    logger.info("PIPELINE_START")

    try:
        raw = extract_source()
        if len(raw) != EXPECTED_SOURCE_ROWS:
            raise ValueError(
                f"Unexpected source row count: expected {EXPECTED_SOURCE_ROWS}, got {len(raw)}"
            )

        clean, quarantine = validate_and_clean(raw)
        write_quarantine(quarantine)

        aggregated = aggregate_hourly(clean)
        write_parquet(aggregated)

        elapsed = time.perf_counter() - started
        logger.info(
            "PIPELINE_SUCCESS | source_rows=%d | clean_rows=%d | quarantined_rows=%d | output_rows=%d | elapsed_seconds=%.4f",
            len(raw),
            len(clean),
            len(quarantine),
            len(aggregated),
            elapsed,
        )
        print(
            f"Pipeline completed successfully in {elapsed:.4f}s | "
            f"source={len(raw)} | clean={len(clean)} | quarantine={len(quarantine)} | "
            f"hourly_rows={len(aggregated)}"
        )

    except Exception:
        logger.exception("PIPELINE_FAILURE")
        raise


if __name__ == "__main__":
    run_pipeline()
