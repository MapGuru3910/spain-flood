# Databricks notebook source
# Spain SNCZI — Supplementary Document Download
#
# Purpose : Downloads supplementary PDF reports, metadata documents, and
#           non-SHP ancillary files linked from the SNCZI / MITECO portal.
#           The primary SHP ZIPs are downloaded directly in notebook 01
#           (es_snczi_01_bronze_scrape.py) as part of the bronze scrape stage.
#           This notebook handles any remaining documents discovered in the
#           bronze table that were not downloaded in notebook 01.
#
#           In the initial SNCZI implementation, the MITECO portal distributes
#           spatial data (SHP/KMZ) rather than PDF study reports. This notebook
#           will find zero pending downloads in the typical case and exit cleanly.
#           It is retained for:
#             a) Forward-compatibility: if SNCZI adds linked PDF reports or
#                technical documentation in future updates.
#             b) Re-download: if a previous run failed for any dataset.
#             c) KMZ/alternative format download: the SNCZI also distributes
#                KMZ versions; these can be enabled by changing the `formats`
#                widget below.
#
#           All downloads use chunked streaming with exponential backoff retry.
#           Results are merged back into the bronze table tracking columns.
#
# Layer   : Bronze (writes files to Volume; updates bronze download tracking)
# Reads   : ceg_delta_bronze_prnd.international_flood.bronze_es_snczi_datasets
# Writes  : /Volumes/main/flood_risk/raw/es_snczi/  (supplementary files)
#           ceg_delta_bronze_prnd.international_flood.bronze_es_snczi_datasets (status update)
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Version : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate."
)
dbutils.widgets.dropdown(
    "resource_formats", "all",
    ["all", "shp_only", "pdf_only", "kmz_only"],
    "Resource Formats — which file formats to download. 'all' downloads any pending file."
)
dbutils.widgets.dropdown(
    "retry_failed", "false", ["true", "false"],
    "Retry Failed — if true, re-attempts previously failed downloads."
)
dbutils.widgets.text("max_workers", "2", "Max Workers — parallel download threads (keep low for large files).")
dbutils.widgets.text("batch_size",  "5", "Batch Size — number of completed downloads before flushing to bronze.")

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import os
import random
import re
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, LongType, StringType,
    StructField, StructType, TimestampType,
)

# ── Catalog / table / volume paths ────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
SCHEMA          = "international_flood"
VOLUME_CATALOG  = "main"
VOLUME_SCHEMA   = "flood_risk"
VOLUME_NAME     = "raw"
VOLUME_BASE     = f"/Volumes/{VOLUME_CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}/es_snczi"

FQN_BRONZE      = f"{BRONZE_CATALOG}.{SCHEMA}.bronze_es_snczi_datasets"
FQN_AUDIT       = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"

NOTEBOOK_NAME    = "es_snczi_04_download"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_SNCZI"

# ── HTTP config ────────────────────────────────────────────────────────────
VERIFY_SSL      = True    # MITECO uses valid HTTPS certificates
MAX_RETRIES     = 3
BASE_DELAY      = 2.0
JITTER          = 0.5
CHUNK_SIZE      = 4 * 1024 * 1024  # 4 MB chunks
CONNECT_TIMEOUT = 30               # seconds
READ_TIMEOUT    = 1800             # 30 minutes — SNCZI ZIPs can be very large

# ── Result schema for MERGE back to bronze ─────────────────────────────────
RESULT_SCHEMA = StructType([
    StructField("resource_id",              StringType(),            False),
    StructField("download_status",          StringType(),            True),
    StructField("volume_path",              StringType(),            True),
    StructField("file_size_bytes_actual",   LongType(),              True),
    StructField("download_timestamp",       TimestampType(),         True),
    StructField("download_error_message",   StringType(),            True),
])

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    level=logging.INFO,
    force=True,
)
log = logging.getLogger(NOTEBOOK_NAME)

# COMMAND ----------

# MAGIC %md ## 3. Parameter Resolution

# COMMAND ----------

RUN_ID           = dbutils.widgets.get("run_id").strip()          or str(uuid.uuid4())
RESOURCE_FORMATS = dbutils.widgets.get("resource_formats")
RETRY_FAILED     = dbutils.widgets.get("retry_failed").lower()   == "true"
MAX_WORKERS      = int(dbutils.widgets.get("max_workers")  or 2)
BATCH_SIZE       = int(dbutils.widgets.get("batch_size")   or 5)
STARTED_AT       = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"ES SNCZI Supplementary Download  |  Run ID: {RUN_ID}")
log.info(f"Resource formats : {RESOURCE_FORMATS}")
log.info(f"Retry failed     : {RETRY_FAILED}")
log.info(f"Max workers      : {MAX_WORKERS}")
log.info(f"Batch size       : {BATCH_SIZE}")
log.info(f"Volume base      : {VOLUME_BASE}")
log.info(f"Started at       : {STARTED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Volume Initialisation

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {VOLUME_CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}
    COMMENT 'Raw flood risk data files — Spain SNCZI SHP ZIPs and supplementary documents.'
""")
log.info(f"Volume {VOLUME_CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME} ensured")

# COMMAND ----------

# MAGIC %md ## 5. Load Pending Resources

# COMMAND ----------

# Build the status filter based on retry_failed flag
_status_filter = "('pending')"
if RETRY_FAILED:
    _status_filter = "('pending', 'failed')"

# Build format filter
_format_filter = ""
if RESOURCE_FORMATS == "shp_only":
    _format_filter = "AND UPPER(resource_format) = 'SHP'"
elif RESOURCE_FORMATS == "pdf_only":
    _format_filter = "AND UPPER(resource_format) = 'PDF'"
elif RESOURCE_FORMATS == "kmz_only":
    _format_filter = "AND UPPER(resource_format) = 'KMZ'"

pending_df = spark.sql(f"""
    SELECT
        resource_id,
        dataset_id,
        download_url,
        filename,
        resource_format,
        return_period_yr,
        region,
        data_type
    FROM {FQN_BRONZE}
    WHERE download_status IN {_status_filter}
      AND download_url IS NOT NULL
      AND _is_current = TRUE
      AND http_status_code = 200
      {_format_filter}
""")

pending_rows = pending_df.collect()
TOTAL        = len(pending_rows)
log.info(f"Found {TOTAL} resource(s) to download (formats={RESOURCE_FORMATS!r}, retry_failed={RETRY_FAILED})")

if TOTAL == 0:
    log.info(
        "Nothing to download — this is expected if notebook 01 already downloaded all SHP ZIPs, "
        "or if SNCZI has not published any new supplementary documents. Exiting cleanly."
    )
    try:
        dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    except Exception:
        pass
    dbutils.notebook.exit(RUN_ID)

# COMMAND ----------

# MAGIC %md ## 6. Download Utilities

# COMMAND ----------

def safe_path_token(value: Optional[str], fallback: str = "unclassified") -> str:
    """Converts a value to a filesystem-safe directory token."""
    if not value:
        return fallback
    return re.sub(r"[^\w\-]", "_", str(value)).strip("_") or fallback


def derive_volume_path(row) -> str:
    """
    Derives the destination Volume path for a resource.
    Structure: {VOLUME_BASE}/{region}/t{return_period:04d}/{filename}
    """
    region        = safe_path_token(row["region"],       fallback="unknown_region")
    return_period = row["return_period_yr"]
    filename      = row["filename"] or f"{row['resource_id']}.bin"
    period_dir    = f"t{int(return_period):04d}" if return_period else "t_unknown"
    return os.path.join(VOLUME_BASE, region, period_dir, filename)


def download_file(url: str, dest_path: str) -> int:
    """
    Downloads url to dest_path using chunked streaming.
    Returns bytes written. Retries on transient errors.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            with requests.get(
                url,
                stream=True,
                verify=VERIFY_SSL,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                allow_redirects=True,
            ) as r:
                r.raise_for_status()
                bytes_written = 0
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)
                return bytes_written
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 401, 403, 404, 410):
                raise
            last_exc = exc
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc

        if attempt < MAX_RETRIES - 1:
            delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, JITTER)
            log.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed ({last_exc}), retrying in {delay:.1f}s")
            time.sleep(delay)

    raise last_exc or RuntimeError(f"All download attempts failed for {url}")


def process_resource(row) -> dict:
    """
    Downloads a single SNCZI resource to the Unity Catalog Volume.
    Returns a result dict aligned to RESULT_SCHEMA.
    """
    resource_id = row["resource_id"]
    url         = row["download_url"]
    dest_path   = derive_volume_path(row)

    result = {
        "resource_id":            resource_id,
        "download_status":        "failed",
        "volume_path":            None,
        "file_size_bytes_actual": None,
        "download_timestamp":     None,
        "download_error_message": None,
    }

    # Skip if file already exists at destination and is non-empty (idempotent)
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        size = os.path.getsize(dest_path)
        log.info(f"[{resource_id}] Already exists at {dest_path} ({size:,} bytes) — marking as downloaded")
        result["download_status"]        = "downloaded"
        result["volume_path"]            = dest_path
        result["file_size_bytes_actual"] = size
        result["download_timestamp"]     = datetime.now(timezone.utc)
        return result

    try:
        log.info(f"[{resource_id}] Downloading: {url} → {dest_path}")
        size = download_file(url, dest_path)
        result["download_status"]        = "downloaded"
        result["volume_path"]            = dest_path
        result["file_size_bytes_actual"] = size
        result["download_timestamp"]     = datetime.now(timezone.utc)
        log.info(f"[{resource_id}] OK — {size:,} bytes → {dest_path}")
    except Exception as exc:
        result["download_error_message"] = str(exc)[:2000]
        log.error(f"[{resource_id}] Download failed ({url}): {exc}")

    return result

# COMMAND ----------

# MAGIC %md ## 7. Download Orchestration

# COMMAND ----------

def merge_results(results: list) -> None:
    """Merges a batch of download results back into the bronze table."""
    if not results:
        return
    sdf = spark.createDataFrame(results, schema=RESULT_SCHEMA)
    sdf.createOrReplaceTempView("_es_snczi_download_results")
    spark.sql(f"""
        MERGE INTO {FQN_BRONZE} AS target
        USING _es_snczi_download_results AS source
        ON target.resource_id = source.resource_id
           AND target._is_current = TRUE
        WHEN MATCHED THEN UPDATE SET
            target.download_status          = source.download_status,
            target.volume_path              = source.volume_path,
            target.file_size_bytes_actual   = source.file_size_bytes_actual,
            target.download_timestamp       = source.download_timestamp,
            target.download_error_message   = source.download_error_message
    """)
    log.info(f"Merged {len(results)} result(s) into {FQN_BRONZE}")


def run_downloads(rows: list) -> tuple:
    """
    Runs all downloads in parallel using a thread pool.
    Flushes results to bronze every BATCH_SIZE completions.
    Returns (downloaded_count, failed_count).
    """
    downloaded, failed = 0, 0
    pending_batch = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_resource, row): row for row in rows}
        for future in as_completed(futures):
            result = future.result()
            pending_batch.append(result)

            if result["download_status"] == "downloaded":
                downloaded += 1
            else:
                failed += 1

            if len(pending_batch) >= BATCH_SIZE:
                merge_results(pending_batch)
                pending_batch = []

    if pending_batch:
        merge_results(pending_batch)

    return downloaded, failed

# COMMAND ----------

# MAGIC %md ## 8. Audit Logging

# COMMAND ----------

def write_audit(
    run_id, stage, notebook_name, notebook_version,
    start_time, end_time, status,
    rows_read=0, rows_written=0, rows_merged=0, rows_rejected=0,
    error_message=None, extra_metadata=None,
):
    try:
        duration = (end_time - start_time).total_seconds() if end_time and start_time else None
        job_id, db_run_id = None, None
        try:
            ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            job_id    = ctx.jobId().getOrElse(None)
            db_run_id = ctx.idInJob().getOrElse(None)
        except Exception:
            pass
        record = [{
            "run_id": run_id, "pipeline_stage": stage,
            "notebook_name": notebook_name, "notebook_version": notebook_version,
            "start_time": start_time, "end_time": end_time, "duration_seconds": duration,
            "status": status, "rows_read": rows_read, "rows_written": rows_written,
            "rows_merged": rows_merged, "rows_rejected": rows_rejected,
            "error_message": error_message,
            "extra_metadata": json.dumps(extra_metadata) if extra_metadata else None,
            "databricks_job_id": str(job_id) if job_id else None,
            "databricks_run_id": str(db_run_id) if db_run_id else None,
            "scrape_version": NOTEBOOK_VERSION,
        }]
        spark.createDataFrame(record).write.format("delta").mode("append").saveAsTable(FQN_AUDIT)
    except Exception as exc:
        log.error(f"write_audit failed (non-fatal): {exc}")

# COMMAND ----------

# MAGIC %md ## 9. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None
_downloaded      = 0
_failed          = 0

try:
    _downloaded, _failed = run_downloads(pending_rows)
    log.info(f"Download run complete — downloaded: {_downloaded}, failed: {_failed}")
    _pipeline_status = "success" if _failed == 0 else "partial"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Download pipeline error: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "download",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_read        = TOTAL,
        rows_written     = _downloaded,
        rows_rejected    = _failed,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "resource_formats": RESOURCE_FORMATS,
            "retry_failed":     RETRY_FAILED,
            "downloaded":       _downloaded,
            "failed":           _failed,
            "volume_base":      VOLUME_BASE,
            "note": (
                "SNCZI distributes SHP ZIPs (downloaded in notebook 01). "
                "This notebook handles supplementary PDFs, KMZs, and re-downloads."
            ),
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
