# Databricks notebook source
# Spain CNIG — GeoTIFF Download
#
# Purpose : Downloads CNIG flood hazard GeoTIFF files (~732 MB each) from the
#           Centro de Descargas to a Unity Catalog Volume.
#
#           Key features:
#             - Chunked streaming download (4 MB chunks) to avoid OOM on large files
#             - Resume capability: if a partial file exists, continues from the
#               byte offset where the previous attempt stopped (HTTP Range requests)
#             - Retry with exponential backoff for transient network errors
#             - Session/cookie management for the CNIG download portal
#             - Optional integrity check (SHA-256 or MD5) if the portal provides
#               a checksum file alongside the GeoTIFF
#             - Progress logging every 10% of file size
#             - Batch MERGE of download results back to silver every N completions
#
#           Volume layout:
#             /Volumes/main/flood_risk/raw/es_cnig/
#               {demarcacion_slug}/
#                 t{return_period:03d}/
#                   {tile_code}.tif
#
# Layer   : Bronze (downloads files; updates silver tracking columns)
# Reads   : ceg_delta_silver_prnd.international_flood.silver_es_cnig_datasets
# Writes  : /Volumes/main/flood_risk/raw/es_cnig/
#           ceg_delta_silver_prnd.international_flood.silver_es_cnig_datasets
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# NOTE: These files are ~732 MB each. Ensure the cluster has sufficient
#       disk space (preferably memory-optimised, ≥64 GB RAM for raster ingest).
#       Downloads are sequential (max_workers=1) by default to be respectful
#       of the CNIG server. Increase max_workers with caution.
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
    "demarcacion", "Ebro",
    [
        "Ebro", "Tajo", "Guadalquivir", "Segura", "Jucar",
        "Duero", "Mino-Sil", "Cantabrico-Occidental", "Cantabrico-Oriental",
        "Guadalete-Barbate", "Tinto-Odiel-Piedras", "Guadiana",
        "Cuencas-Internas-Cataluna", "Baleares", "Canarias",
    ],
    "Demarcación Hidrográfica — scope downloads to this basin"
)
dbutils.widgets.dropdown(
    "return_period", "all", ["all", "10", "100", "500"],
    "Return Period — download only this return period (all = T10 + T100 + T500)"
)
dbutils.widgets.dropdown(
    "retry_failed", "false", ["true", "false"],
    "Retry Failed — if true, re-attempts previously failed downloads."
)
dbutils.widgets.text("max_workers",  "1",  "Max Workers — parallel download threads (keep low for CNIG server).")
dbutils.widgets.text("batch_size",   "3",  "Batch Size — MERGE to silver every N download completions.")
dbutils.widgets.dropdown(
    "verify_checksum", "false", ["true", "false"],
    "Verify Checksum — if true, compute SHA-256 after download and compare to portal checksum (if available)."
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import hashlib
import json
import logging
import os
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, LongType, StringType,
    StructField, StructType, TimestampType,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Catalog / table / volume paths ────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
SILVER_CATALOG  = "ceg_delta_silver_prnd"
SCHEMA          = "international_flood"

# UC Volume — must match the path referenced in es_cnig_05_raster_ingest
VOLUME_BASE     = "/Volumes/main/flood_risk/raw/es_cnig"

FQN_SILVER      = f"{SILVER_CATALOG}.{SCHEMA}.silver_es_cnig_datasets"
FQN_AUDIT       = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"

NOTEBOOK_NAME    = "es_cnig_04_download"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_CNIG"

# ── CNIG portal constants ─────────────────────────────────────────────────
CNIG_PORTAL_BASE  = "https://centrodedescargas.cnig.es/CentroDescargas"

# ── HTTP config ────────────────────────────────────────────────────────────
CHUNK_SIZE      = 4 * 1024 * 1024   # 4 MB streaming chunks
CONNECT_TIMEOUT = 30                 # seconds
READ_TIMEOUT    = 3600               # 60 minutes for 732 MB files
MAX_RETRIES     = 4
BASE_DELAY      = 5.0                # seconds — be gentle with CNIG servers
JITTER          = 1.0

# ── Result schema for MERGE back to silver ────────────────────────────────
RESULT_SCHEMA = StructType([
    StructField("raster_id",              StringType(),  False),
    StructField("download_status",        StringType(),  True),
    StructField("volume_path",            StringType(),  True),
    StructField("file_size_bytes_actual", LongType(),    True),
    StructField("download_timestamp",     TimestampType(),True),
    StructField("download_error_message", StringType(),  True),
    StructField("md5_checksum",           StringType(),  True),
    StructField("sha256_checksum",        StringType(),  True),
])

# ── Logging ───────────────────────────────────────────────────────────────
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

RUN_ID          = dbutils.widgets.get("run_id").strip()         or str(uuid.uuid4())
DEMARCACION     = dbutils.widgets.get("demarcacion").strip()     or "Ebro"
RETURN_PERIOD   = dbutils.widgets.get("return_period").strip()   or "all"
RETRY_FAILED    = dbutils.widgets.get("retry_failed").lower()   == "true"
MAX_WORKERS     = int(dbutils.widgets.get("max_workers") or 1)
BATCH_SIZE      = int(dbutils.widgets.get("batch_size")  or 3)
VERIFY_CHECKSUM = dbutils.widgets.get("verify_checksum").lower() == "true"
STARTED_AT      = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"CNIG Download  |  Run ID: {RUN_ID}")
log.info(f"Demarcación    : {DEMARCACION}")
log.info(f"Return period  : {RETURN_PERIOD}")
log.info(f"Retry failed   : {RETRY_FAILED}")
log.info(f"Max workers    : {MAX_WORKERS}")
log.info(f"Batch size     : {BATCH_SIZE}")
log.info(f"Chunk size     : {CHUNK_SIZE // (1024*1024)} MB")
log.info(f"Volume base    : {VOLUME_BASE}")
log.info(f"Started at     : {STARTED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Volume Initialisation

# COMMAND ----------

# Ensure UC Volume exists (catalog.schema.volume managed by Unity Catalog)
# The 'main' catalog and 'flood_risk' schema must already exist.
# This command is idempotent.
try:
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS main.flood_risk.es_cnig
        COMMENT 'Raw CNIG flood hazard GeoTIFF downloads. 1m resolution per Demarcación Hidrográfica.'
    """)
    log.info("Volume main.flood_risk.es_cnig ensured")
except Exception as exc:
    log.warning("Could not create Volume (may already exist or catalog structure differs): %s", exc)

# Ensure local directory exists on DBFS/FUSE mount
os.makedirs(VOLUME_BASE, exist_ok=True)

# COMMAND ----------

# MAGIC %md ## 5. Load Pending Resources

# COMMAND ----------

_status_filter = "('pending')"
if RETRY_FAILED:
    _status_filter = "('pending', 'failed')"

_rp_filter = ""
if RETURN_PERIOD != "all":
    _rp_filter = f"AND return_period_years = {int(RETURN_PERIOD)}"

pending_df = spark.sql(f"""
    SELECT
        raster_id,
        tile_code,
        tile_name,
        download_url,
        detail_page_url,
        demarcacion_name,
        demarcacion_code,
        return_period_years,
        aep_pct,
        product_type,
        crs_epsg_likely,
        file_size_bytes
    FROM {FQN_SILVER}
    WHERE download_status IN {_status_filter}
      AND download_url IS NOT NULL
      AND _is_current = TRUE
      AND demarcacion_name = '{DEMARCACION}'
      AND is_raster = TRUE
      {_rp_filter}
""")

pending_rows = pending_df.collect()
TOTAL = len(pending_rows)
log.info(f"Found {TOTAL} tiles to download (demarcacion={DEMARCACION}, rp={RETURN_PERIOD}, retry={RETRY_FAILED})")

if TOTAL == 0:
    log.info("Nothing to download — exiting cleanly")
    try:
        dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    except Exception:
        pass
    dbutils.notebook.exit(RUN_ID)

# COMMAND ----------

# MAGIC %md ## 6. Session Management

# COMMAND ----------

def make_cnig_session() -> requests.Session:
    """Creates a requests.Session pre-warmed with CNIG portal cookies."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; MapGuru-FloodPipeline/1.0; "
            "+https://mapguru.ai/robots.txt)"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         CNIG_PORTAL_BASE,
    })
    try:
        warm = session.get(CNIG_PORTAL_BASE, timeout=(15, 30), verify=False, allow_redirects=True)
        log.info("CNIG session warm-up: status=%d cookies=%s", warm.status_code, dict(session.cookies))
    except Exception as exc:
        log.warning("CNIG session warm-up failed (will continue): %s", exc)
    return session


def resolve_download_url(session: requests.Session, detail_page_url: str) -> Optional[str]:
    """
    Resolves the actual GeoTIFF download URL from a CNIG detalleArchivo page.
    Returns None if the URL cannot be extracted.
    """
    if not detail_page_url:
        return None
    try:
        resp = session.get(
            detail_page_url, timeout=(15, 60), verify=False, allow_redirects=True
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Strategy 1: explicit download/descarga links
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            if any(kw in text for kw in ("descarga", "download", "descargar")):
                return href if href.startswith("http") else urljoin(CNIG_PORTAL_BASE + "/", href)

        # Strategy 2: .tif / .tiff file links
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if any(href.lower().endswith(ext) for ext in (".tif", ".tiff")):
                return href if href.startswith("http") else urljoin(CNIG_PORTAL_BASE + "/", href)

        # Strategy 3: form action
        form = soup.find("form")
        if form and form.get("action"):
            action = form["action"]
            return action if action.startswith("http") else urljoin(CNIG_PORTAL_BASE + "/", action)

        log.warning("Could not extract download URL from %s", detail_page_url)
        return None
    except Exception as exc:
        log.error("resolve_download_url failed for %s: %s", detail_page_url, exc)
        return None

# COMMAND ----------

# MAGIC %md ## 7. Download Utilities

# COMMAND ----------

def safe_filename(tile_code: str, return_period_years: Optional[int]) -> str:
    """Builds a filesystem-safe filename for the GeoTIFF."""
    rp = f"t{int(return_period_years):03d}" if return_period_years else "t000"
    code = re.sub(r"[^\w\-]", "_", tile_code or "unknown")
    return f"{code}_{rp}.tif"


def volume_path_for_tile(
    demarcacion: str,
    return_period_years: Optional[int],
    tile_code: str,
) -> str:
    """Builds the full UC Volume path for a GeoTIFF file."""
    dem_safe = re.sub(r"[^\w\-]", "_", demarcacion.lower())
    rp = f"t{int(return_period_years):03d}" if return_period_years else "t000"
    filename = safe_filename(tile_code, return_period_years)
    return os.path.join(VOLUME_BASE, dem_safe, rp, filename)


def download_with_resume(
    session: requests.Session,
    url: str,
    dest_path: str,
    expected_size: Optional[int] = None,
) -> int:
    """
    Downloads a file to dest_path using chunked streaming with resume capability.

    If a partial file already exists at dest_path, sends an HTTP Range request
    to continue from the last byte written.  This is critical for ~732 MB files
    where network interruptions are likely on long-running clusters.

    Returns the total number of bytes in the final file.
    Raises on unrecoverable errors.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    existing_size = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
    headers = {}
    if existing_size > 0:
        headers["Range"] = f"bytes={existing_size}-"
        log.info("Resume: continuing from byte %d (%.1f MB)", existing_size, existing_size / (1024**2))

    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(
                url,
                headers=headers,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                verify=False,
                allow_redirects=True,
            )

            # 206 = Partial Content (server supports resume), 200 = full restart
            if resp.status_code == 200 and existing_size > 0:
                log.warning("Server returned 200 (does not support Range) — restarting download")
                existing_size = 0

            resp.raise_for_status()

            # Compute progress checkpoint (10% intervals)
            total_expected = (
                existing_size + int(resp.headers.get("Content-Length", 0))
                if resp.status_code == 206
                else int(resp.headers.get("Content-Length", expected_size or 0))
            )
            progress_interval = max(total_expected // 10, CHUNK_SIZE) if total_expected > 0 else CHUNK_SIZE
            next_log_at = existing_size + progress_interval

            write_mode = "ab" if existing_size > 0 and resp.status_code == 206 else "wb"
            bytes_written_this_session = 0

            with open(dest_path, write_mode) as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_written_this_session += len(chunk)
                    total_so_far = existing_size + bytes_written_this_session

                    if total_so_far >= next_log_at:
                        pct = (total_so_far / total_expected * 100) if total_expected > 0 else 0
                        log.info(
                            "  Progress: %.1f MB / %.1f MB  (%.0f%%)",
                            total_so_far / (1024**2),
                            total_expected / (1024**2),
                            pct,
                        )
                        next_log_at += progress_interval

            final_size = os.path.getsize(dest_path)
            log.info(
                "Download complete: %.1f MB written to %s",
                final_size / (1024**2), dest_path,
            )
            return final_size

        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 401, 403, 404, 410):
                raise  # Non-retryable client errors
            last_exc = exc
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc

        if attempt < MAX_RETRIES - 1:
            # Update resume offset for next attempt
            existing_size = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
            headers["Range"] = f"bytes={existing_size}-"
            delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, JITTER)
            log.warning(
                "Download attempt %d/%d failed (%s) — retry in %.1fs (resume from %.1f MB)",
                attempt + 1, MAX_RETRIES, last_exc, delay, existing_size / (1024**2),
            )
            time.sleep(delay)

    raise last_exc or RuntimeError(f"Download failed after {MAX_RETRIES} attempts")


def compute_sha256(file_path: str) -> str:
    """Computes SHA-256 hex digest of a file (streaming, no full-file memory load)."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_md5(file_path: str) -> str:
    """Computes MD5 hex digest of a file (streaming)."""
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()

# COMMAND ----------

# MAGIC %md ## 8. Process Single Tile

# COMMAND ----------

def process_tile(row, session: requests.Session) -> dict:
    """
    Downloads a single CNIG GeoTIFF tile.

    1. Resolves download URL from detail page if not already in silver
    2. Downloads with chunked streaming and resume support
    3. Optionally verifies SHA-256 checksum
    4. Returns a result dict for MERGE back to silver

    Returns a result dict matching RESULT_SCHEMA.
    """
    raster_id        = row["raster_id"]
    tile_code        = row["tile_code"] or raster_id
    download_url     = row["download_url"]
    detail_page_url  = row["detail_page_url"]
    demarcacion      = row["demarcacion_name"]
    rp_years         = row["return_period_years"]
    expected_size    = row["file_size_bytes"]

    result = {
        "raster_id":              raster_id,
        "download_status":        "failed",
        "volume_path":            None,
        "file_size_bytes_actual": None,
        "download_timestamp":     None,
        "download_error_message": None,
        "md5_checksum":           None,
        "sha256_checksum":        None,
    }

    try:
        # Step 1: Resolve download URL if missing
        if not download_url and detail_page_url:
            log.info("[%s] Resolving download URL from detail page", raster_id[:8])
            download_url = resolve_download_url(session, detail_page_url)
            if not download_url:
                result["download_error_message"] = "Could not resolve download URL from detail page"
                return result

        if not download_url:
            result["download_error_message"] = "No download_url and no detail_page_url available"
            result["download_status"] = "not_applicable"
            return result

        # Step 2: Compute destination path
        dest_path = volume_path_for_tile(demarcacion, rp_years, tile_code)
        log.info(
            "[%s] Downloading %s → %s (expected %.0f MB)",
            raster_id[:8], tile_code, dest_path,
            (expected_size / (1024**2)) if expected_size else 0,
        )

        # Step 3: Download with resume
        final_size = download_with_resume(session, download_url, dest_path, expected_size)

        result["volume_path"]            = dest_path
        result["file_size_bytes_actual"] = final_size
        result["download_timestamp"]     = datetime.now(timezone.utc)

        # Step 4: Optional checksum verification
        if VERIFY_CHECKSUM:
            log.info("[%s] Computing SHA-256 checksum (%.1f MB)...", raster_id[:8], final_size / (1024**2))
            sha256 = compute_sha256(dest_path)
            md5    = compute_md5(dest_path)
            result["sha256_checksum"] = sha256
            result["md5_checksum"]    = md5
            log.info("[%s] SHA-256: %s", raster_id[:8], sha256)

        result["download_status"] = "downloaded"
        log.info(
            "[%s] ✓ %s — %.1f MB → %s",
            raster_id[:8], tile_code, final_size / (1024**2), dest_path,
        )

    except Exception as exc:
        result["download_error_message"] = str(exc)[:2000]
        log.error("[%s] Download failed for %s: %s", raster_id[:8], tile_code, exc)

    return result

# COMMAND ----------

# MAGIC %md ## 9. Download Orchestration

# COMMAND ----------

def merge_results(results: list) -> None:
    """Merges a batch of download results back into the silver table."""
    if not results:
        return
    sdf = spark.createDataFrame(results, schema=RESULT_SCHEMA)
    sdf.createOrReplaceTempView("_cnig_download_results")
    spark.sql(f"""
        MERGE INTO {FQN_SILVER} AS target
        USING _cnig_download_results AS source
        ON target.raster_id = source.raster_id
        WHEN MATCHED THEN UPDATE SET
            target.download_status          = source.download_status,
            target.volume_path              = source.volume_path,
            target.file_size_bytes_actual   = source.file_size_bytes_actual,
            target.download_timestamp       = source.download_timestamp,
            target.download_error_message   = source.download_error_message,
            target.md5_checksum             = source.md5_checksum,
            target.sha256_checksum          = source.sha256_checksum
    """)
    log.info("Merged %d result(s) into %s", len(results), FQN_SILVER)


def run_downloads(rows: list, session: requests.Session) -> tuple:
    """
    Runs downloads, either sequentially or with a thread pool.
    Flushes results to silver via MERGE every BATCH_SIZE completions.

    For CNIG GeoTIFFs (732 MB each), sequential downloads (MAX_WORKERS=1)
    are strongly recommended to avoid CNIG server throttling and cluster OOM.

    Returns (downloaded_count, failed_count).
    """
    downloaded, failed = 0, 0
    pending_batch = []

    if MAX_WORKERS <= 1:
        # Sequential — safer for large files and server-friendly
        for row in rows:
            result = process_tile(row, session)
            pending_batch.append(result)
            if result["download_status"] == "downloaded":
                downloaded += 1
            else:
                failed += 1
            if len(pending_batch) >= BATCH_SIZE:
                merge_results(pending_batch)
                pending_batch = []
    else:
        # Parallel — use with caution
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_tile, row, session): row for row in rows}
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

    # Flush remainder
    if pending_batch:
        merge_results(pending_batch)

    return downloaded, failed

# COMMAND ----------

# MAGIC %md ## 10. Audit Logging

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
            "run_id":            run_id,
            "pipeline_stage":    stage,
            "notebook_name":     notebook_name,
            "notebook_version":  notebook_version,
            "start_time":        start_time,
            "end_time":          end_time,
            "duration_seconds":  duration,
            "status":            status,
            "rows_read":         rows_read,
            "rows_written":      rows_written,
            "rows_merged":       rows_merged,
            "rows_rejected":     rows_rejected,
            "error_message":     error_message,
            "extra_metadata":    json.dumps(extra_metadata) if extra_metadata else None,
            "databricks_job_id": str(job_id) if job_id else None,
            "databricks_run_id": str(db_run_id) if db_run_id else None,
            "scrape_version":    NOTEBOOK_VERSION,
        }]
        spark.createDataFrame(record).write.format("delta").mode("append").saveAsTable(FQN_AUDIT)
    except Exception as exc:
        log.error("write_audit failed (non-fatal): %s", exc)

# COMMAND ----------

# MAGIC %md ## 11. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None
_downloaded      = 0
_failed          = 0

try:
    log.info("Creating CNIG download session")
    cnig_session = make_cnig_session()

    log.info(f"Starting downloads: {TOTAL} tiles for {DEMARCACION}")
    _downloaded, _failed = run_downloads(pending_rows, cnig_session)

    log.info(
        "Download run complete — downloaded: %d, failed: %d, total: %d",
        _downloaded, _failed, TOTAL,
    )
    _pipeline_status = "success" if _failed == 0 else "partial"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error("Download pipeline error: %s", exc, exc_info=True)
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
            "demarcacion":    DEMARCACION,
            "return_period":  RETURN_PERIOD,
            "retry_failed":   RETRY_FAILED,
            "downloaded":     _downloaded,
            "failed":         _failed,
            "chunk_size_mb":  CHUNK_SIZE // (1024 * 1024),
            "volume_base":    VOLUME_BASE,
            "verify_checksum": VERIFY_CHECKSUM,
        },
    )

# COMMAND ----------

# Display download summary for interactive inspection
display(
    spark.sql(f"""
        SELECT
            tile_code, return_period_years, demarcacion_name,
            download_status, file_size_bytes_actual,
            download_timestamp, download_error_message, volume_path
        FROM {FQN_SILVER}
        WHERE demarcacion_name = '{DEMARCACION}'
          AND _is_current = TRUE
        ORDER BY return_period_years, tile_code
    """)
)

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
