# Databricks notebook source
# Spain SNCZI — Bronze Scrape
#
# Purpose : Enumerates and downloads SHP ZIP files from Spain's Sistema Nacional
#           de Cartografía de Zonas Inundables (SNCZI), operated by MITECO.
#           Records metadata for each dataset in the bronze Delta table and
#           copies raw ZIP files to a Unity Catalog Volume.
#
#           SNCZI distributes national flood zone shapefiles for T=10, T=50,
#           T=100, and T=500 year return periods (both Peninsula+Baleares and
#           Canarias regions). Download URLs are stable and well-known; this
#           notebook enumerates them from a curated DATASETS list rather than
#           scraping a catalog API (SNCZI has no public catalog API).
#
#           Additionally, the notebook performs a HEAD request on each URL to
#           capture Content-Length and Last-Modified headers for change detection,
#           avoiding re-download of files that have not changed since last run.
#
# Layer   : Bronze  (ceg_delta_bronze_prnd.international_flood.bronze_es_snczi_datasets)
# Outputs : ceg_delta_bronze_prnd.international_flood.bronze_es_snczi_datasets
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#           /Volumes/main/flood_risk/raw/es_snczi/  (raw ZIPs)
#
# Authentication: None — all SNCZI/MITECO downloads are public.
# Schedule      : Monthly via Databricks Workflow. Idempotent — safe to re-run.
# Version       : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate a UUID"
)
dbutils.widgets.text(
    "scrape_version", "1.0.0",
    "Scrape Version — semantic version tag for this notebook"
)
dbutils.widgets.dropdown(
    "download_files", "true", ["true", "false"],
    "Download Files — if true, download ZIPs to Volume during this notebook. Set false for metadata-only scrape."
)
dbutils.widgets.dropdown(
    "skip_unchanged", "true", ["true", "false"],
    "Skip Unchanged — if true, skip download when Last-Modified header matches the previously recorded value."
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
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
import requests
import urllib3
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType,
    StructField, StructType, TimestampType,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Catalog / table / volume paths ────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
BRONZE_SCHEMA   = "international_flood"
BRONZE_TABLE    = "bronze_es_snczi_datasets"
AUDIT_TABLE     = "pipeline_run_log"
VOLUME_CATALOG  = "main"
VOLUME_SCHEMA   = "flood_risk"
VOLUME_NAME     = "raw"
VOLUME_BASE     = f"/Volumes/{VOLUME_CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}/es_snczi"

FQN_BRONZE      = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
FQN_AUDIT       = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{AUDIT_TABLE}"

NOTEBOOK_NAME    = "es_snczi_01_bronze_scrape"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_SNCZI"
COUNTRY          = "ES"

# ── HTTP config ────────────────────────────────────────────────────────────
MAX_RETRIES         = 3
RETRY_BACKOFF_BASE  = 2.0
RETRY_JITTER        = 0.5
REQUEST_TIMEOUT     = 60    # seconds for HEAD request
DOWNLOAD_TIMEOUT    = 1800  # 30 minutes for large SHP ZIPs
CHUNK_SIZE          = 4 * 1024 * 1024  # 4 MB chunks

# ── MITECO/SNCZI download base ─────────────────────────────────────────────
MITECO_DOWNLOAD_BASE = "https://www.mapama.gob.es/app/descargas/descargafichero.aspx"

# ── Known SNCZI flood zone datasets ───────────────────────────────────────
# These are the stable national flood zone shapefiles published by MITECO/SNCZI.
# URLs are deterministic and confirmed from the MITECO IDE download page:
#   https://www.miteco.gob.es/es/cartografia-y-sig/ide/descargas/agua/zi-lamina.html
#
# Return period notation: T=10 = 10yr return period = 10% AEP (annual exceedance probability)
# Probability classes per SNCZI:
#   T=10  → Alta (High)        → 10.0% AEP
#   T=50  → Frecuente (Freq)   →  2.0% AEP
#   T=100 → Media (Medium)     →  1.0% AEP
#   T=500 → Baja (Low)         →  0.2% AEP
SNCZI_DATASETS = [
    {
        "dataset_id":     "zi-shp-q10-pb",
        "return_period":  10,
        "aep_pct":        10.0,
        "probability_class": "alta",
        "region":         "peninsula_baleares",
        "format":         "SHP",
        "filename":       "laminasPB-q10.zip",
        "description_es": "Zonas inundables T=10 años — Láminas de inundación. Período de retorno 10 años (probabilidad alta). Ámbito: Península Ibérica y Baleares.",
        "description_en": "Flood zones T=10 years — Flood extents. Return period 10 years (high probability). Coverage: Iberian Peninsula and Balearic Islands.",
        "source_portal":  "MITECO_IDE",
        "data_type":      "zona_inundable_shp",
    },
    {
        "dataset_id":     "zi-shp-q50-all",
        "return_period":  50,
        "aep_pct":        2.0,
        "probability_class": "frecuente",
        "region":         "all",
        "format":         "SHP",
        "filename":       "laminas-q50.zip",
        "description_es": "Zonas inundables T=50 años — Láminas de inundación. Período de retorno 50 años (probabilidad frecuente). Ámbito: Todo el territorio nacional.",
        "description_en": "Flood zones T=50 years — Flood extents. Return period 50 years (frequent probability). Coverage: All national territory.",
        "source_portal":  "MITECO_IDE",
        "data_type":      "zona_inundable_shp",
    },
    {
        "dataset_id":     "zi-shp-q100-pb",
        "return_period":  100,
        "aep_pct":        1.0,
        "probability_class": "media",
        "region":         "peninsula_baleares",
        "format":         "SHP",
        "filename":       "laminasPB-q100.zip",
        "description_es": "Zonas inundables T=100 años — Láminas de inundación. Período de retorno 100 años (probabilidad media/ocasional). Ámbito: Península Ibérica y Baleares.",
        "description_en": "Flood zones T=100 years — Flood extents. Return period 100 years (medium/occasional probability). Coverage: Iberian Peninsula and Balearic Islands.",
        "source_portal":  "MITECO_IDE",
        "data_type":      "zona_inundable_shp",
    },
    {
        "dataset_id":     "zi-shp-q100-canarias",
        "return_period":  100,
        "aep_pct":        1.0,
        "probability_class": "media",
        "region":         "canarias",
        "format":         "SHP",
        "filename":       "laminasCanarias-q100.zip",
        "description_es": "Zonas inundables T=100 años — Láminas de inundación. Período de retorno 100 años (probabilidad media/ocasional). Ámbito: Islas Canarias.",
        "description_en": "Flood zones T=100 years — Flood extents. Return period 100 years (medium/occasional probability). Coverage: Canary Islands.",
        "source_portal":  "MITECO_IDE",
        "data_type":      "zona_inundable_shp",
    },
    {
        "dataset_id":     "zi-shp-q500-pb",
        "return_period":  500,
        "aep_pct":        0.2,
        "probability_class": "baja",
        "region":         "peninsula_baleares",
        "format":         "SHP",
        "filename":       "laminasPB-q500.zip",
        "description_es": "Zonas inundables T=500 años — Láminas de inundación. Período de retorno 500 años (probabilidad baja/excepcional). Ámbito: Península Ibérica y Baleares.",
        "description_en": "Flood zones T=500 years — Flood extents. Return period 500 years (low/exceptional probability). Coverage: Iberian Peninsula and Balearic Islands.",
        "source_portal":  "MITECO_IDE",
        "data_type":      "zona_inundable_shp",
    },
    {
        "dataset_id":     "zi-shp-q500-canarias",
        "return_period":  500,
        "aep_pct":        0.2,
        "probability_class": "baja",
        "region":         "canarias",
        "format":         "SHP",
        "filename":       "laminasCanarias-q500.zip",
        "description_es": "Zonas inundables T=500 años — Láminas de inundación. Período de retorno 500 años (probabilidad baja/excepcional). Ámbito: Islas Canarias.",
        "description_en": "Flood zones T=500 years — Flood extents. Return period 500 years (low/exceptional probability). Coverage: Canary Islands.",
        "source_portal":  "MITECO_IDE",
        "data_type":      "zona_inundable_shp",
    },
]

# Compute download URLs from filenames
for ds in SNCZI_DATASETS:
    ds["download_url"] = f"{MITECO_DOWNLOAD_BASE}?f={ds['filename']}"
    ds["resource_id"]  = hashlib.sha256(
        f"es_snczi::{ds['dataset_id']}".encode("utf-8")
    ).hexdigest()[:32]

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

RUN_ID         = dbutils.widgets.get("run_id").strip()          or str(uuid.uuid4())
SCRAPE_VERSION = dbutils.widgets.get("scrape_version").strip()  or NOTEBOOK_VERSION
DOWNLOAD_FILES = dbutils.widgets.get("download_files").lower() == "true"
SKIP_UNCHANGED = dbutils.widgets.get("skip_unchanged").lower() == "true"
SCRAPED_AT     = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"ES SNCZI Bronze Scrape  |  Run ID: {RUN_ID}")
log.info(f"Notebook version : {NOTEBOOK_VERSION}")
log.info(f"Scrape version   : {SCRAPE_VERSION}")
log.info(f"Download files   : {DOWNLOAD_FILES}")
log.info(f"Skip unchanged   : {SKIP_UNCHANGED}")
log.info(f"Datasets defined : {len(SNCZI_DATASETS)}")
log.info(f"Started at       : {SCRAPED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Bronze Schema Definition

# COMMAND ----------

BRONZE_STRUCT = StructType([
    StructField("resource_id",            StringType(),    False),
    StructField("dataset_id",             StringType(),    False),
    StructField("download_url",           StringType(),    True),
    StructField("filename",               StringType(),    True),
    StructField("return_period_yr",       LongType(),      True),
    StructField("aep_pct",                DoubleType(),    True),
    StructField("probability_class_es",   StringType(),    True),
    StructField("region",                 StringType(),    True),
    StructField("resource_format",        StringType(),    True),
    StructField("data_type",              StringType(),    True),
    StructField("source_portal",          StringType(),    True),
    StructField("description_es",         StringType(),    True),
    StructField("description_en",         StringType(),    True),
    StructField("http_content_length",    LongType(),      True),
    StructField("http_last_modified",     StringType(),    True),
    StructField("http_status_code",       LongType(),      True),
    StructField("volume_path",            StringType(),    True),
    StructField("download_status",        StringType(),    True),
    StructField("download_error_message", StringType(),    True),
    StructField("file_size_bytes_actual", LongType(),      True),
    StructField("download_timestamp",     TimestampType(), True),
    StructField("_run_id",                StringType(),    False),
    StructField("_ingested_at",           TimestampType(), False),
    StructField("_scrape_version",        StringType(),    False),
    StructField("_is_current",            BooleanType(),   False),
    StructField("_source",                StringType(),    False),
    StructField("_country",               StringType(),    False),
])

BRONZE_COLUMNS = [f.name for f in BRONZE_STRUCT.fields]

# COMMAND ----------

# MAGIC %md ## 5. Table and Volume Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_BRONZE} (
    resource_id             STRING     NOT NULL COMMENT 'Deterministic 32-char hex ID: SHA256("es_snczi::{dataset_id}")[:32].',
    dataset_id              STRING     NOT NULL COMMENT 'Stable slug identifying this SNCZI dataset (e.g. zi-shp-q100-pb).',
    download_url            STRING              COMMENT 'Full HTTPS URL for direct download from MITECO.',
    filename                STRING              COMMENT 'Filename component of the download URL (e.g. laminasPB-q100.zip).',
    return_period_yr        LONG                COMMENT 'Return period in years: 10, 50, 100, or 500.',
    aep_pct                 DOUBLE              COMMENT 'Annual Exceedance Probability as a percentage (e.g. 1.0 for T=100).',
    probability_class_es    STRING              COMMENT 'Spanish probability class: alta | frecuente | media | baja.',
    region                  STRING              COMMENT 'Geographic region: peninsula_baleares | canarias | all.',
    resource_format         STRING              COMMENT 'File format: SHP (shapefile in ZIP), KMZ, PDF.',
    data_type               STRING              COMMENT 'SNCZI data type: zona_inundable_shp | peligrosidad_raster | report.',
    source_portal           STRING              COMMENT 'Source portal: MITECO_IDE | CNIG.',
    description_es          STRING              COMMENT 'Spanish-language description of the dataset.',
    description_en          STRING              COMMENT 'English-language description of the dataset.',
    http_content_length     LONG                COMMENT 'Content-Length header from HTTP HEAD request (bytes). NULL if not returned.',
    http_last_modified      STRING              COMMENT 'Last-Modified header string from HTTP HEAD request. Used for change detection.',
    http_status_code        LONG                COMMENT 'HTTP status code from the HEAD request (200 = available, 404 = not found).',
    volume_path             STRING              COMMENT 'Absolute path to the downloaded file in the Unity Catalog Volume. NULL if download not attempted or failed.',
    download_status         STRING              COMMENT 'Download lifecycle: pending | downloaded | failed | skipped_unchanged.',
    download_error_message  STRING              COMMENT 'Error detail if download failed. NULL on success.',
    file_size_bytes_actual  LONG                COMMENT 'Actual bytes written to Volume. NULL if not downloaded.',
    download_timestamp      TIMESTAMP           COMMENT 'UTC timestamp of successful download. NULL if not downloaded.',
    _run_id                 STRING     NOT NULL COMMENT 'UUID identifying the pipeline run that produced this row.',
    _ingested_at            TIMESTAMP  NOT NULL COMMENT 'UTC timestamp when this row was written to bronze.',
    _scrape_version         STRING     NOT NULL COMMENT 'Semantic version of this notebook at time of run.',
    _is_current             BOOLEAN    NOT NULL COMMENT 'TRUE if this is the most recent record for this dataset_id. Toggled FALSE by later runs.',
    _source                 STRING     NOT NULL COMMENT 'Source system — always ES_SNCZI.',
    _country                STRING     NOT NULL COMMENT 'ISO 3166-1 alpha-2 country code — always ES.'
)
USING DELTA
COMMENT 'Bronze layer: raw enumeration of SNCZI national flood zone datasets from MITECO. One row per dataset per pipeline run. Append-only; use _is_current = TRUE for current-state queries.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''          = ''true'',
    ''delta.autoOptimize.autoCompact''      = ''true'',
    ''delta.autoOptimize.optimizeWrite''    = ''true'',
    ''pipelines.autoOptimize.zOrderCols''   = ''dataset_id,_is_current''
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_AUDIT} (
    run_id              STRING     NOT NULL  COMMENT 'UUID shared across all pipeline notebooks in one Workflow run.',
    pipeline_stage      STRING     NOT NULL  COMMENT 'Which notebook produced this record: bronze | silver | gold | download | raster_ingest.',
    notebook_name       STRING               COMMENT 'Filename of the notebook.',
    notebook_version    STRING               COMMENT 'Semantic version of the notebook at time of run.',
    start_time          TIMESTAMP  NOT NULL,
    end_time            TIMESTAMP,
    duration_seconds    DOUBLE,
    status              STRING               COMMENT 'success | failed | partial',
    rows_read           LONG,
    rows_written        LONG,
    rows_merged         LONG,
    rows_rejected       LONG                 COMMENT 'Rows that failed data quality validation and were not written.',
    error_message       STRING,
    extra_metadata      STRING               COMMENT 'JSON blob of stage-specific counters.',
    databricks_job_id   STRING,
    databricks_run_id   STRING,
    scrape_version      STRING
)
USING DELTA
COMMENT 'Pipeline audit log. One row per notebook stage per Workflow run. All es_snczi pipeline notebooks write here.'
TBLPROPERTIES (''delta.enableChangeDataFeed'' = ''true'')
""")

# Create the UC Volume if it does not already exist
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {VOLUME_CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}
    COMMENT 'Raw flood risk data files — Spain SNCZI SHP ZIPs and supplementary documents.'
""")
log.info(f"Volume {VOLUME_CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME} ensured")

# COMMAND ----------

# MAGIC %md ## 6. HTTP Utilities

# COMMAND ----------

class ScrapeError(Exception):
    """Raised when an HTTP request fails after all retry attempts."""
    pass


def http_head(url: str) -> dict:
    """
    Issues an HTTP HEAD request to retrieve metadata headers without downloading
    the file body. Returns a dict with keys: status_code, content_length,
    last_modified. Returns None values on failure.
    """
    result = {"status_code": None, "content_length": None, "last_modified": None}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.head(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                verify=True,
            )
            result["status_code"]    = resp.status_code
            result["content_length"] = _parse_content_length(resp.headers.get("Content-Length"))
            result["last_modified"]  = resp.headers.get("Last-Modified")
            return result
        except requests.RequestException as exc:
            if attempt < MAX_RETRIES:
                sleep_s = RETRY_BACKOFF_BASE ** attempt + random.uniform(0, RETRY_JITTER)
                log.warning(f"HEAD attempt {attempt}/{MAX_RETRIES} failed [{url}]: {exc}. Retrying in {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                log.error(f"HEAD failed after {MAX_RETRIES} attempts [{url}]: {exc}")
    return result


def _parse_content_length(header_value: Optional[str]) -> Optional[int]:
    """Safely parses Content-Length header string to int."""
    try:
        return int(header_value) if header_value else None
    except (ValueError, TypeError):
        return None


def download_file_to_volume(url: str, dest_path: str) -> int:
    """
    Downloads url to dest_path using chunked streaming.
    Returns the number of bytes written.
    Retries on transient errors with exponential backoff.
    Large files (SNCZI ZIPs can be several hundred MB) use a 30-minute timeout.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            with requests.get(
                url,
                stream=True,
                verify=True,
                timeout=(30, DOWNLOAD_TIMEOUT),
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
            delay = RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(0, RETRY_JITTER)
            log.warning(f"Download attempt {attempt + 1}/{MAX_RETRIES} failed ({last_exc}), retrying in {delay:.1f}s")
            time.sleep(delay)

    raise last_exc or RuntimeError(f"Download failed for {url}")

# COMMAND ----------

# MAGIC %md ## 7. Change Detection

# COMMAND ----------

def get_previous_last_modified(dataset_id: str) -> Optional[str]:
    """
    Retrieves the http_last_modified value from the most recent successful
    bronze record for a given dataset_id. Used for skip-if-unchanged logic.
    Returns None if no prior record exists or if it was not recorded.
    """
    try:
        row = spark.sql(f"""
            SELECT http_last_modified
            FROM   {FQN_BRONZE}
            WHERE  dataset_id   = '{dataset_id}'
              AND  _is_current  = TRUE
              AND  download_status IN ('downloaded', 'skipped_unchanged')
            ORDER  BY _ingested_at DESC
            LIMIT  1
        """).collect()
        return row[0]["http_last_modified"] if row else None
    except Exception as exc:
        log.warning(f"Could not retrieve previous last_modified for {dataset_id}: {exc}")
        return None

# COMMAND ----------

# MAGIC %md ## 8. Main Scrape and Download Loop

# COMMAND ----------

def scrape_and_download() -> tuple:
    """
    For each SNCZI dataset:
      1. HEAD request to check availability and capture metadata
      2. Optionally download the ZIP to the UC Volume
      3. Build a bronze row dict

    Returns (rows: list[dict], counters: dict)
    """
    counters = {
        "total": len(SNCZI_DATASETS),
        "available": 0,
        "downloaded": 0,
        "skipped_unchanged": 0,
        "failed": 0,
        "errors": 0,
    }
    rows = []

    for ds in SNCZI_DATASETS:
        dataset_id   = ds["dataset_id"]
        url          = ds["download_url"]
        filename     = ds["filename"]
        region       = ds["region"]
        return_period = ds["return_period"]

        log.info(f"Processing dataset: {dataset_id} | T={return_period}yr | region={region} | url={url}")

        # ── HEAD request ──────────────────────────────────────────────────
        head = http_head(url)
        status_code    = head["status_code"]
        content_length = head["content_length"]
        last_modified  = head["last_modified"]

        row = {
            "resource_id":            ds["resource_id"],
            "dataset_id":             dataset_id,
            "download_url":           url,
            "filename":               filename,
            "return_period_yr":       int(return_period),
            "aep_pct":                ds["aep_pct"],
            "probability_class_es":   ds["probability_class"],
            "region":                 region,
            "resource_format":        ds["format"],
            "data_type":              ds["data_type"],
            "source_portal":          ds["source_portal"],
            "description_es":         ds["description_es"],
            "description_en":         ds["description_en"],
            "http_content_length":    content_length,
            "http_last_modified":     last_modified,
            "http_status_code":       status_code,
            "volume_path":            None,
            "download_status":        "pending",
            "download_error_message": None,
            "file_size_bytes_actual": None,
            "download_timestamp":     None,
            "_run_id":                RUN_ID,
            "_ingested_at":           SCRAPED_AT,
            "_scrape_version":        SCRAPE_VERSION,
            "_is_current":            True,
            "_source":                SOURCE_NAME,
            "_country":               COUNTRY,
        }

        if status_code != 200:
            log.warning(f"[{dataset_id}] HTTP {status_code} — marking as failed")
            row["download_status"]        = "failed"
            row["download_error_message"] = f"HTTP {status_code} on HEAD request"
            counters["errors"] += 1
            rows.append(row)
            continue

        counters["available"] += 1

        # ── Skip-if-unchanged check ───────────────────────────────────────
        if SKIP_UNCHANGED and last_modified:
            prev_last_modified = get_previous_last_modified(dataset_id)
            if prev_last_modified and prev_last_modified == last_modified:
                log.info(f"[{dataset_id}] Unchanged (Last-Modified={last_modified}) — skipping download")
                row["download_status"] = "skipped_unchanged"
                counters["skipped_unchanged"] += 1
                rows.append(row)
                continue

        # ── Download ──────────────────────────────────────────────────────
        if not DOWNLOAD_FILES:
            row["download_status"] = "pending"
            rows.append(row)
            continue

        # Destination path: /Volumes/.../es_snczi/{region}/t{period:04d}/{filename}
        dest_dir  = os.path.join(VOLUME_BASE, region, f"t{return_period:04d}")
        dest_path = os.path.join(dest_dir, filename)

        try:
            log.info(f"[{dataset_id}] Downloading {url} → {dest_path}")
            bytes_written = download_file_to_volume(url, dest_path)
            row["volume_path"]            = dest_path
            row["download_status"]        = "downloaded"
            row["file_size_bytes_actual"] = bytes_written
            row["download_timestamp"]     = datetime.now(timezone.utc)
            counters["downloaded"] += 1
            log.info(f"[{dataset_id}] Downloaded {bytes_written:,} bytes → {dest_path}")
        except Exception as exc:
            row["download_status"]        = "failed"
            row["download_error_message"] = str(exc)[:2000]
            counters["failed"] += 1
            log.error(f"[{dataset_id}] Download failed: {exc}")

        rows.append(row)

    log.info(
        f"Scrape complete: total={counters['total']} available={counters['available']} "
        f"downloaded={counters['downloaded']} skipped={counters['skipped_unchanged']} "
        f"failed={counters['failed']} errors={counters['errors']}"
    )
    return rows, counters

# COMMAND ----------

# MAGIC %md ## 9. Bronze Delta Write

# COMMAND ----------

def write_bronze(rows: list, run_id: str, scraped_at, scrape_version: str) -> dict:
    """
    Annotates rows with pipeline metadata, toggles _is_current=FALSE on
    superseded records, and appends new rows to the bronze table.
    Returns a stats dict.
    """
    if not rows:
        log.warning("write_bronze: no rows to write — skipping")
        return {"rows_written": 0}

    pdf = pd.DataFrame(rows)
    for col in BRONZE_COLUMNS:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[BRONZE_COLUMNS]

    sdf = spark.createDataFrame(pdf, schema=BRONZE_STRUCT)

    # Expire superseded records for the same dataset_ids
    dataset_ids = [r["dataset_id"] for r in rows if r.get("dataset_id")]
    if dataset_ids:
        id_df = spark.createDataFrame([(d,) for d in dataset_ids], ["dataset_id"])
        id_df.createOrReplaceTempView("_es_snczi_current_run_ids")
        spark.sql(f"""
            UPDATE {FQN_BRONZE}
            SET    _is_current = FALSE
            WHERE  dataset_id IN (SELECT dataset_id FROM _es_snczi_current_run_ids)
              AND  _is_current  = TRUE
              AND  _run_id     != '{run_id}'
        """)

    (
        sdf.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .saveAsTable(FQN_BRONZE)
    )

    rows_written = len(rows)
    log.info(f"Bronze write complete — {rows_written} rows appended to {FQN_BRONZE}")
    return {"rows_written": rows_written}

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
            ctx       = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
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
            "scrape_version":    SCRAPE_VERSION,
        }]
        (
            spark.createDataFrame(record)
            .write.format("delta").mode("append").saveAsTable(FQN_AUDIT)
        )
        log.info(f"Audit written: stage={stage} status={status} rows_written={rows_written}")
    except Exception as audit_exc:
        log.error(f"write_audit failed (non-fatal): {audit_exc}")

# COMMAND ----------

# MAGIC %md ## 11. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None
_write_stats     = {"rows_written": 0}
_counters        = {}

try:
    log.info(f"Starting ES SNCZI Bronze Scrape | run_id={RUN_ID}")

    _rows, _counters = scrape_and_download()
    _write_stats     = write_bronze(_rows, RUN_ID, SCRAPED_AT, SCRAPE_VERSION)

    _pipeline_status = "success" if _counters.get("failed", 0) == 0 else "partial"

    log.info(
        f"Run complete | rows_written={_write_stats['rows_written']} "
        f"downloaded={_counters.get('downloaded', 0)} "
        f"skipped={_counters.get('skipped_unchanged', 0)} "
        f"failed={_counters.get('failed', 0)}"
    )

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "bronze",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_written     = _write_stats.get("rows_written", 0),
        error_message    = _pipeline_error,
        extra_metadata   = {
            "datasets_total":      _counters.get("total", 0),
            "datasets_available":  _counters.get("available", 0),
            "downloaded":          _counters.get("downloaded", 0),
            "skipped_unchanged":   _counters.get("skipped_unchanged", 0),
            "failed":              _counters.get("failed", 0),
            "errors":              _counters.get("errors", 0),
            "download_files":      DOWNLOAD_FILES,
            "skip_unchanged":      SKIP_UNCHANGED,
            "volume_base":         VOLUME_BASE,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
