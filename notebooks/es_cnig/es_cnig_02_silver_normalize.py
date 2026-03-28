# Databricks notebook source
# Spain CNIG — Silver Normalisation
#
# Purpose : Reads the latest bronze scrape records from bronze_es_cnig_datasets,
#           normalises and enriches raster metadata, classifies flood products
#           by return period and product type, and writes to the silver Delta
#           table via MERGE upsert.
#
#           Key transformations:
#             - Derive stable raster_id (SHA-256 of filename + demarcacion_code)
#             - Map return periods to AEP % and Spanish probability classes
#             - Translate Spanish field names to English schema equivalents
#             - Normalise CRS: ETRS89 UTM zones (EPSG:25829/25830/25831)
#             - Enrich with demarcación Hidrográfica lookup metadata
#
# Layer   : Silver  (ceg_delta_silver_prnd.international_flood.silver_es_cnig_datasets)
# Reads   : ceg_delta_bronze_prnd.international_flood.bronze_es_cnig_datasets
# Writes  : ceg_delta_silver_prnd.international_flood.silver_es_cnig_datasets
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
dbutils.widgets.text(
    "bronze_run_id", "",
    "Bronze Run ID — _run_id from the bronze scrape. Blank = latest run."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, rebuilds silver from entire bronze history."
)
dbutils.widgets.dropdown(
    "demarcacion", "Ebro",
    [
        "Ebro", "Tajo", "Guadalquivir", "Segura", "Jucar",
        "Duero", "Mino-Sil", "Cantabrico-Occidental", "Cantabrico-Oriental",
        "Guadalete-Barbate", "Tinto-Odiel-Piedras", "Guadiana",
        "Cuencas-Internas-Cataluna", "Baleares", "Canarias",
    ],
    "Demarcación Hidrográfica — must match the bronze scrape run"
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
SILVER_CATALOG  = "ceg_delta_silver_prnd"
SCHEMA          = "international_flood"

FQN_BRONZE_SCRAPE = f"{BRONZE_CATALOG}.{SCHEMA}.bronze_es_cnig_datasets"
FQN_AUDIT         = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_SILVER        = f"{SILVER_CATALOG}.{SCHEMA}.silver_es_cnig_datasets"

NOTEBOOK_NAME    = "es_cnig_02_silver_normalize"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_CNIG"
COUNTRY          = "ES"

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

RUN_ID        = dbutils.widgets.get("run_id").strip()        or str(uuid.uuid4())
BRONZE_RUN_ID = dbutils.widgets.get("bronze_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
DEMARCACION   = dbutils.widgets.get("demarcacion").strip()   or "Ebro"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"CNIG Silver Normalisation  |  Run ID: {RUN_ID}")
log.info(f"Bronze run ID : {BRONZE_RUN_ID or 'latest'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Demarcación   : {DEMARCACION}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_SILVER} (
    -- Identity
    raster_id                   STRING     NOT NULL  COMMENT 'Stable SHA-256-derived ID: es_cnig::{demarcacion_code}::{tile_code}::{return_period_years}. First 32 chars.',
    tile_id                     STRING               COMMENT 'Bronze tile_id — the natural key from bronze_es_cnig_datasets.',
    tile_code                   STRING               COMMENT 'CNIG tile code as scraped.',
    tile_name                   STRING               COMMENT 'Human-readable tile name from CNIG portal.',
    -- Classification
    demarcacion_code            STRING               COMMENT 'Demarcación Hidrográfica internal code, e.g. DH-EB.',
    demarcacion_name            STRING               COMMENT 'Demarcación slug, e.g. Ebro.',
    demarcacion_name_es         STRING               COMMENT 'Full Spanish name, e.g. Ebro.',
    return_period_years         INTEGER              COMMENT 'Return period in years: 10 | 100 | 500.',
    aep_pct                     DOUBLE               COMMENT 'Annual Exceedance Probability %: 10.0 | 1.0 | 0.2.',
    probability_class_es        STRING               COMMENT 'Spanish probability class: alta | media | baja.',
    probability_class_en        STRING               COMMENT 'English probability class: high | medium | low.',
    product_type                STRING               COMMENT 'CNIG product type: peligrosidad_fluvial | peligrosidad_costera | mdt.',
    product_category_en         STRING               COMMENT 'English product category: flood_hazard_raster | dtm | other.',
    is_raster                   BOOLEAN              COMMENT 'TRUE for all GeoTIFF hazard maps.',
    -- CRS
    crs_epsg_likely             INTEGER              COMMENT 'Expected CRS EPSG code based on demarcación geography.',
    crs_name                    STRING               COMMENT 'Human-readable CRS name, e.g. ETRS89 / UTM zone 30N.',
    -- Source URLs
    download_url                STRING               COMMENT 'Resolved direct GeoTIFF download URL.',
    detail_page_url             STRING               COMMENT 'CNIG detalleArchivo page URL.',
    -- File metadata
    file_size_bytes             LONG                 COMMENT 'File size from Content-Length header.',
    file_size_mb                DOUBLE               COMMENT 'File size in megabytes.',
    last_modified               STRING               COMMENT 'Last-Modified header for change detection.',
    -- Download tracking (populated by es_cnig_04_download)
    download_status             STRING               COMMENT 'Lifecycle: pending | downloaded | failed | not_applicable.',
    download_error_message      STRING               COMMENT 'Error detail if download failed.',
    volume_path                 STRING               COMMENT 'UC Volume path once downloaded.',
    file_size_bytes_actual      LONG                 COMMENT 'Actual bytes written during download.',
    download_timestamp          TIMESTAMP            COMMENT 'UTC timestamp of last successful download.',
    md5_checksum                STRING               COMMENT 'MD5 hex digest if verified after download.',
    sha256_checksum             STRING               COMMENT 'SHA-256 hex digest if verified after download.',
    -- Pipeline provenance
    _is_current                 BOOLEAN              COMMENT 'TRUE for the active record.',
    _bronze_run_id              STRING               COMMENT '_run_id of the bronze scrape row.',
    _silver_processed_at        TIMESTAMP            COMMENT 'UTC timestamp of silver write.',
    _source                     STRING               COMMENT 'ES_CNIG.',
    _country                    STRING               COMMENT 'ES.'
)
USING DELTA
COMMENT 'Silver layer: one row per CNIG flood hazard GeoTIFF tile per Demarcación Hidrográfica. MERGE on raster_id. Includes download tracking columns populated by es_cnig_04_download.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''        = ''true'',
    ''delta.autoOptimize.optimizeWrite''  = ''true'',
    ''pipelines.autoOptimize.zOrderCols'' = ''raster_id,demarcacion_code,return_period_years''
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Classification Utilities

# COMMAND ----------

# ── Return period → AEP % and probability class ────────────────────────────
RETURN_PERIOD_MAP = {
    10:  {"aep_pct": 10.0, "probability_class_es": "alta",     "probability_class_en": "high"},
    50:  {"aep_pct": 2.0,  "probability_class_es": "frecuente","probability_class_en": "frequent"},
    100: {"aep_pct": 1.0,  "probability_class_es": "media",    "probability_class_en": "medium"},
    500: {"aep_pct": 0.2,  "probability_class_es": "baja",     "probability_class_en": "low"},
}

# ── CRS EPSG → description ─────────────────────────────────────────────────
CRS_NAMES = {
    25829: "ETRS89 / UTM zone 29N",
    25830: "ETRS89 / UTM zone 30N",
    25831: "ETRS89 / UTM zone 31N",
    32628: "WGS 84 / UTM zone 28N",
    4258:  "ETRS89 Geographic",
    4326:  "WGS84 Geographic",
}

# ── Product type → English category ───────────────────────────────────────
PRODUCT_CATEGORY_MAP = {
    "peligrosidad_fluvial":  "flood_hazard_raster",
    "peligrosidad_costera":  "coastal_hazard_raster",
    "mdt":                   "dtm",
}


def classify_tile(tile: dict) -> dict:
    """
    Enriches a bronze tile dict with normalised silver classification fields.
    """
    rp = tile.get("return_period_years")
    rp_class = RETURN_PERIOD_MAP.get(int(rp) if rp else 0, {})
    product_type = tile.get("product_type", "peligrosidad_fluvial")
    crs_epsg = int(tile.get("crs_epsg_likely") or 25830)

    file_size = tile.get("file_size_bytes")
    file_size_mb = round(file_size / (1024 * 1024), 2) if file_size else None

    return {
        "aep_pct":               rp_class.get("aep_pct"),
        "probability_class_es":  rp_class.get("probability_class_es"),
        "probability_class_en":  rp_class.get("probability_class_en"),
        "product_category_en":   PRODUCT_CATEGORY_MAP.get(product_type, "other"),
        "is_raster":             product_type in ("peligrosidad_fluvial", "peligrosidad_costera"),
        "crs_name":              CRS_NAMES.get(crs_epsg, f"EPSG:{crs_epsg}"),
        "file_size_mb":          file_size_mb,
    }


def stable_raster_id(demarcacion_code: str, tile_code: str, return_period_years: Optional[int]) -> str:
    """
    Computes a deterministic 32-char hex raster_id.
    Matches the tile_id logic in es_cnig_01 so they stay in sync.
    """
    rp_str = str(return_period_years or "unknown")
    key = f"es_cnig::{demarcacion_code or ''}::{tile_code or ''}::{rp_str}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]

# COMMAND ----------

# MAGIC %md ## 6. Load Bronze Records

# COMMAND ----------

if FULL_REFRESH:
    log.info("Full refresh mode — loading all current bronze records")
    bronze_df = spark.sql(f"""
        SELECT * FROM {FQN_BRONZE_SCRAPE}
        WHERE _is_current = TRUE
    """)
elif BRONZE_RUN_ID:
    log.info(f"Incremental mode — processing bronze run_id = {BRONZE_RUN_ID}")
    bronze_df = spark.sql(f"""
        SELECT * FROM {FQN_BRONZE_SCRAPE}
        WHERE _run_id = '{BRONZE_RUN_ID}'
          AND _is_current = TRUE
    """)
else:
    log.info("Incremental mode — processing latest bronze run_id")
    latest_run = spark.sql(f"""
        SELECT _run_id FROM {FQN_BRONZE_SCRAPE}
        ORDER BY _scraped_at DESC LIMIT 1
    """).collect()
    if not latest_run:
        log.warning("No bronze records found — nothing to process")
        try:
            dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
        except Exception:
            pass
        dbutils.notebook.exit(RUN_ID)
    latest_id = latest_run[0]["_run_id"]
    log.info(f"Latest bronze run_id resolved: {latest_id}")
    bronze_df = spark.sql(f"""
        SELECT * FROM {FQN_BRONZE_SCRAPE}
        WHERE _run_id = '{latest_id}'
          AND _is_current = TRUE
    """)

bronze_pdf = bronze_df.toPandas()
_bronze_run_id_used = bronze_pdf["_run_id"].iloc[0] if len(bronze_pdf) > 0 else RUN_ID
log.info(f"Loaded {len(bronze_pdf)} bronze rows (run_id={_bronze_run_id_used})")

# COMMAND ----------

# MAGIC %md ## 7. Transform and Classify

# COMMAND ----------

silver_rows = []
for _, row in bronze_pdf.iterrows():
    tile_code    = row.get("tile_code", "")
    tile_id      = row.get("tile_id", "")
    dem_code     = row.get("demarcacion_code", "")
    rp_years     = row.get("return_period_years")
    rp_int       = int(rp_years) if pd.notna(rp_years) else None

    raster_id = stable_raster_id(dem_code, tile_code, rp_int)
    cls = classify_tile(dict(row))

    silver_rows.append({
        # Identity
        "raster_id":                raster_id,
        "tile_id":                  tile_id,
        "tile_code":                tile_code,
        "tile_name":                row.get("tile_name"),
        # Classification
        "demarcacion_code":         dem_code,
        "demarcacion_name":         row.get("demarcacion_name"),
        "demarcacion_name_es":      row.get("demarcacion_name_es"),
        "return_period_years":      rp_int,
        "aep_pct":                  cls["aep_pct"],
        "probability_class_es":     cls["probability_class_es"],
        "probability_class_en":     cls["probability_class_en"],
        "product_type":             row.get("product_type"),
        "product_category_en":      cls["product_category_en"],
        "is_raster":                cls["is_raster"],
        # CRS
        "crs_epsg_likely":          int(row.get("crs_epsg_likely") or 25830),
        "crs_name":                 cls["crs_name"],
        # Source URLs
        "download_url":             row.get("download_url"),
        "detail_page_url":          row.get("detail_page_url"),
        # File metadata
        "file_size_bytes":          row.get("file_size_bytes"),
        "file_size_mb":             cls["file_size_mb"],
        "last_modified":            row.get("last_modified"),
        # Download tracking defaults
        "download_status":          "pending",
        "download_error_message":   None,
        "volume_path":              None,
        "file_size_bytes_actual":   None,
        "download_timestamp":       None,
        "md5_checksum":             None,
        "sha256_checksum":          None,
        # Pipeline provenance
        "_is_current":              True,
        "_bronze_run_id":           row.get("_run_id"),
        "_silver_processed_at":     PROCESSED_AT,
        "_source":                  SOURCE_NAME,
        "_country":                 COUNTRY,
    })

silver_pdf = pd.DataFrame(silver_rows)

# Coerce numerics
for col in ("return_period_years", "crs_epsg_likely"):
    silver_pdf[col] = pd.to_numeric(silver_pdf[col], errors="coerce").astype("Int64")
for col in ("aep_pct", "file_size_mb"):
    silver_pdf[col] = pd.to_numeric(silver_pdf[col], errors="coerce")
for col in ("file_size_bytes", "file_size_bytes_actual"):
    silver_pdf[col] = pd.to_numeric(silver_pdf[col], errors="coerce").astype("Int64")

log.info(f"Prepared {len(silver_pdf)} silver rows for MERGE")

# Log classification summary
if len(silver_pdf) > 0:
    rp_counts = silver_pdf["return_period_years"].value_counts().to_dict()
    cat_counts = silver_pdf["product_category_en"].value_counts().to_dict()
    log.info(f"Return periods: {rp_counts}")
    log.info(f"Product categories: {cat_counts}")

# COMMAND ----------

# MAGIC %md ## 8. MERGE into Silver

# COMMAND ----------

if len(silver_pdf) > 0:
    silver_sdf = spark.createDataFrame(silver_pdf)
    silver_sdf.createOrReplaceTempView("_incoming_cnig_datasets")

    spark.sql(f"""
        MERGE INTO {FQN_SILVER} AS target
        USING _incoming_cnig_datasets AS source
        ON target.raster_id = source.raster_id
        WHEN MATCHED THEN UPDATE SET
            target.tile_id                  = source.tile_id,
            target.tile_code                = source.tile_code,
            target.tile_name                = source.tile_name,
            target.demarcacion_code         = source.demarcacion_code,
            target.demarcacion_name         = source.demarcacion_name,
            target.demarcacion_name_es      = source.demarcacion_name_es,
            target.return_period_years      = source.return_period_years,
            target.aep_pct                  = source.aep_pct,
            target.probability_class_es     = source.probability_class_es,
            target.probability_class_en     = source.probability_class_en,
            target.product_type             = source.product_type,
            target.product_category_en      = source.product_category_en,
            target.is_raster                = source.is_raster,
            target.crs_epsg_likely          = source.crs_epsg_likely,
            target.crs_name                 = source.crs_name,
            target.download_url             = source.download_url,
            target.detail_page_url          = source.detail_page_url,
            target.file_size_bytes          = source.file_size_bytes,
            target.file_size_mb             = source.file_size_mb,
            target.last_modified            = source.last_modified,
            target._is_current              = TRUE,
            target._bronze_run_id           = source._bronze_run_id,
            target._silver_processed_at     = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    _rows_merged = len(silver_pdf)
    log.info(f"MERGE complete — {_rows_merged} rows into {FQN_SILVER}")
else:
    _rows_merged = 0
    log.warning("No silver rows to merge — bronze may be empty")

# COMMAND ----------

# MAGIC %md ## 9. Audit Logging

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
        log.info("Audit written: stage=%s status=%s", stage, status)
    except Exception as exc:
        log.error("write_audit failed (non-fatal): %s", exc)

# COMMAND ----------

# MAGIC %md ## 10. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("CNIG silver normalisation complete")
    log.info(f"  Rows merged : {_rows_merged}")
    _pipeline_status = "success"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Silver pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "silver",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_read        = len(bronze_pdf),
        rows_written     = _rows_merged,
        rows_merged      = _rows_merged,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "demarcacion":      DEMARCACION,
            "rows_merged":      _rows_merged,
            "bronze_run_id":    _bronze_run_id_used,
            "full_refresh":     FULL_REFRESH,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
