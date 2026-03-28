# Databricks notebook source
# Spain CNIG — Gold Catalog
#
# Purpose : Reads the CNIG silver table and produces two gold-layer outputs:
#             - gold_es_cnig_raster_catalog — CNIG-specific, fully denormalized,
#               one row per tile; full overwrite per Demarcación.
#             - au_flood_raster_catalog — shared multi-source gold table;
#               CNIG rows are isolated by _source = 'ES_CNIG' with source-scoped
#               MERGE upsert on raster_id.
#
#           The shared au_flood_raster_catalog receives lightweight catalog entries
#           for discovered tiles (regardless of whether the GeoTIFF has been
#           downloaded yet).  Spatial metadata columns (bbox, width_px, etc.) are
#           NULL until es_cnig_05_raster_ingest has run.
#
# Layer   : Gold  (ceg_delta_gold_prnd.international_flood.*)
# Reads   : ceg_delta_silver_prnd.international_flood.silver_es_cnig_datasets
# Writes  : ceg_delta_gold_prnd.international_flood.gold_es_cnig_raster_catalog
#           ceg_delta_gold_prnd.international_flood.au_flood_raster_catalog
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
    "silver_run_id", "",
    "Silver Run ID — informational only."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, rebuilds gold_es_cnig_raster_catalog from all current silver data."
)
dbutils.widgets.dropdown(
    "demarcacion", "Ebro",
    [
        "Ebro", "Tajo", "Guadalquivir", "Segura", "Jucar",
        "Duero", "Mino-Sil", "Cantabrico-Occidental", "Cantabrico-Oriental",
        "Guadalete-Barbate", "Tinto-Odiel-Piedras", "Guadiana",
        "Cuencas-Internas-Cataluna", "Baleares", "Canarias",
    ],
    "Demarcación Hidrográfica — used to scope source-specific catalog overwrite"
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
)

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
SILVER_CATALOG  = "ceg_delta_silver_prnd"
GOLD_CATALOG    = "ceg_delta_gold_prnd"
SCHEMA          = "international_flood"

FQN_AUDIT              = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_SILVER             = f"{SILVER_CATALOG}.{SCHEMA}.silver_es_cnig_datasets"
FQN_GOLD_CNIG          = f"{GOLD_CATALOG}.{SCHEMA}.gold_es_cnig_raster_catalog"
FQN_RASTER_CATALOG     = f"{GOLD_CATALOG}.{SCHEMA}.au_flood_raster_catalog"

NOTEBOOK_NAME    = "es_cnig_03_gold_catalog"
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
SILVER_RUN_ID = dbutils.widgets.get("silver_run_id").strip() or None
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
DEMARCACION   = dbutils.widgets.get("demarcacion").strip()   or "Ebro"
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"CNIG Gold Catalog  |  Run ID: {RUN_ID}")
log.info(f"Demarcación    : {DEMARCACION}")
log.info(f"Silver run ID  : {SILVER_RUN_ID or 'current snapshot'}")
log.info(f"Full refresh   : {FULL_REFRESH}")
log.info(f"Started at     : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{SCHEMA}")

# ── gold_es_cnig_raster_catalog ────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_GOLD_CNIG} (
    raster_id                   STRING      NOT NULL  COMMENT 'Stable SHA-256-derived tile identifier.',
    tile_code                   STRING                COMMENT 'CNIG tile/file code.',
    tile_name                   STRING                COMMENT 'Human-readable tile name.',
    demarcacion_code            STRING                COMMENT 'Demarcación Hidrográfica internal code.',
    demarcacion_name            STRING                COMMENT 'Demarcación slug.',
    demarcacion_name_es         STRING                COMMENT 'Full Spanish name.',
    return_period_years         INTEGER               COMMENT 'Return period in years: 10 | 100 | 500.',
    aep_pct                     DOUBLE                COMMENT 'Annual Exceedance Probability %.',
    probability_class_es        STRING                COMMENT 'Spanish: alta | media | baja.',
    probability_class_en        STRING                COMMENT 'English: high | medium | low.',
    product_type                STRING                COMMENT 'peligrosidad_fluvial | peligrosidad_costera | mdt.',
    product_category_en         STRING                COMMENT 'flood_hazard_raster | coastal_hazard_raster | dtm | other.',
    is_raster                   BOOLEAN               COMMENT 'TRUE for all GeoTIFF hazard maps.',
    crs_epsg_likely             INTEGER               COMMENT 'Expected CRS EPSG.',
    crs_name                    STRING                COMMENT 'Human-readable CRS description.',
    download_url                STRING                COMMENT 'Direct GeoTIFF download URL.',
    detail_page_url             STRING                COMMENT 'CNIG detalleArchivo page URL.',
    file_size_bytes             LONG                  COMMENT 'Expected file size from HEAD response.',
    file_size_mb                DOUBLE                COMMENT 'File size in MB.',
    download_status             STRING                COMMENT 'pending | downloaded | failed | not_applicable.',
    volume_path                 STRING                COMMENT 'UC Volume path after download.',
    download_timestamp          TIMESTAMP             COMMENT 'UTC timestamp of last successful download.',
    -- Spatial (populated after raster ingest)
    bbox_wkt                    STRING                COMMENT 'WKT POLYGON bounding box in EPSG:4326. NULL until raster ingested.',
    bbox_min_lon                DOUBLE                COMMENT 'Min longitude (WGS84).',
    bbox_min_lat                DOUBLE                COMMENT 'Min latitude (WGS84).',
    bbox_max_lon                DOUBLE                COMMENT 'Max longitude (WGS84).',
    bbox_max_lat                DOUBLE                COMMENT 'Max latitude (WGS84).',
    width_px                    INTEGER               COMMENT 'Raster width in pixels.',
    height_px                   INTEGER               COMMENT 'Raster height in pixels.',
    -- Pipeline provenance
    _gold_processed_at          TIMESTAMP             COMMENT 'UTC timestamp of gold write.',
    _silver_run_id              STRING                COMMENT 'Silver notebook run_id.',
    _source                     STRING                COMMENT 'ES_CNIG.',
    _country                    STRING                COMMENT 'ES.'
)
USING DELTA
COMMENT 'Gold layer: CNIG-specific flood hazard raster catalog — one row per GeoTIFF tile per Demarcación Hidrográfica. Full overwrite per Demarcación per run.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''       = ''true'',
    ''delta.autoOptimize.optimizeWrite'' = ''true''
)
""")

# ── au_flood_raster_catalog — shared multi-source ─────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_RASTER_CATALOG} (
    raster_id                   STRING      NOT NULL  COMMENT 'Stable identifier derived from file path / tile key (SHA-256 hex, 32 chars).',
    resource_id                 STRING                COMMENT 'FK to source-specific silver resource table.',
    volume_path                 STRING                COMMENT 'Absolute path to the raster file in the Unity Catalog Volume.',
    original_filename           STRING                COMMENT 'Filename as stored in the Volume.',
    file_format                 STRING                COMMENT 'TIF | ASC | etc.',
    is_extracted_from_zip       BOOLEAN               COMMENT 'TRUE if extracted from a ZIP archive.',
    source_state                STRING                COMMENT 'Source state or country code: QLD | NSW | ES.',
    source_agency               STRING                COMMENT 'Publishing organisation name.',
    study_name                  STRING                COMMENT 'Human-readable study or product title.',
    project_id                  STRING                COMMENT 'Source-specific study or project identifier.',
    council_lga_primary         STRING                COMMENT 'Primary LGA or administrative unit. Demarcación for CNIG.',
    inferred_aep_pct            DOUBLE                COMMENT 'Annual Exceedance Probability %.',
    inferred_return_period_yr   INTEGER               COMMENT 'Return period in years.',
    value_type                  STRING                COMMENT 'depth | wse | hazard | unknown.',
    publication_date            DATE                  COMMENT 'Dataset publication or last modification date.',
    crs_epsg                    INTEGER               COMMENT 'EPSG code of native raster CRS.',
    width_px                    INTEGER               COMMENT 'Raster width in pixels.',
    height_px                   INTEGER               COMMENT 'Raster height in pixels.',
    num_bands                   INTEGER               COMMENT 'Number of raster bands.',
    scale_x                     DOUBLE                COMMENT 'Pixel width in CRS units.',
    scale_y                     DOUBLE                COMMENT 'Pixel height in CRS units.',
    nodata_value                DOUBLE                COMMENT 'NoData sentinel for band 1.',
    native_envelope_wkt         STRING                COMMENT 'WKT POLYGON in native CRS.',
    bbox_wkt                    STRING                COMMENT 'WKT POLYGON reprojected to EPSG:4326.',
    bbox_min_lon                DOUBLE                COMMENT 'Min longitude (EPSG:4326).',
    bbox_min_lat                DOUBLE                COMMENT 'Min latitude (EPSG:4326).',
    bbox_max_lon                DOUBLE                COMMENT 'Max longitude (EPSG:4326).',
    bbox_max_lat                DOUBLE                COMMENT 'Max latitude (EPSG:4326).',
    band1_min                   DOUBLE                COMMENT 'Min pixel value in band 1.',
    band1_max                   DOUBLE                COMMENT 'Max pixel value in band 1.',
    band1_mean                  DOUBLE                COMMENT 'Mean pixel value in band 1.',
    band1_count                 LONG                  COMMENT 'Valid pixel count in band 1.',
    adls_path                   STRING                COMMENT 'Volume path (alias for volume_path).',
    ingestion_timestamp         TIMESTAMP             COMMENT 'UTC when this row was written.',
    _source                     STRING                COMMENT 'Source system: NSW_SES | BCC | ES_CNIG.',
    _state                      STRING                COMMENT 'State/country: QLD | NSW | ES.'
)
USING DELTA
COMMENT 'Gold layer: shared multi-source raster catalog. One row per raster file. _source isolates each source pipeline. Spatial metadata populated by source-specific raster ingest notebooks.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''       = ''true'',
    ''delta.autoOptimize.optimizeWrite'' = ''true''
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Build CNIG Gold Catalog

# COMMAND ----------

gold_df = spark.sql(f"""
    SELECT
        raster_id,
        tile_code,
        tile_name,
        demarcacion_code,
        demarcacion_name,
        demarcacion_name_es,
        return_period_years,
        aep_pct,
        probability_class_es,
        probability_class_en,
        product_type,
        product_category_en,
        is_raster,
        crs_epsg_likely,
        crs_name,
        download_url,
        detail_page_url,
        file_size_bytes,
        file_size_mb,
        download_status,
        volume_path,
        download_timestamp,
        CAST(NULL AS STRING)  AS bbox_wkt,
        CAST(NULL AS DOUBLE)  AS bbox_min_lon,
        CAST(NULL AS DOUBLE)  AS bbox_min_lat,
        CAST(NULL AS DOUBLE)  AS bbox_max_lon,
        CAST(NULL AS DOUBLE)  AS bbox_max_lat,
        CAST(NULL AS INT)     AS width_px,
        CAST(NULL AS INT)     AS height_px,
        CURRENT_TIMESTAMP()   AS _gold_processed_at,
        '{SILVER_RUN_ID or ""}' AS _silver_run_id,
        _source,
        _country
    FROM {FQN_SILVER}
    WHERE _is_current = TRUE
      AND _source = 'ES_CNIG'
""")

_catalog_count = gold_df.count()
log.info(f"CNIG gold catalog: {_catalog_count} rows from silver")

# COMMAND ----------

# MAGIC %md ## 6. Write CNIG Gold Catalog (Source-Scoped Overwrite)
# MAGIC
# MAGIC For the Demarcación-specific gold table, we delete rows for this
# MAGIC Demarcación and re-insert from current silver. This avoids wiping
# MAGIC other Demarcaciones when running incremental pipelines.

# COMMAND ----------

# Delete current Demarcación rows then insert
spark.sql(f"""
    DELETE FROM {FQN_GOLD_CNIG}
    WHERE demarcacion_name = '{DEMARCACION}'
""")
log.info(f"CNIG gold catalog: deleted existing rows for demarcacion={DEMARCACION}")

gold_df.createOrReplaceTempView("_incoming_cnig_gold")
spark.sql(f"""
    INSERT INTO {FQN_GOLD_CNIG}
    SELECT * FROM _incoming_cnig_gold
""")
log.info(f"CNIG gold catalog: inserted {_catalog_count} rows into {FQN_GOLD_CNIG}")

# COMMAND ----------

# MAGIC %md ## 7. Optimize CNIG Gold Catalog

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {FQN_GOLD_CNIG}
    ZORDER BY (demarcacion_code, return_period_years, product_category_en)
""")
log.info("CNIG gold catalog: OPTIMIZE + ZORDER complete")

# COMMAND ----------

# MAGIC %md ## 8. Build Shared Raster Catalog Rows (ES_CNIG Contribution)

# COMMAND ----------

shared_catalog_df = spark.sql(f"""
    SELECT
        raster_id,
        raster_id                               AS resource_id,
        volume_path,
        COALESCE(tile_code, raster_id)          AS original_filename,
        'TIF'                                   AS file_format,
        CAST(FALSE AS BOOLEAN)                  AS is_extracted_from_zip,
        'ES'                                    AS source_state,
        'CNIG - Centro Nacional de Información Geográfica' AS source_agency,
        COALESCE(tile_name, tile_code)          AS study_name,
        raster_id                               AS project_id,
        COALESCE(demarcacion_name_es, demarcacion_name) AS council_lga_primary,
        aep_pct                                 AS inferred_aep_pct,
        return_period_years                     AS inferred_return_period_yr,
        'hazard'                                AS value_type,
        CAST(NULL AS DATE)                      AS publication_date,
        crs_epsg_likely                         AS crs_epsg,
        CAST(NULL AS INT)                       AS width_px,
        CAST(NULL AS INT)                       AS height_px,
        CAST(NULL AS INT)                       AS num_bands,
        CAST(NULL AS DOUBLE)                    AS scale_x,
        CAST(NULL AS DOUBLE)                    AS scale_y,
        CAST(NULL AS DOUBLE)                    AS nodata_value,
        CAST(NULL AS STRING)                    AS native_envelope_wkt,
        CAST(NULL AS STRING)                    AS bbox_wkt,
        CAST(NULL AS DOUBLE)                    AS bbox_min_lon,
        CAST(NULL AS DOUBLE)                    AS bbox_min_lat,
        CAST(NULL AS DOUBLE)                    AS bbox_max_lon,
        CAST(NULL AS DOUBLE)                    AS bbox_max_lat,
        CAST(NULL AS DOUBLE)                    AS band1_min,
        CAST(NULL AS DOUBLE)                    AS band1_max,
        CAST(NULL AS DOUBLE)                    AS band1_mean,
        CAST(NULL AS LONG)                      AS band1_count,
        volume_path                             AS adls_path,
        CURRENT_TIMESTAMP()                     AS ingestion_timestamp,
        _source,
        _country                                AS _state
    FROM {FQN_SILVER}
    WHERE _is_current = TRUE
      AND _source = 'ES_CNIG'
      AND is_raster = TRUE
""")

_shared_count = shared_catalog_df.count()
log.info(f"Shared raster catalog: {_shared_count} ES_CNIG rows prepared")

# COMMAND ----------

# MAGIC %md ## 9. MERGE into Shared Raster Catalog
# MAGIC
# MAGIC Use MERGE on raster_id so that subsequent raster ingest (nb05) can
# MAGIC UPDATE spatial metadata columns without losing catalog entries.

# COMMAND ----------

shared_catalog_df.createOrReplaceTempView("_incoming_cnig_shared_catalog")

spark.sql(f"""
    MERGE INTO {FQN_RASTER_CATALOG} AS target
    USING _incoming_cnig_shared_catalog AS source
    ON target.raster_id = source.raster_id
    WHEN MATCHED THEN UPDATE SET
        target.resource_id              = source.resource_id,
        target.original_filename        = source.original_filename,
        target.source_state             = source.source_state,
        target.source_agency            = source.source_agency,
        target.study_name               = source.study_name,
        target.project_id               = source.project_id,
        target.council_lga_primary      = source.council_lga_primary,
        target.inferred_aep_pct         = source.inferred_aep_pct,
        target.inferred_return_period_yr = source.inferred_return_period_yr,
        target.value_type               = source.value_type,
        target.crs_epsg                 = source.crs_epsg,
        target.ingestion_timestamp      = source.ingestion_timestamp,
        target._source                  = source._source,
        target._state                   = source._state
    WHEN NOT MATCHED THEN INSERT *
""")
log.info(f"Shared raster catalog: {_shared_count} ES_CNIG rows merged into {FQN_RASTER_CATALOG}")

# COMMAND ----------

# MAGIC %md ## 10. Optimize Shared Raster Catalog

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {FQN_RASTER_CATALOG}
    ZORDER BY (_source, council_lga_primary, inferred_aep_pct)
""")
log.info("Shared raster catalog: OPTIMIZE + ZORDER complete")

# COMMAND ----------

# MAGIC %md ## 11. Audit Logging

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

# MAGIC %md ## 12. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("CNIG gold catalog pipeline complete")
    log.info(f"  gold_es_cnig_raster_catalog : {_catalog_count} rows")
    log.info(f"  au_flood_raster_catalog     : {_shared_count} ES_CNIG rows merged")
    _pipeline_status = "success"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Gold pipeline failed: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "gold",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_read        = _catalog_count,
        rows_written     = _catalog_count + _shared_count,
        rows_merged      = _shared_count,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "demarcacion":              DEMARCACION,
            "gold_cnig_rows":           _catalog_count,
            "shared_catalog_rows":      _shared_count,
            "silver_run_id":            SILVER_RUN_ID,
            "full_refresh":             FULL_REFRESH,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
