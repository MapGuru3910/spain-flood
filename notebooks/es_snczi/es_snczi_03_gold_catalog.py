# Databricks notebook source
# Spain SNCZI — Gold Catalog
#
# Purpose : Reads the SNCZI silver flood zones table and produces two gold-layer
#           outputs:
#             - gold_es_snczi_flood_zones   — source-specific gold table;
#                                             full overwrite each run.
#             - au_flood_report_index       — shared multi-source report index;
#                                             source-scoped refresh (DELETE WHERE
#                                             _source='ES_SNCZI' + INSERT).
#
#           SNCZI distributes spatial data rather than PDF reports, so the
#           contribution to au_flood_report_index is typically zero rows.
#           The shared table write is included here for completeness and
#           forward-compatibility (if SNCZI later publishes associated PDF
#           study reports or documentation).
#
# Layer   : Gold  (ceg_delta_gold_prnd.international_flood.*)
# Reads   : ceg_delta_silver_prnd.international_flood.silver_es_snczi_flood_zones
#           ceg_delta_bronze_prnd.international_flood.bronze_es_snczi_datasets
# Writes  : ceg_delta_gold_prnd.international_flood.gold_es_snczi_flood_zones
#           ceg_delta_gold_prnd.international_flood.au_flood_report_index
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Shared table strategy: au_flood_report_index is partitioned by _source.
# ES_SNCZI rows are fully replaced each run; rows from other sources are untouched.
#
# Version : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate. Should match upstream run_id when chained."
)
dbutils.widgets.text(
    "silver_run_id", "",
    "Silver Run ID — informational only; gold always reads the full current silver snapshot."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, drops and recreates gold_es_snczi_flood_zones from all current silver data."
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

FQN_AUDIT           = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_SILVER          = f"{SILVER_CATALOG}.{SCHEMA}.silver_es_snczi_flood_zones"
FQN_BRONZE          = f"{BRONZE_CATALOG}.{SCHEMA}.bronze_es_snczi_datasets"
FQN_GOLD_ZONES      = f"{GOLD_CATALOG}.{SCHEMA}.gold_es_snczi_flood_zones"
FQN_REPORT_INDEX    = f"{GOLD_CATALOG}.{SCHEMA}.au_flood_report_index"

NOTEBOOK_NAME    = "es_snczi_03_gold_catalog"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_SNCZI"
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
PROCESSED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"ES SNCZI Gold Catalog  |  Run ID: {RUN_ID}")
log.info(f"Silver run ID : {SILVER_RUN_ID or 'current snapshot'}")
log.info(f"Full refresh  : {FULL_REFRESH}")
log.info(f"Started at    : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Gold Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{SCHEMA}")

# ── gold_es_snczi_flood_zones ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_GOLD_ZONES} (
    -- Identity
    zone_id                 STRING      NOT NULL  COMMENT 'Stable zone identifier from silver (SHA256 of geometry_wkt + return_period)[:32].',
    dataset_id              STRING                COMMENT 'Source bronze dataset_id (e.g. zi-shp-q100-pb).',
    resource_id             STRING                COMMENT 'Source bronze resource_id.',
    -- Return period / AEP
    return_period_yr        INTEGER               COMMENT 'Flood return period in years: 10, 50, 100, or 500.',
    aep_pct                 DOUBLE                COMMENT 'Annual Exceedance Probability as a percentage.',
    probability_class_es    STRING                COMMENT 'Spanish probability class: alta | frecuente | media | baja.',
    probability_class_en    STRING                COMMENT 'English probability class: high | frequent | medium | low.',
    region                  STRING                COMMENT 'Geographic region: peninsula_baleares | canarias | all.',
    -- Geometry (WGS84 EPSG:4326)
    geometry_wkt            STRING                COMMENT 'Flood zone polygon in WGS84 WKT format.',
    native_crs_epsg         INTEGER               COMMENT 'Native CRS EPSG code from source SHP (e.g. 4258, 25830).',
    -- Bounding box
    bbox_min_lon            DOUBLE                COMMENT 'Minimum longitude (EPSG:4326).',
    bbox_min_lat            DOUBLE                COMMENT 'Minimum latitude (EPSG:4326).',
    bbox_max_lon            DOUBLE                COMMENT 'Maximum longitude (EPSG:4326).',
    bbox_max_lat            DOUBLE                COMMENT 'Maximum latitude (EPSG:4326).',
    -- Contextual attributes (translated from Spanish)
    flood_zone_name         STRING                COMMENT 'Flood zone name or identifier.',
    river_name              STRING                COMMENT 'River or watercourse name.',
    catchment_name          STRING                COMMENT 'Catchment or sub-basin name.',
    municipality            STRING                COMMENT 'Municipality (municipio).',
    province                STRING                COMMENT 'Province (provincia).',
    autonomous_community    STRING                COMMENT 'Autonomous community (comunidad autónoma).',
    river_basin_district    STRING                COMMENT 'Demarcación hidrográfica (river basin district).',
    river_basin_code        STRING                COMMENT 'Code of the Demarcación hidrográfica.',
    area_m2                 DOUBLE                COMMENT 'Feature area in square metres.',
    depth_m                 DOUBLE                COMMENT 'Indicative flood depth in metres.',
    velocity_ms             DOUBLE                COMMENT 'Indicative flow velocity in m/s.',
    feature_code            STRING                COMMENT 'Internal feature code from the source SHP.',
    date_published          STRING                COMMENT 'Publication/data date from the SHP attributes.',
    -- Source metadata (denormalized from bronze)
    source_portal           STRING                COMMENT 'Source portal: MITECO_IDE.',
    data_type               STRING                COMMENT 'Dataset type: zona_inundable_shp.',
    download_url            STRING                COMMENT 'Original MITECO download URL.',
    filename                STRING                COMMENT 'Source ZIP filename.',
    shp_filename            STRING                COMMENT 'SHP filename within the ZIP archive.',
    -- Pipeline provenance
    _gold_processed_at      TIMESTAMP             COMMENT 'UTC timestamp when this gold row was written.',
    _silver_run_id          STRING                COMMENT 'run_id of the upstream silver notebook run.',
    _source                 STRING                COMMENT 'Source system — always ES_SNCZI.',
    _country                STRING                COMMENT 'ISO 3166-1 alpha-2 country code — always ES.'
)
USING DELTA
COMMENT 'Gold layer: fully denormalized SNCZI national flood zone polygons for Spain. One row per unique flood zone feature per return period. Source-specific table — full overwrite each run. Geometries in WGS84 (EPSG:4326).'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''       = ''true'',
    ''delta.autoOptimize.optimizeWrite'' = ''true''
)
""")

# ── au_flood_report_index — shared table DDL (safety net) ─────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_REPORT_INDEX} (
    resource_id                 STRING      NOT NULL  COMMENT 'Unique resource identifier.',
    resource_name               STRING                COMMENT 'Human-readable name of the PDF resource.',
    resource_download_url       STRING                COMMENT 'Resolved direct download URL.',
    resource_format             STRING                COMMENT 'Normalised file format — PDF for most rows.',
    resource_size_bytes         LONG                  COMMENT 'File size in bytes. NULL if undeclared.',
    resource_found_via          STRING                COMMENT 'Discovery method: api | html_scrape | enumerated.',
    is_final_report             BOOLEAN               COMMENT 'TRUE if this is a final study report.',
    is_appendix                 BOOLEAN               COMMENT 'TRUE if this is an appendix or supplementary document.',
    inferred_aep_pct            DOUBLE                COMMENT 'Inferred AEP percentage. NULL for reports covering all AEPs.',
    inferred_return_period_yr   INTEGER               COMMENT 'Inferred return period in years. NULL if not applicable.',
    download_status             STRING                COMMENT 'Lifecycle: pending | downloaded | failed | not_applicable.',
    volume_path                 STRING                COMMENT 'Unity Catalog Volume path once downloaded.',
    download_timestamp          TIMESTAMP             COMMENT 'UTC timestamp of the most recent successful download.',
    project_id                  STRING                COMMENT 'Study/project identifier.',
    project_title               STRING                COMMENT 'Human-readable project/study title.',
    project_url                 STRING                COMMENT 'URL to the study page on the source portal.',
    organization_name           STRING                COMMENT 'Display name of the owning organisation.',
    council_lga_primary         STRING                COMMENT 'Primary LGA/council or demarcación hidrográfica name.',
    place_name                  STRING                COMMENT 'Place name associated with the study area.',
    river_basin_code            STRING                COMMENT 'River basin code.',
    river_basin_name            STRING                COMMENT 'River basin name.',
    spatial_bbox_min_lon        DOUBLE                COMMENT 'Min longitude of study bounding box (EPSG:4326).',
    spatial_bbox_min_lat        DOUBLE                COMMENT 'Min latitude of study bounding box (EPSG:4326).',
    spatial_bbox_max_lon        DOUBLE                COMMENT 'Max longitude of study bounding box (EPSG:4326).',
    spatial_bbox_max_lat        DOUBLE                COMMENT 'Max latitude of study bounding box (EPSG:4326).',
    author_prepared_by          STRING                COMMENT 'Consulting firm or agency that prepared the study.',
    publication_date            DATE                  COMMENT 'Date the study was published or last modified.',
    approval_state              STRING                COMMENT 'Portal workflow state.',
    _gold_processed_at          TIMESTAMP             COMMENT 'UTC timestamp when this gold row was written.',
    _source                     STRING                COMMENT 'Source system identifier: NSW_SES | BCC | ES_SNCZI.',
    _state                      STRING                COMMENT 'State or country code.'
)
USING DELTA
COMMENT 'Gold layer: shared index of all flood study reports and datasets across all ingested flood data sources. One row per resource. Used for cross-source discovery and download tracking.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''       = ''true'',
    ''delta.autoOptimize.optimizeWrite'' = ''true''
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Build Gold Flood Zones (Denormalized Join)

# COMMAND ----------

# Join silver zones with bronze dataset metadata for download_url, filename, etc.
gold_df = spark.sql(f"""
    SELECT
        z.zone_id,
        z.dataset_id,
        z.resource_id,
        z.return_period_yr,
        z.aep_pct,
        z.probability_class_es,
        z.probability_class_en,
        z.region,
        z.geometry_wkt,
        z.native_crs_epsg,
        z.bbox_min_lon,
        z.bbox_min_lat,
        z.bbox_max_lon,
        z.bbox_max_lat,
        z.flood_zone_name,
        z.river_name,
        z.catchment_name,
        z.municipality,
        z.province,
        z.autonomous_community,
        z.river_basin_district,
        z.river_basin_code,
        z.area_m2,
        z.depth_m,
        z.velocity_ms,
        z.feature_code,
        z.date_published,
        b.source_portal,
        b.data_type,
        b.download_url,
        b.filename,
        z.shp_filename,
        CURRENT_TIMESTAMP()                                         AS _gold_processed_at,
        CAST('{SILVER_RUN_ID or ""}' AS STRING)                     AS _silver_run_id,
        z._source,
        z._country
    FROM {FQN_SILVER} AS z
    LEFT JOIN (
        SELECT dataset_id, source_portal, data_type, download_url, filename
        FROM   {FQN_BRONZE}
        WHERE  _is_current = TRUE
    ) AS b
        ON z.dataset_id = b.dataset_id
""")

_gold_count = gold_df.count()
log.info(f"Gold flood zones: {_gold_count} rows assembled from silver + bronze join")

# COMMAND ----------

# MAGIC %md ## 6. Write Gold Flood Zones (Full Overwrite)

# COMMAND ----------

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FQN_GOLD_ZONES)
)
log.info(f"Gold flood zones: {_gold_count} rows written to {FQN_GOLD_ZONES}")

# COMMAND ----------

# MAGIC %md ## 7. Optimize Gold Flood Zones

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {FQN_GOLD_ZONES}
    ZORDER BY (return_period_yr, region, river_basin_code)
""")
log.info(f"{FQN_GOLD_ZONES}: OPTIMIZE + ZORDER complete")

# COMMAND ----------

# MAGIC %md ## 8. Build Report Index Rows (ES_SNCZI Contribution)
# MAGIC
# MAGIC SNCZI distributes spatial vector data rather than PDF reports, so in the
# MAGIC standard case this will produce zero rows. The logic below handles any
# MAGIC future scenario where SNCZI publishes linked PDF study reports or metadata
# MAGIC documents — the bronze table stores them with data_type = 'report'.
# MAGIC
# MAGIC Each bronze dataset that has data_type = 'report' or resource_format = 'PDF'
# MAGIC is written as one row to the shared au_flood_report_index table with
# MAGIC _source = 'ES_SNCZI'.

# COMMAND ----------

report_index_df = spark.sql(f"""
    SELECT
        b.resource_id,
        b.description_en                                             AS resource_name,
        b.download_url                                               AS resource_download_url,
        b.resource_format,
        b.http_content_length                                        AS resource_size_bytes,
        'enumerated'                                                 AS resource_found_via,
        -- is_final_report: TRUE for the primary SHP dataset (not a draft)
        TRUE                                                         AS is_final_report,
        FALSE                                                        AS is_appendix,
        b.aep_pct                                                    AS inferred_aep_pct,
        CAST(b.return_period_yr AS INT)                              AS inferred_return_period_yr,
        b.download_status,
        b.volume_path,
        b.download_timestamp,
        b.dataset_id                                                 AS project_id,
        b.description_en                                             AS project_title,
        'https://www.miteco.gob.es/es/cartografia-y-sig/ide/descargas/agua/zi-lamina.html'
                                                                     AS project_url,
        'MITECO — Ministerio para la Transición Ecológica y el Reto Demográfico'
                                                                     AS organization_name,
        b.region                                                     AS council_lga_primary,
        CAST(NULL AS STRING)                                         AS place_name,
        CAST(NULL AS STRING)                                         AS river_basin_code,
        'SNCZI — Sistema Nacional de Cartografía de Zonas Inundables'
                                                                     AS river_basin_name,
        -- National-level datasets have no single bbox — these remain NULL
        CAST(NULL AS DOUBLE)                                         AS spatial_bbox_min_lon,
        CAST(NULL AS DOUBLE)                                         AS spatial_bbox_min_lat,
        CAST(NULL AS DOUBLE)                                         AS spatial_bbox_max_lon,
        CAST(NULL AS DOUBLE)                                         AS spatial_bbox_max_lat,
        CAST(NULL AS STRING)                                         AS author_prepared_by,
        CAST(NULL AS DATE)                                           AS publication_date,
        CAST(NULL AS STRING)                                         AS approval_state,
        CURRENT_TIMESTAMP()                                          AS _gold_processed_at,
        b._source,
        b._country                                                   AS _state
    FROM {FQN_BRONZE} AS b
    WHERE b._is_current = TRUE
""")

_report_count = report_index_df.count()
log.info(f"ES_SNCZI report index contribution: {_report_count} rows (expected = {6} for SHP datasets, 0 if no PDFs)")

# COMMAND ----------

# MAGIC %md ## 9. Write Report Index (Source-Scoped Refresh)
# MAGIC
# MAGIC Strategy: delete all existing ES_SNCZI rows, then insert the current snapshot.
# MAGIC Rows from other sources (NSW_SES, BCC, etc.) are unaffected.

# COMMAND ----------

spark.sql(f"""
    DELETE FROM {FQN_REPORT_INDEX}
    WHERE _source = 'ES_SNCZI'
""")
log.info(f"Report index: deleted existing ES_SNCZI rows from {FQN_REPORT_INDEX}")

if _report_count > 0:
    report_index_df.createOrReplaceTempView("_incoming_es_snczi_reports")
    spark.sql(f"""
        INSERT INTO {FQN_REPORT_INDEX}
        SELECT * FROM _incoming_es_snczi_reports
    """)
    log.info(f"Report index: inserted {_report_count} ES_SNCZI rows into {FQN_REPORT_INDEX}")
else:
    log.info("Report index: no ES_SNCZI rows to insert (SNCZI distributes spatial data, not PDFs)")

# COMMAND ----------

# MAGIC %md ## 10. Optimize Report Index

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {FQN_REPORT_INDEX}
    ZORDER BY (_source, council_lga_primary, publication_date)
""")
log.info(f"Report index: OPTIMIZE + ZORDER complete")

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
        log.info(f"Audit written: stage={stage} status={status}")
    except Exception as exc:
        log.error(f"write_audit failed (non-fatal): {exc}")

# COMMAND ----------

# MAGIC %md ## 12. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("ES SNCZI gold catalog pipeline complete")
    log.info(f"  gold_es_snczi_flood_zones : {_gold_count} rows")
    log.info(f"  au_flood_report_index     : {_report_count} ES_SNCZI rows")
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
        rows_read        = _gold_count,
        rows_written     = _gold_count + _report_count,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "gold_zones_rows":           _gold_count,
            "report_index_rows":         _report_count,
            "silver_run_id":             SILVER_RUN_ID,
            "full_refresh":              FULL_REFRESH,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
