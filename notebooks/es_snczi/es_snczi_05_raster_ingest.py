# Databricks notebook source
# Spain SNCZI — Raster Ingest (Phase 2 Placeholder)
#
# Purpose : Placeholder notebook for future ingestion of CNIG GeoTIFF flood
#           hazard rasters (Mapas de Peligrosidad por Inundación Fluvial) into
#           the shared au_flood_raster_catalog gold table via Apache Sedona.
#
#           This notebook intentionally does NOT ingest rasters in the current
#           Phase 1 implementation. The SNCZI P0 pipeline focuses on vector
#           flood zone shapefiles (SHP), which are ingested directly into the
#           silver and gold Delta tables by notebooks 02 and 03.
#
# ── Why this notebook exists ──────────────────────────────────────────────
#   The 5-notebook pipeline structure is preserved for consistency with the
#   australia-flood repo pattern (nsw_ses, bcc). Notebook 05 is always the
#   raster ingest stage. Keeping it present (even as a placeholder) means the
#   Databricks Workflow definition and task dependency graph remain symmetric
#   with the Australia pipelines, simplifying operational monitoring.
#
# ── What Phase 2 will implement ──────────────────────────────────────────
#   CNIG (Centro Nacional de Información Geográfica) distributes flood hazard
#   GeoTIFF rasters at 1-metre resolution for each Demarcación Hidrográfica
#   (river basin district). These files are very large (~732 MB per tile at
#   1m resolution) and require a dedicated download pipeline.
#
#   The Phase 2 `es_cnig/` notebook track will implement:
#     notebooks/es_cnig/es_cnig_01_bronze_scrape.py
#       — Scrape the CNIG Centro de Descargas to enumerate GeoTIFF tiles
#         (URL: https://centrodedescargas.cnig.es/CentroDescargas/
#                mapas-peligrosidad-inundacion-fluvial)
#     notebooks/es_cnig/es_cnig_02_silver_normalize.py
#       — Normalise tile metadata; classify return period and Demarcación
#     notebooks/es_cnig/es_cnig_03_gold_catalog.py
#       — Register tiles in au_flood_raster_catalog with _source = 'ES_CNIG'
#     notebooks/es_cnig/es_cnig_04_download.py
#       — Stream download CNIG GeoTIFFs to UC Volume (large-file optimised:
#         4 MB chunks, 1800s timeout, max_workers=2)
#     notebooks/es_cnig/es_cnig_05_raster_ingest.py
#       — Extract spatial metadata via Sedona RS_* functions:
#           RS_SRID → crs_epsg  (expect 25829/25830/25831 — ETRS89 UTM)
#           RS_Width/Height     → raster dimensions
#           RS_Envelope + ST_Transform(... 'EPSG:4326') → WGS84 bbox
#           RS_SummaryStatsAll  → band 1 statistics (depth, hazard class)
#       — MERGE rows into au_flood_raster_catalog with _source = 'ES_CNIG'
#
#   Coverage: T=10, T=100, T=500 year return periods.
#   CRS notes: ETRS89 and WGS84 differ by <1m as of 2026. The existing
#   ST_Transform(geom, 'EPSG:25830', 'EPSG:4326') pattern in the Australia
#   pipeline works identically for Spain — no Sedona code change needed.
#
# ── CNIG Raster Sources ──────────────────────────────────────────────────
#   Flood hazard (fluvial):
#     https://centrodedescargas.cnig.es/CentroDescargas/
#         mapas-peligrosidad-inundacion-fluvial
#   Flood hazard (coastal):
#     https://centrodedescargas.cnig.es/CentroDescargas/
#         mapas-peligrosidad-inundacion-costera
#   DTM high-risk areas:
#     https://centrodedescargas.cnig.es/CentroDescargas/
#         mdt-areas-alto-riesgo-inundacion-fluvial
#
# ── Storage Warning ──────────────────────────────────────────────────────
#   CNIG GeoTIFFs are ~732 MB each. Full national coverage across all return
#   periods and Demarcaciones is hundreds of GB. Start with one Demarcación
#   (recommendation: Júcar — highest recent flood risk post-October 2024) as
#   a proof of concept before committing to full national download.
#
# ── CNIG Download Mechanism ──────────────────────────────────────────────
#   CNIG uses a two-step download:
#     1. Visit: https://centrodedescargas.cnig.es/CentroDescargas/
#               detalleArchivo?sec={tile_id}
#     2. Parse the actual download link from the response HTML
#     3. Stream the GeoTIFF from the resolved link
#   A session cookie may be required. Test manually before automating.
#
# Layer   : Gold (placeholder — no writes in Phase 1)
# Reads   : ceg_delta_bronze_prnd.international_flood.bronze_es_snczi_datasets
# Writes  : ceg_delta_bronze_prnd.international_flood.pipeline_run_log (audit only)
#
# Version : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate."
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

BRONZE_CATALOG   = "ceg_delta_bronze_prnd"
SCHEMA           = "international_flood"
FQN_AUDIT        = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
NOTEBOOK_NAME    = "es_snczi_05_raster_ingest"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_SNCZI"

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

RUN_ID      = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())
STARTED_AT  = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"ES SNCZI Raster Ingest  |  Run ID: {RUN_ID}")
log.info(f"Notebook version : {NOTEBOOK_VERSION}")
log.info(f"Started at       : {STARTED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Phase 1 Skip — Raster Ingest Not Yet Implemented
# MAGIC
# MAGIC **This notebook is a Phase 2 placeholder.**
# MAGIC
# MAGIC The SNCZI P0 pipeline (Phase 1) ingests **vector** flood zone shapefiles,
# MAGIC not rasters. Vector ingestion is handled in notebooks 02 and 03.
# MAGIC
# MAGIC Raster ingestion for CNIG GeoTIFF flood hazard maps is scheduled for Phase 2
# MAGIC and will be implemented in the `notebooks/es_cnig/` track.
# MAGIC
# MAGIC See the notebook header docstring for the full implementation plan.

# COMMAND ----------

_SKIP_REASON = (
    "Phase 1 (SNCZI P0): Raster ingest not yet implemented. "
    "CNIG GeoTIFF flood hazard rasters will be ingested in Phase 2 "
    "via the notebooks/es_cnig/ pipeline track. "
    "Vector flood zone SHPs from SNCZI are processed by notebooks 02 and 03."
)

log.info("=" * 60)
log.info("PHASE 2 PLACEHOLDER — SKIPPING RASTER INGEST")
log.info(_SKIP_REASON)
log.info("")
log.info("Phase 2 implementation plan:")
log.info("  Track : notebooks/es_cnig/ (separate pipeline)")
log.info("  Source: CNIG Centro de Descargas")
log.info("  URL   : https://centrodedescargas.cnig.es/CentroDescargas/mapas-peligrosidad-inundacion-fluvial")
log.info("  Format: GeoTIFF (1m resolution, ~732 MB per tile)")
log.info("  CRS   : ETRS89 UTM zones 29N/30N/31N (EPSG:25829/25830/25831)")
log.info("  Target: au_flood_raster_catalog with _source = 'ES_CNIG'")
log.info("  Return periods: T=10, T=100, T=500 years")
log.info("  Sedona funcs: RS_SRID, RS_Envelope, ST_Transform, RS_SummaryStatsAll")
log.info("=" * 60)

# Summarise what IS available from Phase 1 for context
try:
    vector_summary = spark.sql(f"""
        SELECT
            return_period_yr,
            region,
            COUNT(*) AS zone_count,
            COUNT(DISTINCT river_basin_code) AS distinct_basins
        FROM ceg_delta_silver_prnd.{SCHEMA}.silver_es_snczi_flood_zones
        GROUP BY return_period_yr, region
        ORDER BY return_period_yr, region
    """)
    log.info("Phase 1 vector data currently available in silver:")
    for row in vector_summary.collect():
        log.info(
            f"  T={row['return_period_yr']}yr | region={row['region']} | "
            f"zones={row['zone_count']} | basins={row['distinct_basins']}"
        )
except Exception as exc:
    log.info(f"Silver table not yet populated (expected on first run): {exc}")

# COMMAND ----------

# MAGIC %md ## 5. Phase 2 Raster Ingest — Design Reference
# MAGIC
# MAGIC When Phase 2 is implemented in `notebooks/es_cnig/es_cnig_05_raster_ingest.py`,
# MAGIC it will follow this pattern (adapted from `bcc_05_raster_ingest.py`):
# MAGIC
# MAGIC ```python
# MAGIC from sedona.spark import SedonaContext
# MAGIC sedona = SedonaContext.create(spark)
# MAGIC
# MAGIC # Load GeoTIFF from Volume
# MAGIC binary_df = (
# MAGIC     sedona.read.format("binaryFile")
# MAGIC     .load(batch_paths)
# MAGIC     .withColumn("path_normalised", F.regexp_replace(F.col("path"), "^dbfs:", ""))
# MAGIC )
# MAGIC
# MAGIC # Parse raster
# MAGIC raster_df = binary_df.withColumn("raster", F.expr("RS_FromGeoTiff(content)"))
# MAGIC
# MAGIC # Extract spatial metadata
# MAGIC meta_df = (
# MAGIC     raster_df
# MAGIC     .withColumn("crs_epsg",   F.expr("RS_SRID(raster)"))          # Expect 25829/25830/25831
# MAGIC     .withColumn("width_px",   F.expr("RS_Width(raster)"))
# MAGIC     .withColumn("height_px",  F.expr("RS_Height(raster)"))
# MAGIC     .withColumn("num_bands",  F.expr("RS_NumBands(raster)"))
# MAGIC     .withColumn("scale_x",    F.expr("RS_ScaleX(raster)"))
# MAGIC     .withColumn("scale_y",    F.expr("RS_ScaleY(raster)"))
# MAGIC     .withColumn("nodata",     F.expr("RS_BandNoDataValue(raster, 1)"))
# MAGIC     .withColumn("native_env", F.expr("RS_Envelope(raster)"))
# MAGIC     .withColumn("band1_stats",F.expr("RS_SummaryStatsAll(raster, 1, true)"))
# MAGIC )
# MAGIC
# MAGIC # Reproject to WGS84 — ETRS89 UTM → EPSG:4326
# MAGIC meta_df = meta_df.withColumn(
# MAGIC     "envelope_wgs84",
# MAGIC     F.expr("""
# MAGIC         CASE WHEN crs_epsg IS NOT NULL AND crs_epsg != 0
# MAGIC         THEN ST_Transform(native_env,
# MAGIC                           concat('EPSG:', CAST(crs_epsg AS STRING)),
# MAGIC                           'EPSG:4326')
# MAGIC         ELSE NULL END
# MAGIC     """)
# MAGIC )
# MAGIC
# MAGIC # MERGE into shared gold catalog
# MAGIC result_df.createOrReplaceTempView("_incoming_es_cnig_rasters")
# MAGIC sedona.sql(f"""
# MAGIC     MERGE INTO {FQN_RASTER_CATALOG} AS target
# MAGIC     USING _incoming_es_cnig_rasters AS source
# MAGIC     ON target.raster_id = source.raster_id
# MAGIC     WHEN MATCHED THEN UPDATE SET *
# MAGIC     WHEN NOT MATCHED THEN INSERT *
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC **CRS Note:** ETRS89 (EPSG:25830) and WGS84 (EPSG:4326) differ by <1m as of
# MAGIC 2026 due to continental plate motion. The existing Sedona ST_Transform handles
# MAGIC this correctly — no special handling required beyond using the correct EPSG codes.
# MAGIC
# MAGIC **Tile size warning:** Reduce `batch_size` to 1–3 for CNIG GeoTIFFs (~732 MB each).
# MAGIC Ensure the cluster node type has ≥32 GB RAM (e.g. Standard_DS5_v2).

# COMMAND ----------

# MAGIC %md ## 6. Audit Logging

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

# MAGIC %md ## 7. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "success"  # Intentional skip is a successful outcome
_pipeline_error  = None

try:
    log.info("ES SNCZI raster ingest: Phase 2 placeholder — no rasters processed")
    log.info(f"Skip reason: {_SKIP_REASON}")

except Exception as exc:
    _pipeline_error  = str(exc)
    _pipeline_status = "failed"
    log.error(f"Raster ingest pipeline failed unexpectedly: {exc}", exc_info=True)
    raise

finally:
    _pipeline_end = datetime.now(timezone.utc)
    write_audit(
        run_id           = RUN_ID,
        stage            = "raster_ingest",
        notebook_name    = NOTEBOOK_NAME,
        notebook_version = NOTEBOOK_VERSION,
        start_time       = _pipeline_start,
        end_time         = _pipeline_end,
        status           = _pipeline_status,
        rows_read        = 0,
        rows_written     = 0,
        rows_merged      = 0,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "phase":        1,
            "skip_reason":  _SKIP_REASON,
            "phase_2_track": "notebooks/es_cnig/",
            "cnig_source":  "https://centrodedescargas.cnig.es/CentroDescargas/mapas-peligrosidad-inundacion-fluvial",
            "cnig_target":  "ceg_delta_gold_prnd.international_flood.au_flood_raster_catalog",
            "cnig_source_name": "ES_CNIG",
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
