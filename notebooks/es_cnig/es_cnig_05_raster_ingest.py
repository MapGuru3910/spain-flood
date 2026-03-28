# Databricks notebook source
# Spain CNIG — Raster Ingest
#
# Purpose : Reads downloaded CNIG flood hazard GeoTIFF files from the Unity
#           Catalog Volume into Apache Sedona, extracts spatial metadata and
#           band statistics via RS_* functions, and writes one row per raster
#           to two gold-layer Delta tables:
#             - silver_es_cnig_raster_tiles — CNIG-specific tile-level metadata
#               with full Sedona RS_* extraction (CRS, dimensions, bbox, stats)
#             - au_flood_raster_catalog — shared multi-source catalog updated
#               via MERGE on raster_id (updates spatial columns populated as
#               NULL during nb03 catalog creation)
#
#           CRS Handling:
#             CNIG GeoTIFFs use ETRS89 UTM zones:
#               EPSG:25829 (Zone 29N) — NW Spain / Galicia
#               EPSG:25830 (Zone 30N) — Central Spain (most common)
#               EPSG:25831 (Zone 31N) — NE Spain / Catalonia / Balearics
#               EPSG:32628 (Zone 28N) — Canary Islands
#             If RS_SRID returns 0 (CRS not embedded), we fall back to
#             crs_epsg_likely from the silver table (derived from demarcación).
#
#           Memory note: 732 MB GeoTIFFs are large. Use batch_size=1 on
#           memory-constrained clusters. Recommended: Standard_DS5_v2 (56 GB RAM)
#           or memory-optimised VM types.
#
# Layer   : Gold + Silver (updates)
# Reads   : ceg_delta_silver_prnd.international_flood.silver_es_cnig_datasets
#           /Volumes/main/flood_risk/raw/es_cnig/
# Writes  : ceg_delta_silver_prnd.international_flood.silver_es_cnig_raster_tiles
#           ceg_delta_gold_prnd.international_flood.au_flood_raster_catalog
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Requires Apache Sedona:
#   org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.8.1
#   org.datasyslab:geotools-wrapper:1.8.1-28.2
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
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, clears ES_CNIG rows from au_flood_raster_catalog and re-ingests all downloaded rasters."
)
dbutils.widgets.text(
    "batch_size", "1",
    "Batch Size — rasters per Spark job. Keep at 1 for 732 MB files unless on a large cluster."
)
dbutils.widgets.dropdown(
    "demarcacion", "Ebro",
    [
        "Ebro", "Tajo", "Guadalquivir", "Segura", "Jucar",
        "Duero", "Mino-Sil", "Cantabrico-Occidental", "Cantabrico-Oriental",
        "Guadalete-Barbate", "Tinto-Odiel-Piedras", "Guadiana",
        "Cuencas-Internas-Cataluna", "Baleares", "Canarias",
    ],
    "Demarcación Hidrográfica — scope ingest to this basin"
)
dbutils.widgets.dropdown(
    "return_period", "all", ["all", "10", "100", "500"],
    "Return Period — ingest only this return period, or all"
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
)
from sedona.spark import SedonaContext

# ── Catalog / table / volume paths ────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
SILVER_CATALOG  = "ceg_delta_silver_prnd"
GOLD_CATALOG    = "ceg_delta_gold_prnd"
SCHEMA          = "international_flood"
VOLUME_BASE     = "/Volumes/main/flood_risk/raw/es_cnig"

FQN_SILVER_DATASETS = f"{SILVER_CATALOG}.{SCHEMA}.silver_es_cnig_datasets"
FQN_SILVER_TILES    = f"{SILVER_CATALOG}.{SCHEMA}.silver_es_cnig_raster_tiles"
FQN_RASTER_CATALOG  = f"{GOLD_CATALOG}.{SCHEMA}.au_flood_raster_catalog"
FQN_AUDIT           = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"

NOTEBOOK_NAME    = "es_cnig_05_raster_ingest"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_CNIG"
COUNTRY          = "ES"

# ── Known ETRS89 UTM EPSG codes — fallback detection ──────────────────────
ETRS89_UTM_EPSGS = {25829, 25830, 25831, 25828, 32628}

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

RUN_ID        = dbutils.widgets.get("run_id").strip()        or str(uuid.uuid4())
FULL_REFRESH  = dbutils.widgets.get("full_refresh").lower() == "true"
BATCH_SIZE    = int(dbutils.widgets.get("batch_size") or 1)
DEMARCACION   = dbutils.widgets.get("demarcacion").strip()   or "Ebro"
RETURN_PERIOD = dbutils.widgets.get("return_period").strip() or "all"
STARTED_AT    = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"CNIG Raster Ingest  |  Run ID: {RUN_ID}")
log.info(f"Demarcación    : {DEMARCACION}")
log.info(f"Return period  : {RETURN_PERIOD}")
log.info(f"Full refresh   : {FULL_REFRESH}")
log.info(f"Batch size     : {BATCH_SIZE}")
log.info(f"Volume base    : {VOLUME_BASE}")
log.info(f"Started at     : {STARTED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Sedona Initialisation

# COMMAND ----------

# SedonaContext.create() registers all RS_* and ST_* SQL functions.
# Sedona JARs must be installed as cluster libraries:
#   org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.8.1
#   org.datasyslab:geotools-wrapper:1.8.1-28.2
sedona = SedonaContext.create(spark)
log.info("Sedona context initialised")

# COMMAND ----------

# MAGIC %md ## 5. Silver Tiles Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_SILVER_TILES} (
    -- Identity
    raster_id                   STRING     NOT NULL  COMMENT 'Stable tile ID from silver_es_cnig_datasets (SHA-256-based).',
    volume_path                 STRING               COMMENT 'Absolute UC Volume path to the GeoTIFF file.',
    original_filename           STRING               COMMENT 'Filename as stored in the Volume.',
    -- Study context (denormalized)
    demarcacion_code            STRING               COMMENT 'Demarcación Hidrográfica internal code.',
    demarcacion_name            STRING               COMMENT 'Demarcación slug.',
    demarcacion_name_es         STRING               COMMENT 'Full Spanish name.',
    return_period_years         INTEGER              COMMENT 'Return period in years: 10 | 100 | 500.',
    aep_pct                     DOUBLE               COMMENT 'Annual Exceedance Probability %.',
    product_type                STRING               COMMENT 'peligrosidad_fluvial | peligrosidad_costera | mdt.',
    -- Raster CRS (extracted vs. inferred)
    crs_epsg_embedded           INTEGER              COMMENT 'EPSG from RS_SRID — 0 if not embedded in file.',
    crs_epsg_used               INTEGER              COMMENT 'EPSG actually used for reprojection: embedded if non-zero, else fallback from silver.',
    crs_fallback_applied        BOOLEAN              COMMENT 'TRUE if crs_epsg_likely was used because RS_SRID returned 0.',
    crs_name                    STRING               COMMENT 'Human-readable CRS name.',
    -- Raster dimensions
    width_px                    INTEGER              COMMENT 'Raster width in pixels.',
    height_px                   INTEGER              COMMENT 'Raster height in pixels.',
    num_bands                   INTEGER              COMMENT 'Number of raster bands.',
    scale_x                     DOUBLE               COMMENT 'Pixel width in CRS units (positive).',
    scale_y                     DOUBLE               COMMENT 'Pixel height in CRS units (negative, upper-left origin).',
    nodata_value                DOUBLE               COMMENT 'NoData sentinel for band 1.',
    -- Bounding box in native CRS
    native_envelope_wkt         STRING               COMMENT 'WKT POLYGON in native ETRS89 UTM CRS.',
    -- Bounding box reprojected to WGS84 (EPSG:4326)
    bbox_wkt                    STRING               COMMENT 'WKT POLYGON in EPSG:4326.',
    bbox_min_lon                DOUBLE               COMMENT 'Min longitude (EPSG:4326).',
    bbox_min_lat                DOUBLE               COMMENT 'Min latitude (EPSG:4326).',
    bbox_max_lon                DOUBLE               COMMENT 'Max longitude (EPSG:4326).',
    bbox_max_lat                DOUBLE               COMMENT 'Max latitude (EPSG:4326).',
    -- Band 1 statistics (excludes NoData pixels)
    band1_min                   DOUBLE               COMMENT 'Min pixel value in band 1.',
    band1_max                   DOUBLE               COMMENT 'Max pixel value in band 1.',
    band1_mean                  DOUBLE               COMMENT 'Mean pixel value in band 1.',
    band1_stddev                DOUBLE               COMMENT 'Std dev of pixel values in band 1.',
    band1_count                 LONG                 COMMENT 'Count of valid (non-NoData) pixels.',
    band1_sum                   DOUBLE               COMMENT 'Sum of valid pixel values.',
    -- File provenance
    file_size_bytes             LONG                 COMMENT 'Actual file size on disk.',
    -- Pipeline provenance
    ingestion_timestamp         TIMESTAMP            COMMENT 'UTC when this row was written.',
    _run_id                     STRING               COMMENT 'Pipeline run UUID.',
    _source                     STRING               COMMENT 'ES_CNIG.',
    _country                    STRING               COMMENT 'ES.'
)
USING DELTA
COMMENT 'Silver layer: CNIG GeoTIFF raster tile metadata extracted via Apache Sedona RS_* functions. One row per downloaded GeoTIFF. MERGE on raster_id.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''        = ''true'',
    ''delta.autoOptimize.optimizeWrite''  = ''true'',
    ''pipelines.autoOptimize.zOrderCols'' = ''raster_id,demarcacion_code,return_period_years''
)
""")
log.info(f"Silver tiles table {FQN_SILVER_TILES} ensured")

# COMMAND ----------

# MAGIC %md ## 6. Build Raster Path List

# COMMAND ----------

_rp_filter = ""
if RETURN_PERIOD != "all":
    _rp_filter = f"AND return_period_years = {int(RETURN_PERIOD)}"

resources_df = spark.sql(f"""
    SELECT
        raster_id,
        tile_code,
        tile_name,
        volume_path,
        demarcacion_code,
        demarcacion_name,
        demarcacion_name_es,
        return_period_years,
        aep_pct,
        product_type,
        crs_epsg_likely
    FROM {FQN_SILVER_DATASETS}
    WHERE download_status = 'downloaded'
      AND volume_path IS NOT NULL
      AND _is_current = TRUE
      AND _source = 'ES_CNIG'
      AND demarcacion_name = '{DEMARCACION}'
      {_rp_filter}
""")

raster_rows   = resources_df.collect()
raster_paths  = [row["volume_path"] for row in raster_rows]
TOTAL_RASTERS = len(raster_paths)
log.info(f"Found {TOTAL_RASTERS} downloaded GeoTIFF(s) to ingest (demarcacion={DEMARCACION})")

if TOTAL_RASTERS == 0:
    log.info(
        "No downloaded rasters found — has es_cnig_04_download been run? "
        "Check silver_es_cnig_datasets.download_status for this Demarcación."
    )
    try:
        dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    except Exception:
        pass
    dbutils.notebook.exit(RUN_ID)

# Build lookup dict: volume_path → silver row metadata
raster_meta_by_path = {row["volume_path"]: row for row in raster_rows}

# COMMAND ----------

# MAGIC %md ## 7. CRS Lookup Utility

# COMMAND ----------

CRS_NAMES = {
    25829: "ETRS89 / UTM zone 29N",
    25830: "ETRS89 / UTM zone 30N",
    25831: "ETRS89 / UTM zone 31N",
    25828: "ETRS89 / UTM zone 28N",
    32628: "WGS 84 / UTM zone 28N",
    4258:  "ETRS89 Geographic",
    4326:  "WGS84 Geographic",
}


def resolve_crs(epsg_embedded: Optional[int], epsg_likely: Optional[int]) -> tuple:
    """
    Determines the CRS EPSG to use for reprojection.

    Returns (crs_epsg_used, crs_fallback_applied, crs_name).
    """
    if epsg_embedded and epsg_embedded != 0:
        return epsg_embedded, False, CRS_NAMES.get(epsg_embedded, f"EPSG:{epsg_embedded}")
    # Fallback to the likely CRS from the demarcación metadata
    fallback = epsg_likely or 25830
    return fallback, True, CRS_NAMES.get(fallback, f"EPSG:{fallback}")

# COMMAND ----------

# MAGIC %md ## 8. Full Refresh
# MAGIC
# MAGIC If full_refresh=true, clear the ES_CNIG partition of au_flood_raster_catalog
# MAGIC (scoped to this Demarcación if not doing a full rebuild) and clear the
# MAGIC silver_es_cnig_raster_tiles rows for this Demarcación.

# COMMAND ----------

if FULL_REFRESH:
    sedona.sql(f"""
        DELETE FROM {FQN_RASTER_CATALOG}
        WHERE _source = 'ES_CNIG'
          AND council_lga_primary LIKE '%{DEMARCACION}%'
    """)
    spark.sql(f"""
        DELETE FROM {FQN_SILVER_TILES}
        WHERE demarcacion_name = '{DEMARCACION}'
    """)
    log.info(
        "Full refresh: cleared ES_CNIG rows for demarcacion=%s from raster catalog and silver tiles",
        DEMARCACION,
    )

# COMMAND ----------

# MAGIC %md ## 9. Ingest Rasters in Batches
# MAGIC
# MAGIC ### Key design decisions for 732 MB GeoTIFFs:
# MAGIC
# MAGIC 1. **batch_size=1**: Load one GeoTIFF at a time into Spark.  Reading the
# MAGIC    full binary content of a 732 MB file into a Spark DataFrame requires the
# MAGIC    executor to hold the byte array in JVM heap.  With a single-node driver
# MAGIC    handling `RS_FromGeoTiff`, the effective memory requirement is ~2-3× the
# MAGIC    file size (original + Sedona raster object + metadata extraction overhead).
# MAGIC    Batch size of 1 guarantees GC can reclaim memory between tiles.
# MAGIC
# MAGIC 2. **binaryFile reader**: Standard Databricks approach for loading raster
# MAGIC    content as `content` (BINARY) + `path` + `length` columns.
# MAGIC
# MAGIC 3. **RS_FromGeoTiff / RS_SRID fallback**: CNIG GeoTIFFs *should* embed
# MAGIC    ETRS89 CRS in the GeoTIFF header, but some older tiles may not.  When
# MAGIC    RS_SRID returns 0 we apply the fallback from the silver crs_epsg_likely.
# MAGIC
# MAGIC 4. **ST_Transform for WGS84**: ETRS89 and WGS84 differ by <1m in practice,
# MAGIC    but we use proper ST_Transform (not an identity) for correctness.

# COMMAND ----------

_rows_written = 0

for batch_start in range(0, TOTAL_RASTERS, BATCH_SIZE):
    batch_paths = raster_paths[batch_start : batch_start + BATCH_SIZE]
    batch_num   = batch_start // BATCH_SIZE + 1
    log.info(
        "Batch %d/%d: loading %d GeoTIFF(s) (tiles %d–%d of %d)",
        batch_num,
        (TOTAL_RASTERS + BATCH_SIZE - 1) // BATCH_SIZE,
        len(batch_paths),
        batch_start + 1,
        batch_start + len(batch_paths),
        TOTAL_RASTERS,
    )

    # ── Load binary content ───────────────────────────────────────────────
    binary_df = (
        sedona.read.format("binaryFile")
        .option("recursiveFileLookup", "false")
        .load(batch_paths)
        .withColumn("path_normalised", F.regexp_replace(F.col("path"), "^dbfs:", ""))
        .select("path_normalised", "content", "length")
    )

    # ── Parse into Sedona raster objects ─────────────────────────────────
    raster_df = (
        binary_df
        .withColumn("raster", F.expr("RS_FromGeoTiff(content)"))
        .drop("content")
    )

    # ── Extract RS_* metadata ─────────────────────────────────────────────
    meta_df = (
        raster_df
        .withColumn("crs_epsg_embedded",  F.expr("RS_SRID(raster)"))
        .withColumn("width_px",           F.expr("RS_Width(raster)"))
        .withColumn("height_px",          F.expr("RS_Height(raster)"))
        .withColumn("num_bands",          F.expr("RS_NumBands(raster)"))
        .withColumn("scale_x",            F.expr("RS_ScaleX(raster)"))
        .withColumn("scale_y",            F.expr("RS_ScaleY(raster)"))
        .withColumn("nodata_value",       F.expr("RS_BandNoDataValue(raster, 1)"))
        .withColumn("native_envelope",    F.expr("RS_Envelope(raster)"))
        .withColumn("native_envelope_wkt",F.expr("ST_AsText(RS_Envelope(raster))"))
        .withColumn("_band1_stats",       F.expr("RS_SummaryStatsAll(raster, 1, true)"))
        .withColumn("band1_min",          F.col("_band1_stats.min"))
        .withColumn("band1_max",          F.col("_band1_stats.max"))
        .withColumn("band1_mean",         F.col("_band1_stats.mean"))
        .withColumn("band1_stddev",       F.col("_band1_stats.stddev"))
        .withColumn("band1_count",        F.col("_band1_stats.count"))
        .withColumn("band1_sum",          F.col("_band1_stats.sum"))
        .drop("_band1_stats", "raster")
    )

    # ── Add silver metadata via Python-side join (small lookup) ──────────
    # We use a Python UDF approach to look up silver metadata by path,
    # as the raster_meta_by_path dict is small (a few tiles per batch).
    path_to_meta = {
        path: raster_meta_by_path.get(path, {})
        for path in batch_paths
    }

    @F.udf(StringType())
    def lookup_raster_id(path):
        return path_to_meta.get(path, {}).get("raster_id")

    @F.udf(StringType())
    def lookup_demarcacion_code(path):
        return path_to_meta.get(path, {}).get("demarcacion_code")

    @F.udf(StringType())
    def lookup_demarcacion_name(path):
        return path_to_meta.get(path, {}).get("demarcacion_name")

    @F.udf(StringType())
    def lookup_demarcacion_name_es(path):
        return path_to_meta.get(path, {}).get("demarcacion_name_es")

    @F.udf(IntegerType())
    def lookup_return_period(path):
        rp = path_to_meta.get(path, {}).get("return_period_years")
        return int(rp) if rp else None

    @F.udf(DoubleType())
    def lookup_aep_pct(path):
        aep = path_to_meta.get(path, {}).get("aep_pct")
        return float(aep) if aep else None

    @F.udf(StringType())
    def lookup_product_type(path):
        return path_to_meta.get(path, {}).get("product_type", "peligrosidad_fluvial")

    @F.udf(IntegerType())
    def lookup_crs_epsg_likely(path):
        epsg = path_to_meta.get(path, {}).get("crs_epsg_likely")
        return int(epsg) if epsg else 25830

    @F.udf(StringType())
    def lookup_tile_code(path):
        return path_to_meta.get(path, {}).get("tile_code")

    meta_df = (
        meta_df
        .withColumn("raster_id",           lookup_raster_id(F.col("path_normalised")))
        .withColumn("demarcacion_code",     lookup_demarcacion_code(F.col("path_normalised")))
        .withColumn("demarcacion_name",     lookup_demarcacion_name(F.col("path_normalised")))
        .withColumn("demarcacion_name_es",  lookup_demarcacion_name_es(F.col("path_normalised")))
        .withColumn("return_period_years",  lookup_return_period(F.col("path_normalised")))
        .withColumn("aep_pct",             lookup_aep_pct(F.col("path_normalised")))
        .withColumn("product_type",        lookup_product_type(F.col("path_normalised")))
        .withColumn("crs_epsg_likely",     lookup_crs_epsg_likely(F.col("path_normalised")))
        .withColumn("tile_code",           lookup_tile_code(F.col("path_normalised")))
    )

    # ── CRS resolution: use embedded if non-zero, else fallback ──────────
    meta_df = meta_df.withColumn(
        "crs_epsg_used",
        F.expr("""
            CASE
                WHEN crs_epsg_embedded IS NOT NULL AND crs_epsg_embedded != 0
                THEN crs_epsg_embedded
                ELSE crs_epsg_likely
            END
        """),
    ).withColumn(
        "crs_fallback_applied",
        F.expr("crs_epsg_embedded IS NULL OR crs_epsg_embedded = 0"),
    )

    # ── Reproject native envelope to WGS84 ────────────────────────────────
    # ETRS89 UTM → WGS84: functionally near-identical (<1m) but semantically
    # correct.  ST_Transform handles the datum shift via GeoTools EPSG registry.
    meta_df = (
        meta_df
        .withColumn(
            "envelope_wgs84",
            F.expr("""
                CASE
                    WHEN crs_epsg_used IS NOT NULL AND crs_epsg_used != 0
                    THEN ST_Transform(
                            native_envelope,
                            concat('EPSG:', CAST(crs_epsg_used AS STRING)),
                            'EPSG:4326'
                         )
                    ELSE NULL
                END
            """),
        )
        .withColumn("bbox_wkt",     F.expr("ST_AsText(envelope_wgs84)"))
        .withColumn("bbox_min_lon", F.expr("ST_XMin(envelope_wgs84)"))
        .withColumn("bbox_min_lat", F.expr("ST_YMin(envelope_wgs84)"))
        .withColumn("bbox_max_lon", F.expr("ST_XMax(envelope_wgs84)"))
        .withColumn("bbox_max_lat", F.expr("ST_YMax(envelope_wgs84)"))
        .drop("native_envelope", "envelope_wgs84")
    )

    # ── File size from disk (actual, not Content-Length from HEAD) ────────
    meta_df = meta_df.withColumn("file_size_bytes", F.col("length"))

    # ── Derive filename and CRS name ──────────────────────────────────────
    meta_df = (
        meta_df
        .withColumn(
            "original_filename",
            F.regexp_extract(F.col("path_normalised"), r"([^/]+)$", 1),
        )
        .withColumn(
            "crs_name",
            F.expr("""
                CASE crs_epsg_used
                    WHEN 25829 THEN 'ETRS89 / UTM zone 29N'
                    WHEN 25830 THEN 'ETRS89 / UTM zone 30N'
                    WHEN 25831 THEN 'ETRS89 / UTM zone 31N'
                    WHEN 25828 THEN 'ETRS89 / UTM zone 28N'
                    WHEN 32628 THEN 'WGS 84 / UTM zone 28N'
                    ELSE concat('EPSG:', CAST(crs_epsg_used AS STRING))
                END
            """),
        )
    )

    # ── Build silver tile rows ────────────────────────────────────────────
    silver_tiles_df = meta_df.select(
        F.col("raster_id"),
        F.col("path_normalised").alias("volume_path"),
        F.col("original_filename"),
        F.col("demarcacion_code"),
        F.col("demarcacion_name"),
        F.col("demarcacion_name_es"),
        F.col("return_period_years"),
        F.col("aep_pct"),
        F.col("product_type"),
        F.col("crs_epsg_embedded"),
        F.col("crs_epsg_used"),
        F.col("crs_fallback_applied"),
        F.col("crs_name"),
        F.col("width_px"),
        F.col("height_px"),
        F.col("num_bands"),
        F.col("scale_x"),
        F.col("scale_y"),
        F.col("nodata_value"),
        F.col("native_envelope_wkt"),
        F.col("bbox_wkt"),
        F.col("bbox_min_lon"),
        F.col("bbox_min_lat"),
        F.col("bbox_max_lon"),
        F.col("bbox_max_lat"),
        F.col("band1_min"),
        F.col("band1_max"),
        F.col("band1_mean"),
        F.col("band1_stddev"),
        F.col("band1_count"),
        F.col("band1_sum"),
        F.col("file_size_bytes"),
        F.lit(STARTED_AT).alias("ingestion_timestamp"),
        F.lit(RUN_ID).alias("_run_id"),
        F.lit(SOURCE_NAME).alias("_source"),
        F.lit(COUNTRY).alias("_country"),
    )

    # ── MERGE into silver_es_cnig_raster_tiles ────────────────────────────
    silver_tiles_df.createOrReplaceTempView("_incoming_cnig_tiles")

    sedona.sql(f"""
        MERGE INTO {FQN_SILVER_TILES} AS target
        USING _incoming_cnig_tiles AS source
        ON target.raster_id = source.raster_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info("Batch %d: silver tiles MERGE complete", batch_num)

    # ── UPDATE au_flood_raster_catalog with spatial metadata ──────────────
    # Rows were created by nb03 with NULL spatial columns.
    # Now we fill them in via MERGE on raster_id.
    shared_update_df = meta_df.select(
        F.col("raster_id"),
        F.col("path_normalised").alias("volume_path"),
        F.col("original_filename"),
        F.lit("TIF").alias("file_format"),
        F.lit(False).alias("is_extracted_from_zip"),
        F.lit("ES").alias("source_state"),
        F.lit("CNIG - Centro Nacional de Información Geográfica").alias("source_agency"),
        F.col("tile_code").alias("study_name"),
        F.col("raster_id").alias("project_id"),
        F.col("demarcacion_name_es").alias("council_lga_primary"),
        F.col("aep_pct").alias("inferred_aep_pct"),
        F.col("return_period_years").alias("inferred_return_period_yr"),
        F.lit("hazard").alias("value_type"),
        F.lit(None).cast(DateType()).alias("publication_date"),
        F.col("crs_epsg_used").alias("crs_epsg"),
        F.col("width_px"),
        F.col("height_px"),
        F.col("num_bands"),
        F.col("scale_x"),
        F.col("scale_y"),
        F.col("nodata_value"),
        F.col("native_envelope_wkt"),
        F.col("bbox_wkt"),
        F.col("bbox_min_lon"),
        F.col("bbox_min_lat"),
        F.col("bbox_max_lon"),
        F.col("bbox_max_lat"),
        F.col("band1_min"),
        F.col("band1_max"),
        F.col("band1_mean"),
        F.col("band1_count"),
        F.col("path_normalised").alias("adls_path"),
        F.lit(STARTED_AT).alias("ingestion_timestamp"),
        F.lit(SOURCE_NAME).alias("_source"),
        F.lit(COUNTRY).alias("_state"),
    )

    shared_update_df.createOrReplaceTempView("_cnig_shared_updates")

    sedona.sql(f"""
        MERGE INTO {FQN_RASTER_CATALOG} AS target
        USING _cnig_shared_updates AS source
        ON target.raster_id = source.raster_id
        WHEN MATCHED THEN UPDATE SET
            target.volume_path              = source.volume_path,
            target.original_filename        = source.original_filename,
            target.source_state             = source.source_state,
            target.council_lga_primary      = source.council_lga_primary,
            target.crs_epsg                 = source.crs_epsg,
            target.width_px                 = source.width_px,
            target.height_px                = source.height_px,
            target.num_bands                = source.num_bands,
            target.scale_x                  = source.scale_x,
            target.scale_y                  = source.scale_y,
            target.nodata_value             = source.nodata_value,
            target.native_envelope_wkt      = source.native_envelope_wkt,
            target.bbox_wkt                 = source.bbox_wkt,
            target.bbox_min_lon             = source.bbox_min_lon,
            target.bbox_min_lat             = source.bbox_min_lat,
            target.bbox_max_lon             = source.bbox_max_lon,
            target.bbox_max_lat             = source.bbox_max_lat,
            target.band1_min                = source.band1_min,
            target.band1_max                = source.band1_max,
            target.band1_mean               = source.band1_mean,
            target.band1_count              = source.band1_count,
            target.ingestion_timestamp      = source.ingestion_timestamp
        WHEN NOT MATCHED THEN INSERT *
    """)

    batch_count = silver_tiles_df.count()
    _rows_written += batch_count
    log.info(
        "Batch %d: %d row(s) merged — silver_tiles + au_flood_raster_catalog updated",
        batch_num, batch_count,
    )

log.info("Raster ingest complete — %d total row(s) written", _rows_written)

# COMMAND ----------

# MAGIC %md ## 10. Optimise Gold Table

# COMMAND ----------

sedona.sql(f"""
    OPTIMIZE {FQN_RASTER_CATALOG}
    ZORDER BY (_source, council_lga_primary, inferred_aep_pct, crs_epsg)
""")
log.info("%s: OPTIMIZE + ZORDER complete", FQN_RASTER_CATALOG)

spark.sql(f"""
    OPTIMIZE {FQN_SILVER_TILES}
    ZORDER BY (demarcacion_code, return_period_years)
""")
log.info("%s: OPTIMIZE + ZORDER complete", FQN_SILVER_TILES)

# COMMAND ----------

# MAGIC %md ## 11. Summary Display

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            demarcacion_name, return_period_years, aep_pct,
            crs_epsg_used, crs_fallback_applied,
            width_px, height_px, num_bands,
            ROUND(scale_x, 2) AS scale_x_m, ROUND(ABS(scale_y), 2) AS scale_y_m,
            ROUND(band1_min, 3) AS band1_min, ROUND(band1_max, 3) AS band1_max,
            ROUND(band1_mean, 3) AS band1_mean,
            bbox_min_lon, bbox_min_lat, bbox_max_lon, bbox_max_lat,
            ROUND(file_size_bytes / 1024.0 / 1024.0, 1) AS file_size_mb,
            ingestion_timestamp
        FROM {FQN_SILVER_TILES}
        WHERE demarcacion_name = '{DEMARCACION}'
        ORDER BY return_period_years
    """)
)

# COMMAND ----------

# MAGIC %md ## 12. Audit Logging

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

# MAGIC %md ## 13. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("CNIG raster ingest pipeline complete")
    log.info(f"  Rasters processed : {TOTAL_RASTERS}")
    log.info(f"  Rows written       : {_rows_written}")
    _pipeline_status = "success"

except Exception as exc:
    _pipeline_error = str(exc)
    log.error(f"Raster ingest pipeline failed: {exc}", exc_info=True)
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
        rows_read        = TOTAL_RASTERS,
        rows_written     = _rows_written,
        rows_merged      = _rows_written,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "demarcacion":      DEMARCACION,
            "return_period":    RETURN_PERIOD,
            "total_rasters":    TOTAL_RASTERS,
            "rows_written":     _rows_written,
            "batch_size":       BATCH_SIZE,
            "full_refresh":     FULL_REFRESH,
            "volume_base":      VOLUME_BASE,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
