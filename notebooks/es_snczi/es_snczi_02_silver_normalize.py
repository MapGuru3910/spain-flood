# Databricks notebook source
# Spain SNCZI — Silver Normalisation
#
# Purpose : Reads the latest bronze records from bronze_es_snczi_datasets,
#           extracts and reads SHP files from the downloaded ZIP archives using
#           GeoPandas (on the driver), reprojects geometries from ETRS89 UTM
#           (EPSG:25829/25830/25831) or ETRS89 geographic (EPSG:4258) to
#           WGS84 (EPSG:4326), maps Spanish field names to English schema,
#           classifies AEP from return period, and writes to the silver Delta
#           table via MERGE upsert on geometry hash + return period.
#
#           ETRS89 and WGS84 differ by less than 1 metre as of 2026 — the
#           transform is applied for correctness but the practical difference
#           for flood risk assessment at property scale is negligible.
#
# Layer   : Silver  (ceg_delta_silver_prnd.international_flood.silver_es_snczi_flood_zones)
# Reads   : ceg_delta_bronze_prnd.international_flood.bronze_es_snczi_datasets
#           /Volumes/main/flood_risk/raw/es_snczi/  (ZIP archives)
# Writes  : ceg_delta_silver_prnd.international_flood.silver_es_snczi_flood_zones
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Requirements : geopandas, pyproj, fiona — pre-installed on DBR 13+ or install
#                via cluster-scoped init scripts / requirements.txt.
#
# Version : 1.0.0

# COMMAND ----------

# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text(
    "run_id", "",
    "Run ID — leave blank to auto-generate. Should match bronze run_id when chained."
)
dbutils.widgets.text(
    "bronze_run_id", "",
    "Bronze Run ID — _run_id from the bronze scrape to process. Blank = latest run."
)
dbutils.widgets.dropdown(
    "full_refresh", "false", ["true", "false"],
    "Full Refresh — if true, rebuilds silver table from all current bronze records."
)
dbutils.widgets.text(
    "max_features_per_file", "0",
    "Max Features Per File — 0 = unlimited. Set >0 to cap features per SHP for testing."
)

# COMMAND ----------

# MAGIC %md ## 2. Imports and Configuration

# COMMAND ----------

import hashlib
import json
import logging
import os
import re
import uuid
import zipfile
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# GeoPandas + PyProj for SHP reading and reprojection on the driver node.
# These are available on Databricks Runtime 13+ via the pre-installed geospatial libs.
# If not available, install via: %pip install geopandas pyproj fiona
try:
    import geopandas as gpd
    import pyproj
    from shapely.geometry import mapping
    from shapely import wkt as shapely_wkt
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False
    log_msg = "geopandas not available — SHP parsing will be skipped. Install via %pip install geopandas pyproj fiona"

# ── Catalog / table paths ──────────────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
SILVER_CATALOG  = "ceg_delta_silver_prnd"
SCHEMA          = "international_flood"

FQN_BRONZE       = f"{BRONZE_CATALOG}.{SCHEMA}.bronze_es_snczi_datasets"
FQN_AUDIT        = f"{BRONZE_CATALOG}.{SCHEMA}.pipeline_run_log"
FQN_SILVER       = f"{SILVER_CATALOG}.{SCHEMA}.silver_es_snczi_flood_zones"

NOTEBOOK_NAME    = "es_snczi_02_silver_normalize"
NOTEBOOK_VERSION = "1.0.0"
SOURCE_NAME      = "ES_SNCZI"
COUNTRY          = "ES"

# ── Temporary extraction directory (driver-local) ──────────────────────────
TMP_EXTRACT_DIR  = "/tmp/es_snczi_shp_extract"

# ── CRS configuration ──────────────────────────────────────────────────────
# SNCZI SHP files are distributed in ETRS89 geographic (EPSG:4258) or
# ETRS89 UTM zones (EPSG:25829, 25830, 25831).
# All geometries are normalised to WGS84 (EPSG:4326).
TARGET_CRS       = "EPSG:4326"

# ── Spanish → English field name mapping ──────────────────────────────────
# Actual field names may vary by SHP file version; common alternatives listed.
FIELD_TRANSLATIONS = {
    # Return period
    "per_retorno":        "return_period_yr",
    "periodo_retorno":    "return_period_yr",
    "t_retorno":          "return_period_yr",
    "pr":                 "return_period_yr",
    # Flood zone name / identifier
    "zona_inundable":     "flood_zone_name",
    "zona_inund":         "flood_zone_name",
    "nombre":             "flood_zone_name",
    # River / watercourse
    "rio":                "river_name",
    "nombre_rio":         "river_name",
    "cuenca":             "catchment_name",
    # Administrative
    "municipio":          "municipality",
    "provincia":          "province",
    "ccaa":               "autonomous_community",
    "demarcacion":        "river_basin_district",
    "cod_dem":            "river_basin_code",
    # Geometry attributes
    "area":               "area_m2",
    "perimetro":          "perimeter_m",
    # Hazard / depth
    "calado":             "depth_m",
    "velocidad":          "velocity_ms",
    "lamina":             "water_surface_el_m",
    # Status
    "estado":             "status",
    "origen":             "data_origin",
    "fuente":             "data_source",
    "fecha":              "date_published",
    "year":               "year_published",
    "escala":             "map_scale",
    "codigo":             "feature_code",
    "objectid":           "objectid",
    "fid":                "fid",
}

# ── Return period → AEP mapping ────────────────────────────────────────────
RETURN_PERIOD_AEP = {
    10:  {"aep_pct": 10.0, "probability_class_es": "alta",      "probability_class_en": "high"},
    50:  {"aep_pct": 2.0,  "probability_class_es": "frecuente", "probability_class_en": "frequent"},
    100: {"aep_pct": 1.0,  "probability_class_es": "media",     "probability_class_en": "medium"},
    500: {"aep_pct": 0.2,  "probability_class_es": "baja",      "probability_class_en": "low"},
}

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

RUN_ID          = dbutils.widgets.get("run_id").strip()         or str(uuid.uuid4())
BRONZE_RUN_ID   = dbutils.widgets.get("bronze_run_id").strip()  or None
FULL_REFRESH    = dbutils.widgets.get("full_refresh").lower()   == "true"
MAX_FEATURES    = int(dbutils.widgets.get("max_features_per_file") or 0)
PROCESSED_AT    = datetime.now(timezone.utc)

log.info("=" * 60)
log.info(f"ES SNCZI Silver Normalisation  |  Run ID: {RUN_ID}")
log.info(f"Bronze run ID   : {BRONZE_RUN_ID or 'latest'}")
log.info(f"Full refresh    : {FULL_REFRESH}")
log.info(f"Max features    : {MAX_FEATURES or 'unlimited'}")
log.info(f"GeoPandas avail : {GEOPANDAS_AVAILABLE}")
log.info(f"Target CRS      : {TARGET_CRS}")
log.info(f"Started at      : {PROCESSED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Silver Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_SILVER} (
    -- Identity
    zone_id                 STRING     NOT NULL  COMMENT 'Deterministic 32-char hex ID: SHA256(geometry_wkt_wgs84 || "|" || return_period_yr)[:32].',
    dataset_id              STRING               COMMENT 'FK to bronze_es_snczi_datasets.dataset_id (e.g. zi-shp-q100-pb).',
    resource_id             STRING               COMMENT 'FK to bronze_es_snczi_datasets.resource_id.',
    -- Geometry (WGS84 EPSG:4326)
    geometry_wkt            STRING               COMMENT 'Well-Known Text polygon geometry in WGS84 (EPSG:4326). Derived via ST_Transform from native CRS.',
    native_crs_epsg         INTEGER              COMMENT 'EPSG code of the CRS as read from the SHP .prj file. E.g. 4258, 25829, 25830, 25831.',
    -- Return period / AEP classification
    return_period_yr        INTEGER              COMMENT 'Flood return period in years: 10, 50, 100, or 500.',
    aep_pct                 DOUBLE               COMMENT 'Annual Exceedance Probability as a percentage (10.0, 2.0, 1.0, or 0.2).',
    probability_class_es    STRING               COMMENT 'Spanish probability class: alta | frecuente | media | baja.',
    probability_class_en    STRING               COMMENT 'English probability class: high | frequent | medium | low.',
    region                  STRING               COMMENT 'Geographic region: peninsula_baleares | canarias | all.',
    -- Translated attribute fields
    flood_zone_name         STRING               COMMENT 'Flood zone name or identifier (from zona_inundable / nombre field).',
    river_name              STRING               COMMENT 'River or watercourse name (from rio / nombre_rio field).',
    catchment_name          STRING               COMMENT 'Catchment name (from cuenca field).',
    municipality            STRING               COMMENT 'Municipality name (from municipio field).',
    province                STRING               COMMENT 'Province name (from provincia field).',
    autonomous_community    STRING               COMMENT 'Autonomous community / CCAA (from ccaa field).',
    river_basin_district    STRING               COMMENT 'Demarcación hidrográfica (river basin district) name.',
    river_basin_code        STRING               COMMENT 'Code of the Demarcación hidrográfica (from cod_dem field).',
    area_m2                 DOUBLE               COMMENT 'Feature area in square metres (from area field, if present).',
    depth_m                 DOUBLE               COMMENT 'Representative flood depth in metres (from calado field, if present).',
    velocity_ms             DOUBLE               COMMENT 'Representative flow velocity in m/s (from velocidad field, if present).',
    feature_code            STRING               COMMENT 'Internal feature code from the SHP (from codigo / objectid / fid).',
    date_published          STRING               COMMENT 'Publication/data date from the SHP (from fecha field, raw string).',
    -- Bounding box (WGS84, derived from geometry)
    bbox_min_lon            DOUBLE               COMMENT 'Minimum longitude of the feature bounding box (EPSG:4326).',
    bbox_min_lat            DOUBLE               COMMENT 'Minimum latitude of the feature bounding box (EPSG:4326).',
    bbox_max_lon            DOUBLE               COMMENT 'Maximum longitude of the feature bounding box (EPSG:4326).',
    bbox_max_lat            DOUBLE               COMMENT 'Maximum latitude of the feature bounding box (EPSG:4326).',
    -- Pipeline provenance
    shp_filename            STRING               COMMENT 'Name of the SHP file within the ZIP archive this feature was read from.',
    _bronze_run_id          STRING               COMMENT '_run_id of the bronze record that produced this silver record.',
    _silver_processed_at    TIMESTAMP            COMMENT 'UTC timestamp when this silver row was written.',
    _source                 STRING               COMMENT 'Source system — always ES_SNCZI.',
    _country                STRING               COMMENT 'ISO 3166-1 alpha-2 country code — always ES.'
)
USING DELTA
COMMENT 'Silver layer: normalised SNCZI flood zone polygon features. One row per unique flood zone polygon per return period. Geometries reprojected to WGS84 (EPSG:4326). MERGE upsert on zone_id.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''        = ''true'',
    ''delta.autoOptimize.optimizeWrite''  = ''true'',
    ''pipelines.autoOptimize.zOrderCols'' = ''return_period_yr,region,river_basin_code''
)
""")

# COMMAND ----------

# MAGIC %md ## 5. Load Bronze Records

# COMMAND ----------

if FULL_REFRESH:
    log.info("Full refresh mode — loading all current downloaded bronze records")
    bronze_df = spark.sql(f"""
        SELECT * FROM {FQN_BRONZE}
        WHERE _is_current = TRUE
          AND download_status = 'downloaded'
    """)
elif BRONZE_RUN_ID:
    log.info(f"Incremental mode — processing bronze run_id = {BRONZE_RUN_ID}")
    bronze_df = spark.sql(f"""
        SELECT * FROM {FQN_BRONZE}
        WHERE _run_id = '{BRONZE_RUN_ID}'
          AND _is_current = TRUE
          AND download_status = 'downloaded'
    """)
else:
    log.info("Incremental mode — processing latest bronze run_id")
    latest_run = spark.sql(f"""
        SELECT _run_id
        FROM   {FQN_BRONZE}
        WHERE  download_status = 'downloaded'
        ORDER  BY _ingested_at DESC
        LIMIT  1
    """).collect()
    if not latest_run:
        log.warning("No downloaded bronze records found — nothing to normalise")
        try:
            dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
        except Exception:
            pass
        dbutils.notebook.exit(RUN_ID)
    latest_id = latest_run[0]["_run_id"]
    log.info(f"Latest bronze run_id resolved: {latest_id}")
    bronze_df = spark.sql(f"""
        SELECT * FROM {FQN_BRONZE}
        WHERE _run_id = '{latest_id}'
          AND _is_current = TRUE
          AND download_status = 'downloaded'
    """)

bronze_pdf = bronze_df.toPandas()
_bronze_run_id_used = bronze_pdf["_run_id"].iloc[0] if len(bronze_pdf) > 0 else RUN_ID
log.info(f"Loaded {len(bronze_pdf)} bronze rows with downloaded ZIPs (run_id={_bronze_run_id_used})")

# COMMAND ----------

# MAGIC %md ## 6. SHP Extraction and Normalisation Utilities

# COMMAND ----------

def translate_fields(gdf: "gpd.GeoDataFrame") -> dict:
    """
    Scans a GeoDataFrame's columns for known Spanish field names and returns
    a mapping {original_col: translated_col}. Case-insensitive matching.
    """
    mapping_found = {}
    for col in gdf.columns:
        col_lower = col.lower().strip()
        if col_lower in FIELD_TRANSLATIONS:
            mapping_found[col] = FIELD_TRANSLATIONS[col_lower]
    return mapping_found


def safe_numeric(val) -> Optional[float]:
    """Safely converts a value to float, returning None on failure."""
    try:
        f = float(val)
        return None if (f != f) else f  # NaN → None
    except (TypeError, ValueError):
        return None


def compute_zone_id(geom_wkt: str, return_period: int) -> str:
    """
    Deterministic 32-char zone_id from the WGS84 WKT and return period.
    Stable across re-runs — used as the MERGE key in silver.
    """
    key = f"{geom_wkt or ''}|{return_period}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def extract_shp_from_zip(zip_path: str, extract_dir: str) -> list:
    """
    Extracts all files from a ZIP archive to extract_dir (flat — no subdirs).
    Returns a list of paths to .shp files found inside the archive.
    Guards against zip-slip path traversal.
    """
    os.makedirs(extract_dir, exist_ok=True)
    shp_files = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            safe_name = os.path.basename(member.filename)
            if not safe_name:
                continue
            dest = os.path.join(extract_dir, safe_name)
            with zf.open(member) as src, open(dest, "wb") as dst:
                dst.write(src.read())
            if safe_name.lower().endswith(".shp"):
                shp_files.append(dest)

    log.info(f"Extracted {len(shp_files)} SHP file(s) from {os.path.basename(zip_path)}")
    return shp_files


def read_and_normalise_shp(
    shp_path: str,
    dataset_id: str,
    resource_id: str,
    return_period: int,
    region: str,
    bronze_run_id: str,
) -> list:
    """
    Reads a single SHP file via GeoPandas, reprojects to WGS84, maps field names,
    and returns a list of silver row dicts.

    Parameters
    ----------
    shp_path       : Absolute path to the .shp file
    dataset_id     : Parent bronze dataset_id
    resource_id    : Parent bronze resource_id
    return_period  : Return period in years (10, 50, 100, 500)
    region         : Geographic region string
    bronze_run_id  : _run_id from the parent bronze record

    Returns
    -------
    list of dict — one entry per polygon feature
    """
    if not GEOPANDAS_AVAILABLE:
        log.error("geopandas not available — cannot read SHP files")
        return []

    log.info(f"Reading SHP: {shp_path}")

    try:
        # Read with UTF-8 first; fall back to Latin-1 for accented Spanish characters
        try:
            gdf = gpd.read_file(shp_path, encoding="utf-8")
        except Exception:
            gdf = gpd.read_file(shp_path, encoding="latin-1")

        if MAX_FEATURES > 0:
            gdf = gdf.head(MAX_FEATURES)
            log.info(f"Capped at {MAX_FEATURES} features for testing")

        log.info(f"Read {len(gdf)} features, CRS={gdf.crs}, columns={list(gdf.columns)}")

        # Capture native CRS EPSG before reprojection
        native_epsg = None
        if gdf.crs is not None:
            try:
                native_epsg = gdf.crs.to_epsg()
            except Exception:
                native_epsg = None

        # Reproject to WGS84
        if gdf.crs is None:
            log.warning(f"SHP has no CRS defined — assuming EPSG:25830 (ETRS89 UTM Zone 30N)")
            gdf = gdf.set_crs("EPSG:25830")
        gdf = gdf.to_crs(TARGET_CRS)

        # Translate field names
        field_map = translate_fields(gdf)
        log.info(f"Field translations: {field_map}")

        # AEP classification from return period
        aep_info = RETURN_PERIOD_AEP.get(return_period, {
            "aep_pct": None,
            "probability_class_es": None,
            "probability_class_en": None,
        })

        shp_filename = os.path.basename(shp_path)
        rows = []

        for _, feat in gdf.iterrows():
            geom = feat.geometry
            if geom is None or geom.is_empty:
                continue

            try:
                geom_wkt = geom.wkt
            except Exception:
                continue

            zone_id = compute_zone_id(geom_wkt, return_period)

            # Extract bounds for bbox columns
            try:
                minx, miny, maxx, maxy = geom.bounds
            except Exception:
                minx = miny = maxx = maxy = None

            # Helper to get a translated field value
            def get_field(target_name):
                # Find first original column that maps to this target name
                for orig, trans in field_map.items():
                    if trans == target_name:
                        val = feat.get(orig)
                        if pd.notna(val) if not isinstance(val, str) else bool(val):
                            return str(val) if val is not None else None
                return None

            # Infer return period from SHP field if not already known
            shp_return_period = return_period
            rp_raw = get_field("return_period_yr")
            if rp_raw:
                try:
                    shp_return_period = int(float(rp_raw))
                except (ValueError, TypeError):
                    pass

            row = {
                "zone_id":              zone_id,
                "dataset_id":           dataset_id,
                "resource_id":          resource_id,
                "geometry_wkt":         geom_wkt,
                "native_crs_epsg":      native_epsg,
                "return_period_yr":     shp_return_period,
                "aep_pct":              aep_info["aep_pct"],
                "probability_class_es": aep_info["probability_class_es"],
                "probability_class_en": aep_info["probability_class_en"],
                "region":               region,
                "flood_zone_name":      get_field("flood_zone_name"),
                "river_name":           get_field("river_name"),
                "catchment_name":       get_field("catchment_name"),
                "municipality":         get_field("municipality"),
                "province":             get_field("province"),
                "autonomous_community": get_field("autonomous_community"),
                "river_basin_district": get_field("river_basin_district"),
                "river_basin_code":     get_field("river_basin_code"),
                "area_m2":              safe_numeric(get_field("area_m2")),
                "depth_m":              safe_numeric(get_field("depth_m")),
                "velocity_ms":          safe_numeric(get_field("velocity_ms")),
                "feature_code":         get_field("feature_code") or get_field("objectid") or get_field("fid"),
                "date_published":       get_field("date_published"),
                "bbox_min_lon":         safe_numeric(minx),
                "bbox_min_lat":         safe_numeric(miny),
                "bbox_max_lon":         safe_numeric(maxx),
                "bbox_max_lat":         safe_numeric(maxy),
                "shp_filename":         shp_filename,
                "_bronze_run_id":       bronze_run_id,
                "_silver_processed_at": PROCESSED_AT,
                "_source":              SOURCE_NAME,
                "_country":             COUNTRY,
            }
            rows.append(row)

        log.info(f"Produced {len(rows)} silver rows from {shp_filename}")
        return rows

    except Exception as exc:
        log.error(f"Failed to read/normalise SHP {shp_path}: {exc}", exc_info=True)
        return []

# COMMAND ----------

# MAGIC %md ## 7. Transform: Extract ZIPs and Normalise SHPs

# COMMAND ----------

all_silver_rows = []
_datasets_processed = 0
_features_total     = 0
_shp_errors         = 0

for _, bronze_row in bronze_pdf.iterrows():
    dataset_id   = bronze_row["dataset_id"]
    resource_id  = bronze_row["resource_id"]
    volume_path  = bronze_row["volume_path"]
    return_period = int(bronze_row["return_period_yr"])
    region       = bronze_row["region"]
    bronze_run_id = bronze_row["_run_id"]

    if not volume_path or not os.path.exists(volume_path):
        log.warning(f"[{dataset_id}] volume_path not found: {volume_path} — skipping")
        continue

    # Extract ZIP
    extract_dir = os.path.join(TMP_EXTRACT_DIR, dataset_id)
    try:
        shp_files = extract_shp_from_zip(volume_path, extract_dir)
    except Exception as exc:
        log.error(f"[{dataset_id}] ZIP extraction failed: {exc}")
        _shp_errors += 1
        continue

    if not shp_files:
        log.warning(f"[{dataset_id}] No SHP files found in ZIP: {volume_path}")
        continue

    # Normalise each SHP file found in the ZIP
    for shp_path in shp_files:
        try:
            rows = read_and_normalise_shp(
                shp_path=shp_path,
                dataset_id=dataset_id,
                resource_id=resource_id,
                return_period=return_period,
                region=region,
                bronze_run_id=bronze_run_id,
            )
            all_silver_rows.extend(rows)
            _features_total += len(rows)
        except Exception as exc:
            log.error(f"[{dataset_id}] SHP normalisation failed for {shp_path}: {exc}")
            _shp_errors += 1

    _datasets_processed += 1

log.info(
    f"Normalisation complete: datasets_processed={_datasets_processed} "
    f"features_total={_features_total} shp_errors={_shp_errors}"
)

# COMMAND ----------

# MAGIC %md ## 8. Write Silver (MERGE Upsert)

# COMMAND ----------

_zones_merged = 0

if not all_silver_rows:
    log.warning("No silver rows produced — nothing to write. Check that bronze ZIPs have been downloaded.")
else:
    silver_pdf = pd.DataFrame(all_silver_rows)

    # Coerce numeric columns
    for num_col in ["area_m2", "depth_m", "velocity_ms", "bbox_min_lon", "bbox_min_lat",
                    "bbox_max_lon", "bbox_max_lat", "aep_pct"]:
        silver_pdf[num_col] = pd.to_numeric(silver_pdf[num_col], errors="coerce")
    for int_col in ["return_period_yr", "native_crs_epsg"]:
        silver_pdf[int_col] = pd.to_numeric(silver_pdf[int_col], errors="coerce").astype("Int64")

    # Deduplicate on zone_id (geometry + return_period combination)
    silver_pdf = silver_pdf.drop_duplicates(subset=["zone_id"])
    log.info(f"Deduped to {len(silver_pdf)} unique zone_ids")

    silver_sdf = spark.createDataFrame(silver_pdf)
    silver_sdf.createOrReplaceTempView("_incoming_es_snczi_zones")

    spark.sql(f"""
        MERGE INTO {FQN_SILVER} AS target
        USING _incoming_es_snczi_zones AS source
        ON target.zone_id = source.zone_id
        WHEN MATCHED THEN UPDATE SET
            target.dataset_id              = source.dataset_id,
            target.resource_id             = source.resource_id,
            target.geometry_wkt            = source.geometry_wkt,
            target.native_crs_epsg         = source.native_crs_epsg,
            target.return_period_yr        = source.return_period_yr,
            target.aep_pct                 = source.aep_pct,
            target.probability_class_es    = source.probability_class_es,
            target.probability_class_en    = source.probability_class_en,
            target.region                  = source.region,
            target.flood_zone_name         = source.flood_zone_name,
            target.river_name              = source.river_name,
            target.catchment_name          = source.catchment_name,
            target.municipality            = source.municipality,
            target.province                = source.province,
            target.autonomous_community    = source.autonomous_community,
            target.river_basin_district    = source.river_basin_district,
            target.river_basin_code        = source.river_basin_code,
            target.area_m2                 = source.area_m2,
            target.depth_m                 = source.depth_m,
            target.velocity_ms             = source.velocity_ms,
            target.feature_code            = source.feature_code,
            target.date_published          = source.date_published,
            target.bbox_min_lon            = source.bbox_min_lon,
            target.bbox_min_lat            = source.bbox_min_lat,
            target.bbox_max_lon            = source.bbox_max_lon,
            target.bbox_max_lat            = source.bbox_max_lat,
            target.shp_filename            = source.shp_filename,
            target._bronze_run_id          = source._bronze_run_id,
            target._silver_processed_at    = source._silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    _zones_merged = len(silver_pdf)
    log.info(f"Silver MERGE complete: {_zones_merged} rows merged into {FQN_SILVER}")

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

# MAGIC %md ## 10. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None

try:
    log.info("ES SNCZI silver normalisation complete")
    log.info(f"  Bronze rows processed  : {len(bronze_pdf)}")
    log.info(f"  Datasets processed     : {_datasets_processed}")
    log.info(f"  Features total         : {_features_total}")
    log.info(f"  SHP errors             : {_shp_errors}")
    log.info(f"  Zones merged to silver : {_zones_merged}")
    _pipeline_status = "success" if _shp_errors == 0 else "partial"

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
        rows_written     = _zones_merged,
        rows_merged      = _zones_merged,
        rows_rejected    = _shp_errors,
        error_message    = _pipeline_error,
        extra_metadata   = {
            "bronze_run_id":       _bronze_run_id_used,
            "datasets_processed":  _datasets_processed,
            "features_total":      _features_total,
            "zones_merged":        _zones_merged,
            "shp_errors":          _shp_errors,
            "full_refresh":        FULL_REFRESH,
            "geopandas_available": GEOPANDAS_AVAILABLE,
        },
    )

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
