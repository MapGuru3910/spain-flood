# Spain Flood Data Pipeline — Data Dictionary

**Version:** v1.0.0  
**Last updated:** 2026-03-28  
**Schema:** `international_flood` (shared with Australia pipeline)  
**Catalogs:** `ceg_delta_bronze_prnd`, `ceg_delta_silver_prnd`, `ceg_delta_gold_prnd`

---

## Contents

1. [Bronze Tables](#1-bronze-tables)
   - [bronze_es_snczi_datasets](#11-bronze_es_snczi_datasets)
2. [Silver Tables](#2-silver-tables)
   - [silver_es_snczi_flood_zones](#21-silver_es_snczi_flood_zones)
3. [Gold Tables](#3-gold-tables)
   - [gold_es_snczi_flood_zones](#31-gold_es_snczi_flood_zones)
   - [au_flood_report_index (ES_SNCZI contribution)](#32-au_flood_report_index--es_snczi-contribution)
4. [Shared Tables (reference)](#4-shared-tables-reference)
   - [pipeline_run_log](#41-pipeline_run_log)
5. [Classification Reference](#5-classification-reference)
   - [Return Period / AEP Mapping](#51-return-period--aep-mapping)
   - [Spanish → English Field Translation](#52-spanish--english-field-translation)
   - [Probability Class Vocabulary](#53-probability-class-vocabulary)
6. [Volume Structure](#6-volume-structure)

---

## 1. Bronze Tables

### 1.1 `bronze_es_snczi_datasets`

**Catalog:** `ceg_delta_bronze_prnd.international_flood`  
**Notebook:** `es_snczi_01_bronze_scrape.py`  
**Write strategy:** Append-only; `_is_current` toggled on superseded rows  
**Query pattern:** `WHERE _is_current = TRUE AND download_status = 'downloaded'`

One row per SNCZI dataset per pipeline run. The 6 standard SNCZI datasets (T=10, T=50, T=100×2, T=500×2) each generate one row per monthly run.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `resource_id` | STRING | NO | Deterministic 32-char hex ID. Computed as `SHA256("es_snczi::" + dataset_id)[:32]`. Stable across re-runs. |
| `dataset_id` | STRING | NO | Stable slug identifying the SNCZI dataset (e.g. `zi-shp-q100-pb`). Natural key. |
| `download_url` | STRING | YES | Full HTTPS URL for direct download from MITECO (e.g. `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q100.zip`). |
| `filename` | STRING | YES | ZIP filename component of the download URL (e.g. `laminasPB-q100.zip`). |
| `return_period_yr` | LONG | YES | Return period in years: 10, 50, 100, or 500. |
| `aep_pct` | DOUBLE | YES | Annual Exceedance Probability as a percentage: 10.0, 2.0, 1.0, or 0.2. |
| `probability_class_es` | STRING | YES | Spanish probability class: `alta`, `frecuente`, `media`, `baja`. |
| `region` | STRING | YES | Geographic region: `peninsula_baleares`, `canarias`, or `all`. |
| `resource_format` | STRING | YES | File format: `SHP` (shapefile in ZIP), `KMZ`, `PDF`. |
| `data_type` | STRING | YES | SNCZI data type: `zona_inundable_shp`, `peligrosidad_raster`, `report`. |
| `source_portal` | STRING | YES | Source portal: `MITECO_IDE` or `CNIG`. |
| `description_es` | STRING | YES | Spanish-language description of the dataset. |
| `description_en` | STRING | YES | English-language description of the dataset. |
| `http_content_length` | LONG | YES | `Content-Length` header from HTTP HEAD request (bytes). NULL if server does not return this header. |
| `http_last_modified` | STRING | YES | `Last-Modified` header string from HTTP HEAD request (e.g. `Thu, 15 Feb 2024 10:30:00 GMT`). Used for change detection — if this matches the previous run's value, the file is not re-downloaded. |
| `http_status_code` | LONG | YES | HTTP status code from HEAD request: 200 = available, 404 = not found, etc. |
| `volume_path` | STRING | YES | Absolute path to the downloaded ZIP file in the Unity Catalog Volume. NULL if download was not attempted or failed. |
| `download_status` | STRING | YES | Download lifecycle: `pending` \| `downloaded` \| `failed` \| `skipped_unchanged`. |
| `download_error_message` | STRING | YES | Error detail if download failed. NULL on success. Truncated to 2000 chars. |
| `file_size_bytes_actual` | LONG | YES | Actual bytes written to Volume. NULL if not downloaded. |
| `download_timestamp` | TIMESTAMP | YES | UTC timestamp of successful download. NULL if not downloaded. |
| `_run_id` | STRING | NO | UUID identifying the pipeline run that produced this row. Same across all 5 notebooks in one workflow execution. |
| `_ingested_at` | TIMESTAMP | NO | UTC timestamp when this row was written to bronze. |
| `_scrape_version` | STRING | NO | Semantic version of the bronze scrape notebook at time of run. |
| `_is_current` | BOOLEAN | NO | TRUE if this is the most recent record for this `dataset_id`. Toggled FALSE by subsequent runs. |
| `_source` | STRING | NO | Source system — always `ES_SNCZI`. |
| `_country` | STRING | NO | ISO 3166-1 alpha-2 country code — always `ES`. |

**Table properties:**
- `delta.enableChangeDataFeed = true`
- `delta.autoOptimize.autoCompact = true`
- `delta.autoOptimize.optimizeWrite = true`
- Z-order: `dataset_id, _is_current`

---

## 2. Silver Tables

### 2.1 `silver_es_snczi_flood_zones`

**Catalog:** `ceg_delta_silver_prnd.international_flood`  
**Notebook:** `es_snczi_02_silver_normalize.py`  
**Write strategy:** MERGE upsert on `zone_id`  
**Query pattern:** Direct query — one active row per flood zone polygon

One row per unique flood zone polygon per return period. Geometries are in WGS84 (EPSG:4326). Spanish field names are translated to English. AEP is classified from return period.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `zone_id` | STRING | NO | Deterministic 32-char hex zone identifier. Computed as `SHA256(geometry_wkt_wgs84 + "|" + return_period_yr)[:32]`. Stable MERGE key. |
| `dataset_id` | STRING | YES | FK to `bronze_es_snczi_datasets.dataset_id`. |
| `resource_id` | STRING | YES | FK to `bronze_es_snczi_datasets.resource_id`. |
| `geometry_wkt` | STRING | YES | Well-Known Text polygon geometry in **WGS84 (EPSG:4326)**. Reprojected from native CRS via GeoPandas. |
| `native_crs_epsg` | INTEGER | YES | EPSG code of the native CRS as embedded in the SHP `.prj` file. Common values: 4258 (ETRS89 geographic), 25829, 25830, 25831 (ETRS89 UTM zones 29/30/31N). |
| `return_period_yr` | INTEGER | YES | Flood return period in years: 10, 50, 100, or 500. Sourced from the bronze dataset classification; optionally overridden by the `per_retorno` SHP attribute if present. |
| `aep_pct` | DOUBLE | YES | Annual Exceedance Probability as a percentage: 10.0 (T=10), 2.0 (T=50), 1.0 (T=100), 0.2 (T=500). |
| `probability_class_es` | STRING | YES | Spanish probability class: `alta` \| `frecuente` \| `media` \| `baja`. |
| `probability_class_en` | STRING | YES | English probability class: `high` \| `frequent` \| `medium` \| `low`. |
| `region` | STRING | YES | Geographic region: `peninsula_baleares` \| `canarias` \| `all`. |
| `flood_zone_name` | STRING | YES | Flood zone name or identifier (translated from `zona_inundable`, `nombre`, etc.). |
| `river_name` | STRING | YES | River or watercourse name (translated from `rio`, `nombre_rio`). |
| `catchment_name` | STRING | YES | Catchment or sub-basin name (from `cuenca`). |
| `municipality` | STRING | YES | Municipality (municipio) name. |
| `province` | STRING | YES | Province (provincia) name. |
| `autonomous_community` | STRING | YES | Autonomous community / CCAA (from `ccaa`). |
| `river_basin_district` | STRING | YES | Demarcación hidrográfica (river basin district) name. Spain has 9 mainland + Canarias demarcaciones. |
| `river_basin_code` | STRING | YES | Code of the Demarcación hidrográfica (from `cod_dem`). |
| `area_m2` | DOUBLE | YES | Feature area in square metres (from `area` SHP attribute, if present). |
| `depth_m` | DOUBLE | YES | Representative flood depth in metres (from `calado`). Present in some SNCZI products only. |
| `velocity_ms` | DOUBLE | YES | Representative flow velocity in m/s (from `velocidad`). Present in some SNCZI products only. |
| `feature_code` | STRING | YES | Internal feature code from the SHP (from `codigo`, `objectid`, or `fid` — whichever is populated). |
| `date_published` | STRING | YES | Publication/data date from the SHP attributes (from `fecha`). Raw string — format varies. |
| `bbox_min_lon` | DOUBLE | YES | Minimum longitude of the feature bounding box (EPSG:4326). |
| `bbox_min_lat` | DOUBLE | YES | Minimum latitude of the feature bounding box (EPSG:4326). |
| `bbox_max_lon` | DOUBLE | YES | Maximum longitude of the feature bounding box (EPSG:4326). |
| `bbox_max_lat` | DOUBLE | YES | Maximum latitude of the feature bounding box (EPSG:4326). |
| `shp_filename` | STRING | YES | Name of the SHP file within the ZIP archive this feature was read from. |
| `_bronze_run_id` | STRING | YES | `_run_id` of the bronze record that produced this silver row. |
| `_silver_processed_at` | TIMESTAMP | YES | UTC timestamp when this silver row was written. |
| `_source` | STRING | YES | Source system — always `ES_SNCZI`. |
| `_country` | STRING | YES | ISO 3166-1 alpha-2 country code — always `ES`. |

**Table properties:**
- `delta.enableChangeDataFeed = true`
- `delta.autoOptimize.optimizeWrite = true`
- Z-order: `return_period_yr, region, river_basin_code`

---

## 3. Gold Tables

### 3.1 `gold_es_snczi_flood_zones`

**Catalog:** `ceg_delta_gold_prnd.international_flood`  
**Notebook:** `es_snczi_03_gold_catalog.py`  
**Write strategy:** Full overwrite each run  
**Use:** Primary analysis table for Spain SNCZI flood zone data

Fully denormalized: silver flood zone columns + bronze dataset metadata (source_portal, data_type, download_url, filename). One row per flood zone polygon per return period.

All columns from `silver_es_snczi_flood_zones` are present, plus:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source_portal` | STRING | YES | Source portal identifier (denormalized from bronze): `MITECO_IDE`. |
| `data_type` | STRING | YES | Dataset type (denormalized from bronze): `zona_inundable_shp`. |
| `download_url` | STRING | YES | Original MITECO download URL (denormalized from bronze). |
| `filename` | STRING | YES | Source ZIP filename (denormalized from bronze). |
| `_gold_processed_at` | TIMESTAMP | YES | UTC timestamp when this gold row was written. |
| `_silver_run_id` | STRING | YES | `run_id` of the upstream silver notebook run. |

Z-order: `return_period_yr, region, river_basin_code`

### 3.2 `au_flood_report_index` — ES_SNCZI contribution

**Catalog:** `ceg_delta_gold_prnd.international_flood`  
**Notebook:** `es_snczi_03_gold_catalog.py` (source-scoped refresh)  
**Write strategy:** DELETE WHERE `_source = 'ES_SNCZI'` + INSERT  
**Full schema:** See `australia-flood` repo `docs/data_dictionary.md`

SNCZI distributes spatial data (SHP/KMZ) rather than PDF reports. This table typically contains **6 rows** for ES_SNCZI (one per SNCZI dataset), with `resource_format = 'SHP'` and the national-level dataset as the "study".

ES_SNCZI specific values:

| Column | Value |
|--------|-------|
| `_source` | `ES_SNCZI` |
| `_state` | `ES` |
| `resource_found_via` | `enumerated` |
| `is_final_report` | `TRUE` (primary dataset) |
| `is_appendix` | `FALSE` |
| `organization_name` | `MITECO — Ministerio para la Transición Ecológica y el Reto Demográfico` |
| `river_basin_name` | `SNCZI — Sistema Nacional de Cartografía de Zonas Inundables` |
| `project_url` | `https://www.miteco.gob.es/es/cartografia-y-sig/ide/descargas/agua/zi-lamina.html` |
| `spatial_bbox_*` | `NULL` (national-level datasets have no single bbox) |

---

## 4. Shared Tables (reference)

### 4.1 `pipeline_run_log`

**Catalog:** `ceg_delta_bronze_prnd.international_flood`  
**Shared with:** All pipelines (NSW SES, BCC, ES SNCZI)

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING | UUID shared across all notebooks in one workflow execution. |
| `pipeline_stage` | STRING | Stage: `bronze` \| `silver` \| `gold` \| `download` \| `raster_ingest`. |
| `notebook_name` | STRING | Filename of the notebook (e.g. `es_snczi_01_bronze_scrape`). |
| `notebook_version` | STRING | Semantic version of the notebook. |
| `start_time` | TIMESTAMP | UTC start timestamp. |
| `end_time` | TIMESTAMP | UTC end timestamp. |
| `duration_seconds` | DOUBLE | Wall-clock duration. |
| `status` | STRING | Outcome: `success` \| `partial` \| `failed`. |
| `rows_read` | LONG | Input rows read. |
| `rows_written` | LONG | Output rows written/merged. |
| `rows_merged` | LONG | Rows affected by MERGE. |
| `rows_rejected` | LONG | Rows failed quality validation. |
| `error_message` | STRING | Exception message if `status = failed`. |
| `extra_metadata` | STRING | JSON blob of stage-specific counters (e.g. `{"downloaded": 6, "skipped": 0}`). |
| `databricks_job_id` | STRING | Databricks Workflow job ID. NULL when run interactively. |
| `databricks_run_id` | STRING | Databricks Workflow run ID. NULL when run interactively. |
| `scrape_version` | STRING | Notebook version (redundant with `notebook_version` — kept for schema compatibility). |

---

## 5. Classification Reference

### 5.1 Return Period / AEP Mapping

Spain's SNCZI uses T=period notation. Annual Exceedance Probability (AEP) is the reciprocal.

| Return Period (T, years) | AEP (%) | Spanish Class | English Class | SNCZI Description |
|--------------------------|---------|---------------|---------------|--------------------|
| T = 10 | 10.0% | `alta` | `high` | Zona de Alta Probabilidad — Alta |
| T = 50 | 2.0% | `frecuente` | `frequent` | Zona de Probabilidad Media — Frecuente |
| T = 100 | 1.0% | `media` | `medium` | Zona de Probabilidad Media — Ocasional |
| T = 500 | 0.2% | `baja` | `low` | Zona de Baja Probabilidad — Excepcional |

**Mapping to Australia pipeline conventions:**

| Spain | Australia (BCC/NSW) |
|-------|---------------------|
| T=10 (10% AEP) | q010 / 10yr |
| T=50 (2% AEP) | q050 / 50yr |
| T=100 (1% AEP) | q100 / 100yr |
| T=500 (0.2% AEP) | q0500 / 500yr |

### 5.2 Spanish → English Field Translation

The following field name translations are applied in `es_snczi_02_silver_normalize.py`. SHP field names are limited to 10 characters, so truncated variants are included.

| Spanish field | English column | Notes |
|--------------|----------------|-------|
| `per_retorno`, `periodo_retorno`, `t_retorno`, `pr` | `return_period_yr` | Return period in years |
| `zona_inundable`, `zona_inund`, `nombre` | `flood_zone_name` | Flood zone name/ID |
| `rio`, `nombre_rio` | `river_name` | River/watercourse name |
| `cuenca` | `catchment_name` | Catchment/sub-basin name |
| `municipio` | `municipality` | Municipality |
| `provincia` | `province` | Province |
| `ccaa` | `autonomous_community` | Autonomous community |
| `demarcacion` | `river_basin_district` | Demarcación hidrográfica |
| `cod_dem` | `river_basin_code` | Demarcación code |
| `area` | `area_m2` | Feature area (m²) |
| `calado` | `depth_m` | Flood depth (m) |
| `velocidad` | `velocity_ms` | Flow velocity (m/s) |
| `lamina` | `water_surface_el_m` | Water surface elevation (m) |
| `estado` | `status` | Feature status |
| `origen` | `data_origin` | Data origin |
| `fuente` | `data_source` | Data source |
| `fecha` | `date_published` | Publication date (raw string) |
| `escala` | `map_scale` | Map scale |
| `codigo`, `objectid`, `fid` | `feature_code` | Internal feature code |

### 5.3 Probability Class Vocabulary

| Spanish | English | AEP | Return Period |
|---------|---------|-----|---------------|
| Probabilidad Alta | High Probability | > 10% | T ≤ 10yr |
| Probabilidad Media | Medium Probability | 1–10% | T = 10–100yr |
| Probabilidad Baja | Low Probability | < 1% | T > 100yr |
| Alta | High | 10% | T = 10yr |
| Frecuente | Frequent | 2% | T = 50yr |
| Media / Ocasional | Medium / Occasional | 1% | T = 100yr |
| Baja / Excepcional | Low / Exceptional | 0.2% | T = 500yr |

---

## 6. Volume Structure

**Volume:** `main.flood_risk.raw`  
**FUSE mount path:** `/Volumes/main/flood_risk/raw/`

```
/Volumes/main/flood_risk/raw/
└── es_snczi/
    ├── peninsula_baleares/
    │   ├── t0010/
    │   │   └── laminasPB-q10.zip          # T=10yr Peninsula+Baleares SHP
    │   ├── t0100/
    │   │   └── laminasPB-q100.zip         # T=100yr Peninsula+Baleares SHP
    │   └── t0500/
    │       └── laminasPB-q500.zip         # T=500yr Peninsula+Baleares SHP
    ├── all/
    │   └── t0050/
    │       └── laminas-q50.zip            # T=50yr All Spain SHP
    └── canarias/
        ├── t0100/
        │   └── laminasCanarias-q100.zip   # T=100yr Canarias SHP
        └── t0500/
            └── laminasCanarias-q500.zip   # T=500yr Canarias SHP
```

**File retention:** ZIPs are retained at their `volume_path`. SHP extraction for normalisation is done to a temporary driver-local directory (`/tmp/es_snczi_shp_extract/`) and is not persisted in the Volume — the silver table stores the normalised geometries as WKT.

**Phase 2 (CNIG rasters):** Will use a separate subdirectory `es_cnig/` under the same volume root, organised by Demarcación hidrográfica and return period.
