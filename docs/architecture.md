# Spain Flood Data Pipeline — Architecture

**Version:** v1.0.0  
**Last updated:** 2026-03-28  
**Status:** Implemented (Phase 1 — SNCZI SHP Vector Flood Zones)  
**Author:** Meridian (MapGuru)  
**Based on:** `australia-flood` medallion pipeline (MapGuru3910)

---

## Contents

1. [Overview](#1-overview)
2. [Medallion Layers](#2-medallion-layers)
3. [Pipeline Structure](#3-pipeline-structure)
4. [Data Sources](#4-data-sources)
5. [Table Lineage](#5-table-lineage)
6. [Shared Gold Tables](#6-shared-gold-tables)
7. [CRS / Projection Handling](#7-crs--projection-handling)
8. [Run ID Chaining](#8-run-id-chaining)
9. [Write Strategies by Layer](#9-write-strategies-by-layer)
10. [Unity Catalog Volumes](#10-unity-catalog-volumes)
11. [Phase 2 Roadmap](#11-phase-2-roadmap)
12. [Operational Notes](#12-operational-notes)

---

## 1. Overview

The Spain Flood pipeline extends the `australia-flood` medallion architecture to ingest Spain's national flood zone data from **SNCZI (Sistema Nacional de Cartografía de Zonas Inundables)**, operated by **MITECO** (Ministerio para la Transición Ecológica y el Reto Demográfico).

Spain's flood risk data ecosystem differs from Australia's in a key way: Spain has a **centralised national flood zone mapping system** (SNCZI) rather than Australia's fragmented council-by-council approach. The SNCZI provides national-coverage flood zone shapefiles for four return periods (T=10, T=50, T=100, T=500 years) that can be downloaded from stable, well-known URLs.

### Key Differences from Australia Pipeline

| Dimension | Australia (NSW SES / BCC) | Spain (SNCZI) |
|-----------|---------------------------|----------------|
| Data discovery | CKAN API / ODS API (catalog scrape) | Static known URLs (enumeration) |
| Granularity | Per-study, per-council | National coverage by return period |
| Primary format | GeoTIFF rasters | Shapefiles (SHP) in ZIP |
| Language | English | Spanish (translated in silver) |
| CRS | GDA94/GDA2020 MGA zones | ETRS89 geographic + UTM zones |
| Update frequency | Weekly scrape | Monthly (SNCZI updates annually) |
| Field names | English | Spanish → mapped to English in silver |

---

## 2. Medallion Layers

### Bronze — `ceg_delta_bronze_prnd.international_flood`

Append-only, unmodified record of every pipeline run.

| Property | Value |
|----------|-------|
| Table | `bronze_es_snczi_datasets` |
| Write strategy | Append-only + `_is_current` toggle |
| Granularity | One row per SNCZI dataset per pipeline run |
| History | Full history retained; `_is_current = TRUE` for current state |
| Extra columns | `http_last_modified`, `http_status_code` for change detection |

### Silver — `ceg_delta_silver_prnd.international_flood`

Normalised, classified, geometry-in-WGS84.

| Property | Value |
|----------|-------|
| Table | `silver_es_snczi_flood_zones` |
| Write strategy | MERGE upsert on `zone_id` |
| Granularity | One row per unique flood zone polygon per return period |
| Geometry | WGS84 (EPSG:4326) WKT, reprojecfted from native CRS |
| Classification | AEP %, probability class, English field names |

### Gold — `ceg_delta_gold_prnd.international_flood`

Analysis-ready, denormalized, source-scoped.

| Property | Value |
|----------|-------|
| Source-specific table | `gold_es_snczi_flood_zones` (full overwrite) |
| Shared table | `au_flood_report_index` (DELETE+INSERT WHERE `_source='ES_SNCZI'`) |

---

## 3. Pipeline Structure

```
es_snczi_01_bronze_scrape
    ↓  [run_id via Databricks Task Values]
es_snczi_02_silver_normalize
    ↓
es_snczi_03_gold_catalog
    ↓
es_snczi_04_download         (supplementary files; no-op in standard run)
    ↓
es_snczi_05_raster_ingest    (Phase 2 placeholder; logs skip)
```

Each notebook:
- Reads the run_id set by the previous notebook via `dbutils.jobs.taskValues`
- Writes its own audit record to `pipeline_run_log`
- Sets `run_id` as a task value for the next notebook
- Exits with the run_id string

### Notebook Responsibilities

| Notebook | Stage | Key Actions |
|----------|-------|-------------|
| `es_snczi_01_bronze_scrape.py` | Bronze | HEAD-check 6 SNCZI datasets; download SHP ZIPs to UC Volume; write bronze metadata |
| `es_snczi_02_silver_normalize.py` | Silver | Extract ZIPs; read SHP via GeoPandas; reproject → WGS84; map Spanish→English fields; MERGE silver |
| `es_snczi_03_gold_catalog.py` | Gold | Full overwrite `gold_es_snczi_flood_zones`; source-scoped refresh `au_flood_report_index` |
| `es_snczi_04_download.py` | Download | Download any remaining pending/failed files; typically no-op |
| `es_snczi_05_raster_ingest.py` | Raster | Phase 2 placeholder; logs skip reason + Phase 2 implementation guide |

---

## 4. Data Sources

### SNCZI National Flood Zone Maps (Phase 1 — Implemented)

**Source:** MITECO — Ministerio para la Transición Ecológica y el Reto Demográfico  
**Portal:** https://www.miteco.gob.es/es/cartografia-y-sig/ide/descargas/agua/zi-lamina.html  
**Authentication:** None — public download

| Dataset ID | Return Period | AEP | Region | URL |
|-----------|---------------|-----|--------|-----|
| `zi-shp-q10-pb` | T=10 yr | 10% | Peninsula + Baleares | `...?f=laminasPB-q10.zip` |
| `zi-shp-q50-all` | T=50 yr | 2% | All Spain | `...?f=laminas-q50.zip` |
| `zi-shp-q100-pb` | T=100 yr | 1% | Peninsula + Baleares | `...?f=laminasPB-q100.zip` |
| `zi-shp-q100-canarias` | T=100 yr | 1% | Canary Islands | `...?f=laminasCanarias-q100.zip` |
| `zi-shp-q500-pb` | T=500 yr | 0.2% | Peninsula + Baleares | `...?f=laminasPB-q500.zip` |
| `zi-shp-q500-canarias` | T=500 yr | 0.2% | Canary Islands | `...?f=laminasCanarias-q500.zip` |

**Update cadence:** Infrequent (annual or less; SNCZI follows the EU Floods Directive 6-year cycle)  
**Change detection:** HTTP `Last-Modified` header compared to previous run; unchanged files are skipped

### CNIG Flood Hazard Rasters (Phase 2 — Planned)

**Source:** CNIG — Centro Nacional de Información Geográfica  
**Portal:** https://centrodedescargas.cnig.es/CentroDescargas/mapas-peligrosidad-inundacion-fluvial  
**Format:** GeoTIFF, 1m resolution, ~732 MB per tile  
**CRS:** ETRS89 UTM zones 29N/30N/31N (EPSG:25829/25830/25831)  
**Target table:** `au_flood_raster_catalog` with `_source = 'ES_CNIG'`  
**Implementation:** `notebooks/es_cnig/` pipeline track (future)

---

## 5. Table Lineage

```
MITECO HTTPS endpoint
        │
        ▼
bronze_es_snczi_datasets          (ceg_delta_bronze_prnd.international_flood)
    + /Volumes/main/flood_risk/raw/es_snczi/*.zip
        │
        ▼ [GeoPandas SHP read + ST_Transform + field translation]
silver_es_snczi_flood_zones       (ceg_delta_silver_prnd.international_flood)
        │
        ├──▶ gold_es_snczi_flood_zones     (ceg_delta_gold_prnd.international_flood)
        │
        └──▶ au_flood_report_index         (ceg_delta_gold_prnd.international_flood)
                  [_source = 'ES_SNCZI', alongside NSW_SES and BCC rows]
```

---

## 6. Shared Gold Tables

### `au_flood_report_index`

Multi-source table shared with the Australia pipeline. ES_SNCZI rows are written with:

| Column | ES_SNCZI value |
|--------|----------------|
| `_source` | `ES_SNCZI` |
| `_state` | `ES` |
| `organization_name` | `MITECO — Ministerio para la Transición Ecológica y el Reto Demográfico` |
| `council_lga_primary` | Region name (e.g. `peninsula_baleares`) |
| `river_basin_name` | `SNCZI — Sistema Nacional de Cartografía de Zonas Inundables` |

**Write strategy:** DELETE WHERE `_source = 'ES_SNCZI'` + INSERT — same pattern as BCC.  
SNCZI distributes spatial data rather than PDF reports, so this table typically contains 6 rows (one per dataset) rather than hundreds.

### `au_flood_raster_catalog` (Phase 2)

When Phase 2 is implemented, CNIG GeoTIFFs will be registered here with `_source = 'ES_CNIG'`.

---

## 7. CRS / Projection Handling

### Spain's Coordinate Reference Systems

| EPSG | Name | Coverage | Used by |
|------|------|----------|---------|
| 4258 | ETRS89 Geographic | All Spain | SNCZI SHP vectors (likely) |
| 25829 | ETRS89 / UTM Zone 29N | Western Spain (Galicia) | CNIG rasters |
| 25830 | ETRS89 / UTM Zone 30N | Central Spain (most of peninsula) | CNIG rasters (most common) |
| 25831 | ETRS89 / UTM Zone 31N | Eastern Spain, Baleares | CNIG rasters |
| 32628 | WGS84 / UTM Zone 28N | Canary Islands | Canarias data |
| 4326 | WGS84 | Global | Pipeline standard for all output |

### Reprojection Strategy

The `es_snczi_02_silver_normalize.py` notebook uses **GeoPandas + PyProj** on the Spark driver node to read and reproject SHP features:

```python
gdf = gpd.read_file(shp_path, encoding="utf-8")  # or latin-1 for Spanish chars
gdf = gdf.to_crs("EPSG:4326")  # Reproject to WGS84
```

For Phase 2 raster ingestion (CNIG GeoTIFFs), the existing Sedona pattern is used:

```sql
ST_Transform(RS_Envelope(raster),
             CONCAT('EPSG:', CAST(RS_SRID(raster) AS STRING)),
             'EPSG:4326')
```

**ETRS89 vs WGS84:** These differ by <1m as of 2026. For FM Global flood risk assessment at property scale, the difference is negligible. The pipeline applies the transform for correctness but documents this nuance.

**Canary Islands:** Use UTM Zone 28N (EPSG:32628 or EPSG:25828). Handled as a distinct `region = 'canarias'` value. No special code required — the CRS detection and transform handle it automatically.

**Fallback:** If a SHP has no embedded CRS (rare for SNCZI data), the notebook defaults to EPSG:25830 (ETRS89 UTM Zone 30N — central Spain) and logs a warning.

---

## 8. Run ID Chaining

Each notebook generates (or inherits) a UUID `run_id` and publishes it as a Databricks Task Value:

```python
# Publish at end of notebook
dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)

# Consume in next notebook via widget
# bronze_run_id: "{{tasks.es_snczi_01_bronze_scrape.values.run_id}}"
```

This creates an end-to-end lineage trace through `pipeline_run_log` — every notebook's audit record carries the same `run_id` for the whole workflow execution.

---

## 9. Write Strategies by Layer

| Layer | Table | Strategy | Notes |
|-------|-------|----------|-------|
| Bronze | `bronze_es_snczi_datasets` | Append-only + `_is_current` toggle | Never delete bronze rows |
| Silver | `silver_es_snczi_flood_zones` | MERGE upsert on `zone_id` | `zone_id` = SHA256(geom_wkt + return_period)[:32] |
| Gold (specific) | `gold_es_snczi_flood_zones` | Full overwrite | Reflects current silver state |
| Gold (shared) | `au_flood_report_index` | DELETE+INSERT WHERE `_source='ES_SNCZI'` | Preserves other sources' rows |

---

## 10. Unity Catalog Volumes

| Volume path | Contents |
|-------------|----------|
| `/Volumes/main/flood_risk/raw/es_snczi/` | Root for all SNCZI raw files |
| `/Volumes/main/flood_risk/raw/es_snczi/{region}/t{return_period:04d}/` | SHP ZIPs by region and return period |
| `/Volumes/main/flood_risk/raw/es_snczi/peninsula_baleares/t0100/` | Example: T=100yr Peninsula+Baleares ZIP |
| `/Volumes/main/flood_risk/raw/es_snczi/canarias/t0500/` | Example: T=500yr Canarias ZIP |

**Volume type:** Managed Unity Catalog Volume (`main.flood_risk.raw`).  
**ZIPs are retained** alongside extracted content (extraction is done on the driver, not in the Volume).

---

## 11. Phase 2 Roadmap

| Priority | Track | What | Status |
|----------|-------|------|--------|
| **P0** | `es_snczi` | SNCZI SHP vector flood zones (T=10/50/100/500) | ✅ Implemented |
| **P1** | `es_cnig` | CNIG GeoTIFF flood hazard rasters (1 Demarcación POC) | Planned |
| **P2** | `es_cnig` | CNIG GeoTIFF — all Demarcaciones | Planned |
| **P3** | — | INSPIRE WMS/WFS integration | Future |
| **P4** | `es_aemet` | AEMET precipitation context | Future |
| **P5** | — | SAIH real-time hydrological data (9+ CHs) | Not planned |
| **P6** | — | Copernicus EMS event activations (e.g. EMSR773 Valencia) | Future |

---

## 12. Operational Notes

### Schedule

The `es_snczi_pipeline_job.json` workflow is scheduled monthly (first Sunday at 04:00 Europe/Madrid). SNCZI data updates infrequently — the change detection via `Last-Modified` headers avoids re-downloading unchanged files.

### Network Requirements

The Databricks cluster must have outbound HTTPS (port 443) to:
- `www.mapama.gob.es` — MITECO SHP downloads
- `www.miteco.gob.es` — MITECO portal (HEAD requests)

### Cluster Configuration

- **Runtime:** DBR 17.3 LTS (Spark 4.0, Scala 2.13)
- **Node type:** Standard_DS4_v2 (28 GB RAM, 8 cores) — sufficient for SHP processing
- **Libraries:** Apache Sedona 1.8.1, GeoTools 1.8.1-28.2, geopandas, pyproj, fiona, shapely
- **Workers:** 2–4 autoscale (SNCZI SHP processing is driver-bound; Spark used for Delta writes)

**For Phase 2 (CNIG rasters):** Upgrade to Standard_DS5_v2 (56 GB RAM) and reduce `batch_size` to 1–3. CNIG GeoTIFFs are ~732 MB each.

### Encoding

SNCZI SHP files contain Spanish place names with accented characters (á, é, í, ó, ú, ñ). The silver normalisation notebook tries UTF-8 first and falls back to Latin-1. This handles the character encoding used by Spanish institutional GIS data.

### Shapefile Limitations

SHP format has a 10-character field name limit, which is why Spanish field names in SNCZI data are often truncated (e.g. `per_retorno` instead of `periodo_retorno`). The `FIELD_TRANSLATIONS` dict in notebook 02 includes common variants to handle this.
