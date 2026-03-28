# Spain Flood Data Pipeline — Extension Plan

**Based on:** `australia-flood` repo (MapGuru3910)  
**Date:** 2026-03-28  
**Author:** Meridian (geospatial analysis subagent)  
**Status:** Draft — for Kyle's review

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Australia Repo Analysis](#2-australia-repo-analysis)
3. [Spain Flood Data Sources](#3-spain-flood-data-sources)
4. [Gap Analysis — Australia Pattern vs Spain](#4-gap-analysis)
5. [Architecture Changes](#5-architecture-changes)
6. [CRS / Projection Handling](#6-crs--projection-handling)
7. [Schema Mapping](#7-schema-mapping)
8. [Implementation Steps](#8-implementation-steps)
9. [Gotchas & Risks](#9-gotchas--risks)

---

## 1. Executive Summary

The `australia-flood` repo implements a production Databricks medallion pipeline (Bronze → Silver → Gold) that ingests flood study data from two Australian government portals (NSW SES CKAN, Brisbane City Council ODS), normalises them into Delta Lake tables, downloads raster/report files to Unity Catalog Volumes, and extracts spatial metadata via Apache Sedona. The pipeline is designed for extensibility — shared gold tables (`au_flood_report_index`, `au_flood_raster_catalog`) already use a `_source` column for multi-source isolation.

Spain's flood data ecosystem is fundamentally different: **centralised national flood risk mapping** (SNCZI/MITECO) rather than Australia's fragmented council-by-council approach, but **fragmented real-time hydrological data** across 9+ Confederaciones Hidrográficas (CHs). The primary value for FM Global flood risk assessment comes from:

1. **SNCZI national flood zone maps** — vector polygons (SHP) for T=10, 50, 100, 500 year return periods, plus GeoTIFF raster flood hazard maps from CNIG
2. **MITECO INSPIRE WMS/WFS services** — OGC-standard flood hazard and risk map layers
3. **CNIG flood hazard rasters** — 1m resolution GeoTIFF depth/hazard grids per Demarcación Hidrográfica
4. **AEMET weather/precipitation data** — for triggering context and hydrological indicators

---

## 2. Australia Repo Analysis

### Architecture Summary

```
Source Portal → 01_bronze_scrape → 02_silver_normalize → 03_gold_catalog → 04_download → 05_raster_ingest
```

| Component | Detail |
|-----------|--------|
| **Platform** | Databricks on Azure, DBR 17.3 LTS, Spark 4.0, Scala 2.13 |
| **Storage** | ADLS Gen2 via Unity Catalog Volumes (FUSE paths) |
| **Spatial Engine** | Apache Sedona 1.8.1 (RS_* raster + ST_* vector functions) |
| **Write Strategy** | Bronze: append-only w/ `_is_current` toggle; Silver: SCD2 or MERGE; Gold: full overwrite (source-specific) or source-scoped refresh (shared) |
| **Shared Gold Tables** | `au_flood_report_index` (DELETE+INSERT by `_source`), `au_flood_raster_catalog` (MERGE on `raster_id`) |
| **Run ID Chaining** | UUID propagated across 5 notebooks via Databricks Task Values |
| **Audit** | `pipeline_run_log` — append-only, every notebook writes an entry |
| **Output Format** | Delta Lake tables + COG rasters in UC Volumes |
| **Schedule** | Weekly Sunday 16:00 UTC via Databricks Workflows |

### Key Files

| File | Purpose |
|------|---------|
| `notebooks/nsw_ses/nsw_ses_01_bronze_scrape.py` | CKAN API scrape (5-stage parallel), HTML scrape for PDFs |
| `notebooks/nsw_ses/nsw_ses_02_silver_normalize.py` | SCD2 projects, MERGE datasets/resources, classification rules |
| `notebooks/nsw_ses/nsw_ses_03_gold_catalog.py` | Denormalised catalog + shared report index |
| `notebooks/nsw_ses/nsw_ses_04_download.py` | Threaded file download to UC Volume |
| `notebooks/nsw_ses/nsw_ses_05_raster_ingest.py` | Sedona raster metadata extraction |
| `notebooks/bcc/bcc_01_bronze_scrape.py` | ODS API paginated scrape |
| `notebooks/bcc/bcc_02_silver_normalize.py` | Attachment explosion, classification |
| `notebooks/bcc/bcc_03_gold_catalog.py` | Catalog + shared report index |
| `notebooks/bcc/bcc_04_download.py` | ZIP download + extraction |
| `notebooks/bcc/bcc_05_raster_ingest.py` | Sedona raster metadata extraction |
| `jobs/nsw_ses_pipeline_job.json` | Databricks Workflow definition |
| `jobs/bcc_pipeline_job.json` | Databricks Workflow definition |
| `docs/data_dictionary.md` | Full schema for all Delta tables |
| `docs/architecture.md` | Medallion design, ADRs, shared table design |

### Pattern Strengths for Reuse

1. **Source-scoped shared tables** — Adding `_source = 'ES_SNCZI'` to `au_flood_raster_catalog` and `au_flood_report_index` is the documented extension point
2. **Symmetric 5-notebook pipeline** — Can be cloned for Spain, substituting API clients
3. **Classification rules in silver** — AEP inference, product category, raster detection are isolated and replaceable
4. **Sedona raster ingest** — `RS_FromGeoTiff` and `RS_FromArcInfoAsciiGrid` work identically regardless of source country
5. **Run ID chaining + audit log** — Reusable without modification

---

## 3. Spain Flood Data Sources

### 3.1 SNCZI — Sistema Nacional de Cartografía de Zonas Inundables (Primary Source)

The SNCZI is the national flood zone mapping system operated by MITECO. It is the **primary source** for Spain's flood data and the closest equivalent to what the Australia pipeline captures.

#### 3.1.1 Vector Flood Zone Maps (SHP download)

**Portal:** MITECO IDE Downloads — `https://www.miteco.gob.es/es/cartografia-y-sig/ide/descargas/agua/zi-lamina.html`

**Direct download links:**

| Return Period | Probability | Region | Format | URL |
|--------------|------------|--------|--------|-----|
| T=10 years | Alta (High) | Peninsula + Baleares | SHP ZIP | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q10.zip` |
| T=50 years | Frecuente | All Spain | SHP ZIP | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminas-q50.zip` |
| T=100 years | Media/Ocasional | Peninsula + Baleares | SHP ZIP | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q100.zip` |
| T=100 years | Media/Ocasional | Canarias | SHP ZIP | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasCanarias-q100.zip` |
| T=500 years | Baja/Excepcional | Peninsula + Baleares | SHP ZIP | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q500.zip` |
| T=500 years | Baja/Excepcional | Canarias | SHP ZIP | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasCanarias-q500.zip` |
| T=10 years | Alta (High) | All | KMZ | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q10.kmz` |
| T=50 years | Frecuente | All | KMZ | `https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminas-q50.kmz` |
| T=100 years | Media | All (PB+Canarias) | KMZ | Multiple |
| T=500 years | Baja | All (PB+Canarias) | KMZ | Multiple |

- **Authentication:** None — public download
- **Format:** Shapefile (SHP) ZIP and KMZ
- **CRS:** ETRS89 geographic (EPSG:4258) for the SHP files; WGS84 for KMZ
- **Update cadence:** Infrequent (last update varies, typically annual or less)
- **Language:** Spanish field names and metadata

#### 3.1.2 Raster Flood Hazard Maps (GeoTIFF from CNIG)

**Portal:** Centro de Descargas del CNIG (IGN) — `https://centrodedescargas.cnig.es/CentroDescargas/mapas-peligrosidad-inundacion-fluvial`

Also available for coastal: `https://centrodedescargas.cnig.es/CentroDescargas/mapas-peligrosidad-inundacion-costera`

**What's available:**
- Flood hazard maps (Mapas de peligrosidad por inundación fluvial)
- DTM of high-risk flood areas (MDT de las áreas de alto riesgo de inundación fluvial)
- Organized by **Demarcación Hidrográfica** (river basin district)
- Named by CNIG tile codes, e.g., `ESNZSNCZIMPFT010E77`

**Specifics from the CNIG portal:**
- **Format:** GeoTIFF
- **Resolution:** 1 metre
- **File size:** ~732 MB per tile (very large files!)
- **CRS:** ETRS89 UTM zone (likely EPSG:25830 or 25829 depending on region)
- **Authentication:** Free download after accepting terms; may require session/cookie for download link resolution
- **Download mechanism:** `centrodedescargas.cnig.es/CentroDescargas/detalleArchivo?sec={id}` — HTML-based, requires scraping the download link

**Return periods covered:**
- T=10 years (high probability)
- T=100 years (medium probability)
- T=500 years (low probability)

#### 3.1.3 INSPIRE WMS/WFS Services

**MITECO INSPIRE WMS endpoints for flood zones:**

| Layer | WMS URL | Description |
|-------|---------|-------------|
| Z.I. T=10 | `https://wms.mapama.gob.es/sig/agua/ZI_LaminasQ10/wms.aspx` | Flood zone high probability |
| Z.I. T=50 | `https://wms.mapama.gob.es/sig/agua/ZI_LaminasQ50/wms.aspx` | Flood zone frequent |
| Z.I. T=100 | `https://wms.mapama.gob.es/sig/agua/ZI_LaminasQ100/wms.aspx` | Flood zone medium probability |
| Z.I. T=500 | `https://wms.mapama.gob.es/sig/agua/ZI_LaminasQ500/wms.aspx` | Flood zone low probability |
| ARPSI | WMS (via MITECO IDE catalog) | Areas of Significant Potential Flood Risk |
| Flood Risk Maps | WMS (via MITECO IDE catalog) | Risk to population, economic activity, infrastructure |

**ATOM Download Service:**
`https://www.mapama.gob.es/ide/inspire/atom/CategAgua/downloadservice.xml`

- **Authentication:** None
- **Standard:** OGC WMS 1.3.0 / INSPIRE compliant
- **CRS supported:** EPSG:4258 (ETRS89), EPSG:4326 (WGS84), EPSG:25830, EPSG:3857
- **Use case:** For vector extraction via WFS GetFeature (if WFS is available) or programmatic querying of specific areas

#### 3.1.4 SNCZI Web Viewer

**URL:** `https://sig.miteco.gob.es/snczi/index.html?herramienta=DPHZI`

- The SNCZI viewer is a JavaScript web application backed by ArcGIS Server REST services
- Not directly scrapable via API in the same way as CKAN or ODS, but the underlying WMS/WFS services can be consumed programmatically
- Also available as mobile app: **infoAGUA**

### 3.2 AEMET OpenData (Supplementary — Weather/Precipitation)

**API Base:** `https://opendata.aemet.es/opendata/api`  
**Swagger UI:** `https://opendata.aemet.es/dist/index.html`

- **Authentication:** API key required (free registration at `https://opendata.aemet.es/centrodedescargas/altaUsuario`)
- **Format:** JSON (two-step: API returns a URL, then fetch the actual data from that URL)
- **Rate limits:** Yes (not precisely documented — batch requests carefully)
- **Relevant endpoints:**
  - `/valores/climatologicos/diarios/` — daily climate values (precipitation, temperature)
  - `/observacion/convencional/` — real-time weather station observations
  - `/prediccion/especifica/municipio/` — municipal weather forecasts including precipitation alerts
  - `/avisos_cap/` — weather warnings in CAP format (Common Alerting Protocol)
- **Use case for flood pipeline:** Precipitation alerts and historical extreme rainfall can contextualize flood risk zones

### 3.3 SAIH — Sistemas Automáticos de Información Hidrológica (Supplementary — Real-time)

The SAIH network provides real-time hydrological monitoring (river levels, reservoir levels, rainfall) but is operated independently by each Confederación Hidrográfica. There is **no unified API.**

| Confederación Hidrográfica | SAIH Portal | Notes |
|---------------------------|-------------|-------|
| Ebro | `http://www.saihebro.com` | Has historical data download |
| Tajo | `https://saihtajo.chtajo.es` | Limited public access |
| Júcar | `https://saih.chj.es` | Provides some data exports |
| Guadalquivir | `https://www.chguadalquivir.es/saih` | Web viewer |
| Segura | `https://www.chsegura.es/chs/cuenca/redesdecontrol/SAIH/` | Web viewer |
| Duero | `https://www.saihduero.es` | JSON API available |
| Miño-Sil | `https://saih.chminosil.es` | Web viewer |
| Cantábrico Occidental | Via CHCantabrico | Limited |
| Cantábrico Oriental | Via Agencia Vasca del Agua (URA) | Separate system |
| Guadalete-Barbate | Via CHGuadalquivir | Subsumed |
| Cuencas Internas de Cataluña | Via ACA (Agència Catalana de l'Aigua) | `https://aca.gencat.cat/` |

**Assessment:** SAIH data is real-time operational data (river levels, rainfall gauges), not flood study outputs. It is **not the primary target** for this pipeline extension but could be a Phase 2 enhancement. Each CH's system is different, making aggregation extremely expensive.

### 3.4 Copernicus Emergency Management Service (CEMS)

**Rapid Mapping portal:** `https://emergency.copernicus.eu/mapping/`

- Provides flood extent maps for specific emergency activations (EMSR codes)
- Spain has had many activations, especially the devastating October 2024 Valencia flood (EMSR773, EMSR774)
- **Format:** Vector (SHP, GeoJSON), raster (GeoTIFF), PDF maps
- **Authentication:** Free, open access
- **API:** OGC WMS/WFS via `https://emergency.copernicus.eu/mapping/ows/`
- **Use case:** Event-specific flood footprints — valuable for validation and recent event data

### 3.5 datos.gob.es (National Open Data Portal)

**URL:** `https://datos.gob.es`

- CKAN-based portal — same technology as NSW SES!
- Search for "inundación", "zona inundable", "riesgo inundación"
- Contains metadata linking to MITECO and CNIG datasets
- **Use case:** Discovery/catalog layer rather than primary data source

---

## 4. Gap Analysis

### What exists in the Australia pattern vs what needs to change for Spain

| Dimension | Australia Pattern | Spain Requirement | Change Required |
|-----------|------------------|-------------------|-----------------|
| **Data sources** | 2 portals (CKAN + ODS) with well-defined APIs | 1 primary (MITECO/CNIG) + multiple supplementary, mixed APIs (HTTP download, WMS, ATOM, AEMET REST) | **New scraping/download clients** |
| **Portal API type** | CKAN API v3, OpenDataSoft v2.1 | Static file downloads (MITECO), HTML scraping (CNIG), WMS/WFS (INSPIRE), REST API (AEMET) | **Multiple adapter patterns** |
| **Flood data granularity** | Per-council/study — hundreds of individual flood studies | National-level flood zone maps by return period — fewer but much larger files | **Different discovery pattern** — enumerate known datasets rather than scrape a catalog |
| **Return period convention** | AEP % (1%, 0.5%, 0.2%) and Q-notation (q100, q0200) | T=10, 50, 100, 500 years (periodo de retorno) | **New classification rules** for Spanish conventions |
| **Raster size** | Typically MBs per file | 732 MB per CNIG tile at 1m resolution | **Larger batch sizes, longer timeouts, streaming downloads** |
| **Vector data** | Minimal (mostly raster-focused) | SHP flood zone polygons are the primary national dataset | **New notebook or extend nb05 for vector ingest** |
| **CRS** | GDA94/GDA2020 MGA zones (EPSG:28349-28356, 7849-7856) | ETRS89 UTM zones (EPSG:25829, 25830, 25831) + ETRS89 geographic (EPSG:4258) | **Different EPSG codes but same Sedona `ST_Transform` approach** |
| **Language** | English | Spanish field names, metadata, descriptions | **Translation/mapping layer in silver** |
| **Shared gold tables** | `_source` = `NSW_SES` or `BCC` | Add `_source` = `ES_SNCZI`, `ES_CNIG`, `ES_AEMET` | **Schema-compatible — just new `_source` values** |
| **Hierarchy** | org → project → dataset → resource | demarcación_hidrográfica → periodo_retorno → tile/region | **Different silver schema** |
| **SCD2 change tracking** | NSW SES projects change over time | National datasets change infrequently | **Simpler MERGE upsert sufficient** |
| **Download auth** | None (public) | CNIG may require session cookie; AEMET needs API key | **Auth handling in scrape/download notebooks** |
| **Update frequency** | Weekly scrape (portals update frequently) | SNCZI updates infrequently (annual or less) | **Monthly or on-demand schedule** |

---

## 5. Architecture Changes

### 5.1 New Pipeline Module: `es_snczi`

Create a new folder `notebooks/es_snczi/` following the same 5-notebook pattern:

```
notebooks/es_snczi/
  es_snczi_01_bronze_scrape.py      # Enumerate known SNCZI datasets, scrape CNIG catalog page
  es_snczi_02_silver_normalize.py   # Classify flood zone types, parse return periods
  es_snczi_03_gold_catalog.py       # Source-scoped refresh of shared gold tables
  es_snczi_04_download.py           # Download SHP ZIPs from MITECO + GeoTIFFs from CNIG
  es_snczi_05_ingest.py             # Sedona raster metadata + vector polygon ingest
```

### 5.2 New Pipeline Module (Optional Phase 2): `es_aemet`

```
notebooks/es_aemet/
  es_aemet_01_bronze_scrape.py      # AEMET OpenData API — precipitation stations, warnings
  es_aemet_02_silver_normalize.py   # Normalize station data, parse warnings
  es_aemet_03_gold_catalog.py       # Weather station index, alert summary
```

### 5.3 Bronze Tables — New

| Table | Catalog | Source |
|-------|---------|--------|
| `es_snczi_portal_scrape` | `ceg_delta_bronze_prnd.international_flood` | CNIG/MITECO scrape results |
| `es_aemet_scrape` (Phase 2) | `ceg_delta_bronze_prnd.international_flood` | AEMET station/warning data |

### 5.4 Silver Tables — New

| Table | Catalog | Description |
|-------|---------|-------------|
| `es_snczi_flood_zones` | `ceg_delta_silver_prnd.international_flood` | MERGE on zone_id — one row per flood zone polygon |
| `es_snczi_resources` | `ceg_delta_silver_prnd.international_flood` | MERGE on resource_id — one row per downloadable file |
| `es_snczi_demarcaciones` | `ceg_delta_silver_prnd.international_flood` | Lookup table of Demarcaciones Hidrográficas |

### 5.5 Gold Tables — Extended

| Table | Change |
|-------|--------|
| `au_flood_raster_catalog` | Add rows with `_source = 'ES_CNIG'` — CNIG GeoTIFF flood hazard rasters |
| `au_flood_report_index` | Add rows with `_source = 'ES_SNCZI'` — if SNCZI reports/PDFs are discovered |
| `es_snczi_flood_study_catalog` (NEW) | Source-specific denormalised catalog of Spanish flood zones |

**Rename consideration:** The `au_flood_*` gold tables could be renamed to `intl_flood_*` or the `au_` prefix accepted as legacy with documentation.

### 5.6 UC Volume — New

```sql
CREATE VOLUME IF NOT EXISTS ceg_delta_bronze_prnd.international_flood.es_snczi_raw;
```

Volume path convention:
```
/Volumes/ceg_delta_bronze_prnd/international_flood/es_snczi_raw/
  {data_type}/                 # vector_zones, raster_hazard, reports
    {demarcacion_slug}/        # cantabrico-occidental, ebro, tajo, etc.
      {return_period}/         # t010, t050, t100, t500
        {filename}
        extracted/
```

### 5.7 Workflow Job

New file: `jobs/es_snczi_pipeline_job.json`

```json
{
  "name": "intl_flood_es_snczi_pipeline",
  "schedule": {
    "quartz_cron_expression": "0 0 4 ? * 1 1/1 *",
    "timezone_id": "Europe/Madrid",
    "pause_status": "PAUSED"
  },
  "tasks": [
    {"task_key": "es_snczi_01_bronze_scrape", ...},
    {"task_key": "es_snczi_02_silver_normalize", ...},
    {"task_key": "es_snczi_03_gold_catalog", ...},
    {"task_key": "es_snczi_04_download", ...},
    {"task_key": "es_snczi_05_ingest", ...}
  ]
}
```

Schedule: Monthly (SNCZI updates infrequently). Or on-demand for initial load.

---

## 6. CRS / Projection Handling

### Spain's Coordinate Reference Systems

Spain uses ETRS89 (European Terrestrial Reference System 1989) as the official datum, mandated by Royal Decree 1071/2007. The key CRS codes:

| EPSG | Name | Coverage | Use |
|------|------|----------|-----|
| **4258** | ETRS89 Geographic | All Spain | SHP flood zone vectors (likely CRS) |
| **25829** | ETRS89 / UTM Zone 29N | Western Spain (Galicia, western Andalucía) | Raster grids |
| **25830** | ETRS89 / UTM Zone 30N | Central Spain (most of peninsula) | Most common raster CRS |
| **25831** | ETRS89 / UTM Zone 31N | Eastern Spain (Catalonia, Baleares) | Raster grids |
| **4326** | WGS84 | Global | Pipeline standard for bbox storage |
| **32628** | WGS84 / UTM Zone 28N | Canary Islands | Canarias-specific data |

### Reprojection Strategy

The existing pipeline pattern (in `nsw_ses_05_raster_ingest.py` and `bcc_05_raster_ingest.py`) already handles CRS-agnostic reprojection via:

```sql
ST_Transform(
    native_envelope,
    CONCAT('EPSG:', CAST(crs_epsg AS STRING)),
    'EPSG:4326'
)
```

This works identically for ETRS89 UTM zones. The Sedona + GeoTools wrapper already includes the EPSG database for European CRS. No code change needed for the reprojection step.

**Key difference from Australia:** ETRS89 and WGS84 are practically identical at the surface (sub-metre difference as of 2026, vs the 1.8m GDA94→GDA2020 offset in Australia). This means EPSG:4258 → EPSG:4326 is a near-identity transform — less risky than Australian datum conversions.

### CRS Detection for CNIG GeoTIFFs

CNIG GeoTIFFs should have embedded CRS (unlike some Australian `.asc` files). `RS_SRID(raster)` should return the correct EPSG code. However, validate this in development — some Spanish institutional GeoTIFFs embed CRS as a WKT string without an EPSG authority code, which would make `RS_SRID` return 0.

**Fallback strategy:** If `crs_epsg = 0` for CNIG tiles:
1. Parse the tile code to infer the UTM zone (the CNIG naming convention encodes geographic grid references)
2. Set CRS to EPSG:25830 (Zone 30N) as default for peninsular Spain
3. Flag for manual review

---

## 7. Schema Mapping

### 7.1 Bronze: `es_snczi_portal_scrape`

Maps to `nsw_ses_portal_scrape` / `bcc_portal_scrape` conceptually:

| es_snczi column | Type | Maps from | Australia equivalent |
|-----------------|------|-----------|---------------------|
| `resource_id` | STRING | SHA256 of download URL | `resource_id` |
| `resource_name` | STRING | Filename / tile code from CNIG | `resource_name` |
| `resource_download_url` | STRING | Direct download URL | `resource_download_url` |
| `resource_format` | STRING | SHP, GEOTIFF, KMZ | `resource_format` |
| `resource_size_mb` | DOUBLE | From CNIG catalog page | `resource_size_bytes` / 1M |
| `demarcacion_hidrografica` | STRING | River basin district name | `org` (conceptually) |
| `data_type` | STRING | `peligrosidad_fluvial`, `peligrosidad_costera`, `zona_inundable`, `mdt` | `product_category` |
| `return_period_years` | INTEGER | 10, 50, 100, 500 | `inferred_return_period_yr` |
| `region` | STRING | `peninsula_baleares`, `canarias` | `_state` |
| `tile_code` | STRING | e.g., `ESNZSNCZIMPFT010E77` | No equivalent |
| `source_portal` | STRING | `CNIG` or `MITECO_IDE` | No equivalent |
| `publication_date` | STRING | From portal metadata | `publication_date` |
| `_run_id` | STRING | UUID | `_run_id` |
| `_scraped_at` | TIMESTAMP | UTC | `_scraped_at` |
| `_scrape_version` | STRING | Semver | `_scrape_version` |
| `_is_current` | BOOLEAN | Toggle | `_is_current` |
| `_source` | STRING | `ES_SNCZI` | `_source` |
| `_country` | STRING | `ES` | `_state` → becomes `_country` |

### 7.2 Silver: `es_snczi_resources`

| es_snczi column | Type | Maps to AU | Notes |
|-----------------|------|-----------|-------|
| `resource_id` | STRING | `resource_id` | SHA256-based deterministic ID |
| `resource_name` | STRING | `resource_name` | |
| `resource_download_url` | STRING | `resource_download_url` | |
| `resource_format` | STRING | `resource_format` | SHP, GEOTIFF, KMZ |
| `demarcacion_code` | STRING | `river_basin_code` | **New** — code for the Demarcación Hidrográfica |
| `demarcacion_name` | STRING | `river_basin_name` | e.g., "Ebro", "Tajo" |
| `product_category` | STRING | `product_category` | `flood_zone_vector`, `flood_hazard_raster`, `flood_risk_map`, `dtm`, `report` |
| `return_period_years` | INTEGER | `inferred_return_period_yr` | 10, 50, 100, 500 |
| `aep_pct` | DOUBLE | `inferred_aep_pct` | 10.0, 2.0, 1.0, 0.2 |
| `probability_class` | STRING | No equivalent | `alta`, `frecuente`, `media`, `baja` |
| `is_raster` | BOOLEAN | `is_raster_data` | |
| `is_vector` | BOOLEAN | No equivalent | **New** — Spain has significant vector data |
| `is_report` | BOOLEAN | `is_report` | |
| `region` | STRING | `_state` | `peninsula_baleares` or `canarias` |
| `tile_code` | STRING | No equivalent | CNIG tile code |
| `download_status` | STRING | `download_status` | Same lifecycle |
| `volume_path` | STRING | `volume_path` | |
| `extracted_volume_paths` | ARRAY<STRING> | `extracted_volume_paths` | For SHP ZIPs |

### 7.3 Return Period Mapping (Spain ↔ Australia)

| Spain T (years) | Spain probability class | AEP % | Australia equivalent |
|-----------------|------------------------|-------|---------------------|
| T=10 | Alta / High | 10.0% | 10yr / q010 |
| T=50 | Frecuente / Frequent | 2.0% | 50yr / q050 |
| T=100 | Media / Occasional | 1.0% | 100yr / q100 |
| T=500 | Baja / Exceptional | 0.2% | 500yr / q0500 |

### 7.4 Shared Gold Table Integration

**`au_flood_raster_catalog` additions for Spain:**

| Column | Value for Spain |
|--------|----------------|
| `_source` | `ES_CNIG` |
| `source_state` | `ES` (or demarcación code) |
| `source_agency` | `CNIG - Centro Nacional de Información Geográfica` |
| `council_lga_primary` | Demarcación Hidrográfica name (closest equivalent) |
| `value_type` | `hazard` (CNIG produces hazard maps, not raw depth grids in many cases) |

**`au_flood_report_index` — likely minimal for Spain** as SNCZI distributes spatial data rather than PDF reports. But flood study reports ("Estudios de Inundabilidad") may be referenced.

---

## 8. Implementation Steps

### Phase 1: SNCZI Vector Flood Zones (Highest value, simplest)

**Step 1.1 — Create the notebook skeleton**

Clone `notebooks/bcc/` to `notebooks/es_snczi/` and rename all files:
```
es_snczi_01_bronze_scrape.py
es_snczi_02_silver_normalize.py
es_snczi_03_gold_catalog.py
es_snczi_04_download.py
es_snczi_05_ingest.py
```

**Step 1.2 — Implement `es_snczi_01_bronze_scrape.py`**

Unlike the API-based Australian scrapers, this will be a **known-dataset enumerator** because SNCZI download URLs are stable and well-known.

```python
# Define the known SNCZI vector datasets
SNCZI_VECTOR_DATASETS = [
    {"id": "zi-q10-pb", "return_period": 10, "region": "peninsula_baleares",
     "url": "https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q10.zip",
     "format": "SHP"},
    {"id": "zi-q50-all", "return_period": 50, "region": "all",
     "url": "https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminas-q50.zip",
     "format": "SHP"},
    {"id": "zi-q100-pb", "return_period": 100, "region": "peninsula_baleares",
     "url": "https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q100.zip",
     "format": "SHP"},
    {"id": "zi-q100-canarias", "return_period": 100, "region": "canarias",
     "url": "https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasCanarias-q100.zip",
     "format": "SHP"},
    {"id": "zi-q500-pb", "return_period": 500, "region": "peninsula_baleares",
     "url": "https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasPB-q500.zip",
     "format": "SHP"},
    {"id": "zi-q500-canarias", "return_period": 500, "region": "canarias",
     "url": "https://www.mapama.gob.es/app/descargas/descargafichero.aspx?f=laminasCanarias-q500.zip",
     "format": "SHP"},
]
```

Additionally, scrape the CNIG download page (`centrodedescargas.cnig.es/CentroDescargas/mapas-peligrosidad-inundacion-fluvial`) to enumerate GeoTIFF tiles. This requires HTML parsing (similar to NSW SES stage 5 — reuse `beautifulsoup4`):

```python
# Scrape CNIG catalog page to discover GeoTIFF tiles
def scrape_cnig_catalog(base_url: str) -> list[dict]:
    """Parse the CNIG Centro de Descargas page to extract tile metadata."""
    resp = requests.get(base_url, timeout=30)
    soup = BeautifulSoup(resp.text, "html.parser")
    tiles = []
    # Parse table rows for tile name, format, size, download link
    for row in soup.select("table tr"):  # Actual selector TBD after inspecting DOM
        ...
    return tiles
```

**Step 1.3 — Implement `es_snczi_02_silver_normalize.py`**

Simpler than Australian equivalents — no SCD2 needed (datasets don't change version frequently).

Classification rules for Spain:
```python
# Return period classification
RETURN_PERIOD_MAP = {
    10:  {"aep_pct": 10.0, "probability_class": "alta"},
    50:  {"aep_pct": 2.0,  "probability_class": "frecuente"},
    100: {"aep_pct": 1.0,  "probability_class": "media"},
    500: {"aep_pct": 0.2,  "probability_class": "baja"},
}

# Product category from data type
def classify_product(resource_name: str, resource_format: str) -> str:
    name_lower = resource_name.lower()
    if "peligrosidad" in name_lower or "hazard" in name_lower:
        return "flood_hazard_raster"
    if "lamina" in name_lower or "zona_inundable" in name_lower:
        return "flood_zone_vector"
    if "mdt" in name_lower or "dtm" in name_lower:
        return "dtm"
    if resource_format.upper() == "PDF":
        return "report"
    return "other"
```

**Step 1.4 — Implement `es_snczi_03_gold_catalog.py`**

Follow the `bcc_03_gold_catalog.py` pattern:
- Full overwrite of `es_snczi_flood_study_catalog`
- Source-scoped refresh of `au_flood_report_index` with `_source = 'ES_SNCZI'`

**Step 1.5 — Implement `es_snczi_04_download.py`**

Follow `bcc_04_download.py` pattern but with larger timeouts:
- Read timeout: 1800 seconds (30 minutes) for 732 MB CNIG tiles
- `max_workers`: 2 (parallel downloads — reduce to avoid server throttling)
- `batch_size`: 5 (CNIG files are very large)
- Chunk size: 4 MB (up from 1 MB for large files)

CNIG download requires:
1. Visit `detalleArchivo?sec={id}` page
2. Parse the actual download link from the response
3. Download with streaming

MITECO SHP downloads are simpler direct links.

**Step 1.6 — Implement `es_snczi_05_ingest.py`**

**For rasters (CNIG GeoTIFFs):** Reuse the existing Sedona pattern from `bcc_05_raster_ingest.py`:
```python
# Identical Sedona metadata extraction
RS_SRID(raster)           → crs_epsg  (expect 25829/25830/25831)
RS_Width(raster)          → width_px
RS_Height(raster)         → height_px
RS_Envelope(raster)       → native_envelope
ST_Transform(env, native, 'EPSG:4326') → bbox_wkt
RS_SummaryStatsAll(raster, 1, true)    → band statistics
```

**For vectors (SHP flood zones) — NEW capability:**
The Australia pipeline does not ingest vector data into the raster catalog. For Spain, add a vector ingest path using Sedona's vector capabilities:

```python
# Load SHP via Sedona
df = sedona.read.format("shapefile").load(shp_path)
# Or via spark-shp reader, or via fiona on driver for smaller files

# Extract bounding box
df = df.withColumn("bbox_wkt", ST_AsText(ST_Envelope(ST_GeomFromWKB(col("geometry")))))
```

Alternatively, ingest SHP vector data into a dedicated gold table `es_snczi_flood_zones_gold` (vector-specific) rather than forcing it into the raster catalog schema.

**Step 1.7 — Create job definition**

Create `jobs/es_snczi_pipeline_job.json` following the pattern from `jobs/bcc_pipeline_job.json`.

**Step 1.8 — Create UC Volume and update docs**

```sql
CREATE VOLUME IF NOT EXISTS ceg_delta_bronze_prnd.international_flood.es_snczi_raw;
```

Update `docs/data_dictionary.md`, `docs/architecture.md`, `README.md`.

### Phase 2: CNIG Raster Flood Hazard Maps

This is the most storage-intensive phase.

**Step 2.1 — Extend `es_snczi_01_bronze_scrape.py`** to scrape the full CNIG tile catalog
**Step 2.2 — Implement CNIG download handler** in `es_snczi_04_download.py` with session management
**Step 2.3 — Raster ingest** via existing Sedona pattern — already handled in nb05

### Phase 3: AEMET Precipitation Context (Optional)

**Step 3.1 — Create `notebooks/es_aemet/` pipeline (3 notebooks, no download/raster ingest needed)
**Step 3.2 — Store AEMET API key in Databricks Secrets
**Step 3.3 — Scrape weather station network and extreme precipitation alerts

### Phase 4: INSPIRE WFS Vector Extraction (Optional)

Use the MITECO WFS services to extract vector flood zone data via OGC GetFeature requests. This gives more flexibility than the static SHP downloads (area-specific queries, CRS transformation on the server side).

---

## 9. Gotchas & Risks

### 9.1 CNIG Download Mechanism — Session/Cookie Required

The CNIG Centro de Descargas may require:
- Accepting terms of use via a browser session
- CSRF tokens in download forms
- CAPTCHA (unlikely for bulk downloads, but possible)

**Mitigation:** Test the download flow manually first. If cookie-based auth is needed, implement a session-based downloader similar to the WA DWER `SlipClient` in `src/au_flood/wa_dwer/slip_client.py` — that pattern handles PingFederate auth and could be adapted for CNIG's session management.

### 9.2 Massive File Sizes (732 MB per GeoTIFF tile)

CNIG flood hazard GeoTIFFs are ~732 MB each at 1m resolution. With multiple tiles per Demarcación and multiple return periods, total storage could reach **hundreds of GB to TB**.

**Mitigations:**
- Start with a single Demarcación (e.g., Júcar — Valencia region, highest recent flood risk) as a proof of concept
- Increase `batch_size` for raster ingest to 1-5 (from 25)
- Use `Standard_DS5_v2` or larger cluster nodes (32 GB RAM)
- Consider storing as COG with internal tiling for efficient partial reads
- Evaluate whether Sedona can handle these file sizes in-memory (may need to use GDAL/rasterio on the driver for metadata extraction as a fallback)

### 9.3 Spanish-Language APIs and Metadata

All MITECO, CNIG, and SNCZI interfaces are in Spanish. Field names, descriptions, and error messages will be in Spanish.

**Mitigations:**
- Create a translation lookup for key terms in `es_snczi_02_silver_normalize.py`:
  ```python
  FIELD_TRANSLATIONS = {
      "periodo_retorno": "return_period",
      "peligrosidad": "hazard",
      "zona_inundable": "flood_zone",
      "demarcacion_hidrografica": "river_basin_district",
      "cuenca": "catchment",
      "calado": "depth",
      "velocidad": "velocity",
      "lámina": "water_surface",
  }
  ```
- Store both Spanish original and English-translated values in silver

### 9.4 Fragmented SAIH Data (If Phase 2+ is pursued)

Each CH runs its own SAIH system with different technology stacks:
- Some expose JSON APIs, others only web viewers
- No standard schema across CHs
- Some require registration for data access

**Mitigation:** Defer SAIH to a future phase. Focus on SNCZI (static flood maps) first — this delivers the most value for FM Global's flood risk assessment use case with the least integration complexity.

### 9.5 SNCZI Update Cadence is Slow

Unlike Australian portals (which update regularly as new flood studies are published), SNCZI data is updated when the national flood risk assessment is revised (every 6 years per the EU Floods Directive 2007/60/CE cycle, with interim updates). The current cycle data was largely produced in 2022.

**Mitigation:** Schedule the Spain pipeline monthly (not weekly). Implement change detection via HTTP HEAD requests or Last-Modified headers to avoid re-downloading unchanged files.

### 9.6 Network Egress to Spanish Domains

The Databricks cluster subnet must allow outbound HTTPS to:
- `www.mapama.gob.es` (MITECO downloads)
- `wms.mapama.gob.es` (WMS services)
- `centrodedescargas.cnig.es` (CNIG downloads)
- `sig.miteco.gob.es` (SNCZI viewer / API)
- `opendata.aemet.es` (AEMET API, if Phase 3)
- `www.miteco.gob.es` (MITECO portal)

**Mitigation:** Request firewall allowlisting before development begins. All are standard HTTPS (port 443).

### 9.7 Shapefile Limitations

SNCZI vector data is distributed as Shapefiles, which have known limitations:
- 2 GB file size limit
- 10-character field name limit
- No native support for UTF-8 (Spanish characters like ñ, á may cause encoding issues)
- No multipart/complex geometry support issues

**Mitigation:** 
- Read SHP using `fiona` or GeoPandas with explicit encoding (`encoding='utf-8'` or `'latin-1'`)
- Convert to GeoParquet or Delta Lake with WKB geometry for downstream storage
- Test character encoding early — Spanish place names will contain accented characters

### 9.8 ETRS89 vs WGS84 Nuance

While ETRS89 and WGS84 are practically identical for most applications (<1m difference), they are technically different datums. ETRS89 is fixed to the European tectonic plate while WGS84 is ITRF-aligned and evolves with plate motion.

**For FM Global's use case** (flood risk assessment at property level), the difference is negligible. But:
- Store the native CRS in `crs_epsg` column as-is (e.g., 25830 for ETRS89 UTM30N)
- Use `ST_Transform(geom, 'EPSG:25830', 'EPSG:4326')` for the WGS84 bbox — Sedona/GeoTools handles this correctly
- Document that EPSG:4258 → EPSG:4326 is treated as a datum identity for practical purposes

### 9.9 Canary Islands — Separate UTM Zone

Canary Islands use UTM Zone 28N (EPSG:32628 for WGS84 or EPSG:25828 for ETRS89), which is separate from the peninsular zones 29-31. The SNCZI provides separate Canarias downloads for some return periods.

**Mitigation:** Handle Canarias as a distinct `region` value in the silver schema. The CRS handling via `RS_SRID` + `ST_Transform` already supports this — no special code needed, just awareness.

---

## Appendix: Recommended Implementation Priority

| Priority | Work Item | Value | Effort | Dependencies |
|----------|-----------|-------|--------|--------------|
| **P0** | SNCZI SHP vector flood zones (T=10,50,100,500) | **High** — national coverage, direct FM Global use | **Low** — known stable URLs, simple download | None |
| **P1** | CNIG GeoTIFF flood hazard rasters (1 Demarcación POC) | **High** — raster data for depth analysis | **Medium** — CNIG download flow, large files | P0 infra |
| **P2** | CNIG GeoTIFF — all Demarcaciones | **High** — full national coverage | **High** — storage, time | P1 validated |
| **P3** | INSPIRE WMS/WFS integration | **Medium** — dynamic querying capability | **Medium** — OGC client code | P0 |
| **P4** | AEMET precipitation context | **Low** — supplementary to flood mapping | **Medium** — API key, rate limits | None |
| **P5** | SAIH real-time hydrological data | **Low** — operational use, not static risk | **Very High** — 9+ fragmented systems | None |
| **P6** | Copernicus EMS event activations | **Medium** — recent event validation | **Medium** — EMSR scraping | None |

---

*End of plan. Ready for Kyle's review and refinement.*
