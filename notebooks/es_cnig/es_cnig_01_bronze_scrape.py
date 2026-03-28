# Databricks notebook source
# Spain CNIG — Bronze Scrape
#
# Purpose : Enumerates flood hazard GeoTIFF tiles from Spain's Centro Nacional
#           de Información Geográfica (CNIG) Centro de Descargas portal and
#           records their metadata in the bronze Delta table.
#
#           The CNIG portal (https://centrodedescargas.cnig.es/CentroDescargas/)
#           provides 1-metre resolution flood hazard rasters (Mapas de
#           peligrosidad por inundación fluvial) organised by Demarcación
#           Hidrográfica (river basin district) and return period (T=10, T=100,
#           T=500 years).  Each GeoTIFF is ~732 MB.
#
#           This notebook implements a two-phase approach:
#             1. Scrape the CNIG product catalog page for the selected
#                Demarcación to discover available tile download entries.
#             2. For each discovered tile, issue a HEAD request (with session
#                cookie management) to resolve the actual download URL and
#                capture Content-Length and Last-Modified for change detection.
#
#           Actual GeoTIFF downloads are intentionally deferred to
#           es_cnig_04_download.py to keep this notebook fast and allow
#           metadata inspection before committing to large file transfers.
#
# Layer   : Bronze  (ceg_delta_bronze_prnd.international_flood.bronze_es_cnig_datasets)
# Outputs : ceg_delta_bronze_prnd.international_flood.bronze_es_cnig_datasets
#           ceg_delta_bronze_prnd.international_flood.pipeline_run_log
#
# Authentication : CNIG download portal uses a session cookie issued after an
#                  implicit acceptance of terms.  This notebook manages the
#                  session via requests.Session() — no user credentials needed.
#
# Schedule : Monthly via Databricks Workflow. Idempotent — safe to re-run.
# Version  : 1.0.0

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
    "demarcacion", "Ebro",
    [
        "Ebro", "Tajo", "Guadalquivir", "Segura", "Jucar",
        "Duero", "Mino-Sil", "Cantabrico-Occidental", "Cantabrico-Oriental",
        "Guadalete-Barbate", "Tinto-Odiel-Piedras", "Guadiana",
        "Cuencas-Internas-Cataluna", "Baleares", "Canarias",
    ],
    "Demarcación Hidrográfica — river basin to enumerate"
)
dbutils.widgets.dropdown(
    "skip_unchanged", "true", ["true", "false"],
    "Skip Unchanged — if true, skip tiles whose Last-Modified matches the last recorded scrape"
)
dbutils.widgets.dropdown(
    "discover_only", "false", ["true", "false"],
    "Discover Only — if true, scrape catalog only; do not resolve download URLs (faster but less metadata)"
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
from urllib.parse import urljoin, urlparse, urlencode, parse_qs

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType,
    StructField, StructType, TimestampType,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Catalog / table / volume paths ────────────────────────────────────────
BRONZE_CATALOG  = "ceg_delta_bronze_prnd"
BRONZE_SCHEMA   = "international_flood"
BRONZE_TABLE    = "bronze_es_cnig_datasets"
AUDIT_TABLE     = "pipeline_run_log"

FQN_BRONZE  = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
FQN_AUDIT   = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{AUDIT_TABLE}"

# ── Source constants ───────────────────────────────────────────────────────
SOURCE_NAME      = "ES_CNIG"
COUNTRY          = "ES"
NOTEBOOK_NAME    = "es_cnig_01_bronze_scrape"
NOTEBOOK_VERSION = "1.0.0"

# ── CNIG portal URLs ───────────────────────────────────────────────────────
CNIG_PORTAL_BASE  = "https://centrodedescargas.cnig.es/CentroDescargas"
CNIG_CATALOG_URL  = f"{CNIG_PORTAL_BASE}/mapas-peligrosidad-inundacion-fluvial"
CNIG_CATALOG_COASTAL = f"{CNIG_PORTAL_BASE}/mapas-peligrosidad-inundacion-costera"
CNIG_DETAIL_URL   = f"{CNIG_PORTAL_BASE}/detalleArchivo"

# ── Known CNIG product codes and return periods ────────────────────────────
# CNIG flood hazard tile codes follow the pattern:
#   ESNZSNCZI + product_type + T{period} + tile_ref
# product types: MPFT = Mapa Peligrosidad Fluvial (hazard), MDT = Digital Terrain
RETURN_PERIODS = [10, 100, 500]

# ── Demarcación Hidrográfica slug → display metadata ──────────────────────
DEMARCACION_META = {
    "Ebro":                    {"code": "DH-EB", "name_es": "Ebro",                        "crs_epsg_likely": 25830},
    "Tajo":                    {"code": "DH-TJ", "name_es": "Tajo",                        "crs_epsg_likely": 25830},
    "Guadalquivir":            {"code": "DH-GU", "name_es": "Guadalquivir",                "crs_epsg_likely": 25830},
    "Segura":                  {"code": "DH-SE", "name_es": "Segura",                      "crs_epsg_likely": 25830},
    "Jucar":                   {"code": "DH-JU", "name_es": "Júcar",                       "crs_epsg_likely": 25830},
    "Duero":                   {"code": "DH-DU", "name_es": "Duero",                       "crs_epsg_likely": 25830},
    "Mino-Sil":                {"code": "DH-MS", "name_es": "Miño-Sil",                   "crs_epsg_likely": 25829},
    "Cantabrico-Occidental":   {"code": "DH-CO", "name_es": "Cantábrico Occidental",       "crs_epsg_likely": 25830},
    "Cantabrico-Oriental":     {"code": "DH-CR", "name_es": "Cantábrico Oriental",         "crs_epsg_likely": 25830},
    "Guadalete-Barbate":       {"code": "DH-GB", "name_es": "Guadalete-Barbate",           "crs_epsg_likely": 25829},
    "Tinto-Odiel-Piedras":     {"code": "DH-TO", "name_es": "Tinto, Odiel y Piedras",      "crs_epsg_likely": 25829},
    "Guadiana":                {"code": "DH-GD", "name_es": "Guadiana",                    "crs_epsg_likely": 25829},
    "Cuencas-Internas-Cataluna":{"code": "DH-CI", "name_es": "Cuencas Internas de Cataluña","crs_epsg_likely": 25831},
    "Baleares":                {"code": "DH-BA", "name_es": "Baleares",                    "crs_epsg_likely": 25831},
    "Canarias":                {"code": "DH-CA", "name_es": "Canarias",                    "crs_epsg_likely": 32628},
}

# ── HTTP tuning ────────────────────────────────────────────────────────────
REQUEST_TIMEOUT    = 60    # seconds — portal can be slow
MAX_RETRIES        = 3
RETRY_BACKOFF_BASE = 2.0
RETRY_JITTER       = 0.5
CONNECT_TIMEOUT    = 15
READ_TIMEOUT       = 60

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

RUN_ID          = dbutils.widgets.get("run_id").strip()          or str(uuid.uuid4())
SCRAPE_VERSION  = dbutils.widgets.get("scrape_version").strip()  or NOTEBOOK_VERSION
DEMARCACION     = dbutils.widgets.get("demarcacion").strip()      or "Ebro"
SKIP_UNCHANGED  = dbutils.widgets.get("skip_unchanged").lower()  == "true"
DISCOVER_ONLY   = dbutils.widgets.get("discover_only").lower()   == "true"
SCRAPED_AT      = datetime.now(timezone.utc)

DEM_META = DEMARCACION_META.get(DEMARCACION, {"code": "DH-XX", "name_es": DEMARCACION, "crs_epsg_likely": 25830})

log.info("=" * 60)
log.info(f"CNIG Bronze Scrape  |  Run ID: {RUN_ID}")
log.info(f"Demarcación      : {DEMARCACION} ({DEM_META['code']})")
log.info(f"Scrape version   : {SCRAPE_VERSION}")
log.info(f"Skip unchanged   : {SKIP_UNCHANGED}")
log.info(f"Discover only    : {DISCOVER_ONLY}")
log.info(f"Started at       : {SCRAPED_AT.isoformat()}")
log.info("=" * 60)

# COMMAND ----------

# MAGIC %md ## 4. Schema Definitions

# COMMAND ----------

BRONZE_STRUCT = StructType([
    # Tile identity
    StructField("tile_id",                StringType(),    False),
    StructField("tile_code",              StringType(),    True),
    StructField("tile_name",              StringType(),    True),
    StructField("download_url",           StringType(),    True),
    StructField("detail_page_url",        StringType(),    True),
    # Classification
    StructField("demarcacion_code",       StringType(),    True),
    StructField("demarcacion_name",       StringType(),    True),
    StructField("demarcacion_name_es",    StringType(),    True),
    StructField("return_period_years",    LongType(),      True),
    StructField("aep_pct",               DoubleType(),    True),
    StructField("product_type",           StringType(),    True),
    StructField("crs_epsg_likely",        LongType(),      True),
    # Size / change detection
    StructField("file_size_bytes",        LongType(),      True),
    StructField("last_modified",          StringType(),    True),
    StructField("content_type",           StringType(),    True),
    StructField("head_status",            StringType(),    True),
    # Pipeline provenance
    StructField("_run_id",               StringType(),    False),
    StructField("_scraped_at",           TimestampType(), False),
    StructField("_scrape_version",       StringType(),    False),
    StructField("_is_current",           BooleanType(),   False),
    StructField("_source",               StringType(),    False),
    StructField("_country",              StringType(),    False),
])

BRONZE_COLUMNS = [f.name for f in BRONZE_STRUCT.fields]

# COMMAND ----------

# MAGIC %md ## 5. Table Initialisation

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_BRONZE} (
    tile_id               STRING     NOT NULL  COMMENT 'SHA-256 of demarcacion_code + tile_code + return_period, first 32 chars. Stable across re-runs.',
    tile_code             STRING               COMMENT 'CNIG tile/file code as scraped from the portal, e.g. ESNZSNCZIMPFT010E77.',
    tile_name             STRING               COMMENT 'Human-readable tile name from the CNIG portal.',
    download_url          STRING               COMMENT 'Resolved direct download URL for the GeoTIFF. NULL if not yet resolved.',
    detail_page_url       STRING               COMMENT 'URL to the CNIG detalleArchivo page for this tile.',
    demarcacion_code      STRING               COMMENT 'Internal demarcación code, e.g. DH-EB for Ebro.',
    demarcacion_name      STRING               COMMENT 'Demarcación slug used as the widget value, e.g. Ebro.',
    demarcacion_name_es   STRING               COMMENT 'Full Spanish name, e.g. Ebro.',
    return_period_years   LONG                 COMMENT 'Return period in years: 10 | 100 | 500.',
    aep_pct               DOUBLE               COMMENT 'Annual Exceedance Probability %: 10.0 | 1.0 | 0.2.',
    product_type          STRING               COMMENT 'CNIG product type: peligrosidad_fluvial | peligrosidad_costera | mdt.',
    crs_epsg_likely       LONG                 COMMENT 'Likely EPSG code of the raster CRS inferred from demarcación geography.',
    file_size_bytes       LONG                 COMMENT 'File size from Content-Length response header. NULL if not returned.',
    last_modified         STRING               COMMENT 'Last-Modified response header string for change detection.',
    content_type          STRING               COMMENT 'Content-Type response header (should be image/tiff or application/octet-stream).',
    head_status           STRING               COMMENT 'HTTP HEAD result: ok | error | skipped.',
    _run_id               STRING     NOT NULL  COMMENT 'UUID identifying the pipeline run that produced this row.',
    _scraped_at           TIMESTAMP  NOT NULL  COMMENT 'UTC timestamp when this row was scraped.',
    _scrape_version       STRING     NOT NULL  COMMENT 'Semantic version of this notebook at time of run.',
    _is_current           BOOLEAN    NOT NULL  COMMENT 'TRUE for the most recent record for this tile_id. Toggled FALSE by newer runs.',
    _source               STRING     NOT NULL  COMMENT 'Source system — always ES_CNIG.',
    _country              STRING     NOT NULL  COMMENT 'Country code — always ES.'
)
USING DELTA
COMMENT 'Bronze layer: raw catalog scrape of CNIG flood hazard GeoTIFF tiles per Demarcación Hidrográfica. One row per tile per scrape run. Append-only with _is_current toggling.'
TBLPROPERTIES (
    ''delta.enableChangeDataFeed''        = ''true'',
    ''delta.autoOptimize.autoCompact''    = ''true'',
    ''delta.autoOptimize.optimizeWrite''  = ''true'',
    ''pipelines.autoOptimize.zOrderCols'' = ''demarcacion_code,return_period_years,_is_current''
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN_AUDIT} (
    run_id              STRING     NOT NULL,
    pipeline_stage      STRING     NOT NULL,
    notebook_name       STRING,
    notebook_version    STRING,
    start_time          TIMESTAMP  NOT NULL,
    end_time            TIMESTAMP,
    duration_seconds    DOUBLE,
    status              STRING,
    rows_read           LONG,
    rows_written        LONG,
    rows_merged         LONG,
    rows_rejected       LONG,
    error_message       STRING,
    extra_metadata      STRING,
    databricks_job_id   STRING,
    databricks_run_id   STRING,
    scrape_version      STRING
)
USING DELTA
COMMENT 'Pipeline audit log. One row per notebook stage per Workflow run.'
TBLPROPERTIES (''delta.enableChangeDataFeed'' = ''true'')
""")

# COMMAND ----------

# MAGIC %md ## 6. HTTP Session Utilities
# MAGIC
# MAGIC The CNIG Centro de Descargas portal uses a session cookie that is issued
# MAGIC when the portal home page is first visited.  We use a persistent
# MAGIC `requests.Session` to carry these cookies across all requests.

# COMMAND ----------

def make_cnig_session() -> requests.Session:
    """
    Creates an HTTP session pre-warmed with the CNIG portal's session cookies.

    The CNIG portal issues a JSESSIONID (and possibly a CSRF-equivalent token)
    when the catalog page is first loaded.  By visiting the landing page first,
    we ensure subsequent download requests carry a valid session.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; MapGuru-FloodPipeline/1.0; "
            "+https://mapguru.ai/robots.txt)"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    })
    # Warm up session — visit the portal base URL to receive cookies
    try:
        warm_resp = session.get(
            CNIG_PORTAL_BASE,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            verify=False,
            allow_redirects=True,
        )
        log.info(
            "CNIG session warm-up: status=%d cookies=%s",
            warm_resp.status_code,
            dict(session.cookies),
        )
    except Exception as exc:
        log.warning("CNIG session warm-up failed (will continue): %s", exc)
    return session


def http_get_with_retry(
    session: requests.Session,
    url: str,
    params: dict = None,
    method: str = "GET",
) -> requests.Response:
    """
    Performs a GET or HEAD request with exponential backoff retry.
    Returns the Response or raises after exhausting retries.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method.upper() == "HEAD":
                resp = session.head(
                    url, params=params,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    verify=False, allow_redirects=True,
                )
            else:
                resp = session.get(
                    url, params=params,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    verify=False, allow_redirects=True,
                )
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                delay = (RETRY_BACKOFF_BASE ** attempt) + random.uniform(0, RETRY_JITTER)
                log.warning("HTTP %s attempt %d/%d failed [%s]: %s — retry in %.1fs",
                            method, attempt, MAX_RETRIES, url, exc, delay)
                time.sleep(delay)
    raise requests.RequestException(
        f"All {MAX_RETRIES} {method} attempts failed for {url}: {last_exc}"
    ) from last_exc

# COMMAND ----------

# MAGIC %md ## 7. CNIG Catalog Scraper
# MAGIC
# MAGIC The CNIG Centro de Descargas presents flood hazard tiles in an HTML table
# MAGIC at the product catalog page.  This function parses that page to extract
# MAGIC tile codes, names, and the `sec` parameter needed to build detail page URLs.

# COMMAND ----------

def scrape_cnig_catalog(session: requests.Session, demarcacion: str) -> list:
    """
    Scrapes the CNIG flood hazard catalog page for a given Demarcación.

    The portal at /mapas-peligrosidad-inundacion-fluvial returns an HTML page
    with a table or list of available tiles.  Each tile has a name, a CNIG code,
    and a link to its detail page (which contains the actual download URL).

    Returns a list of dicts with keys:
      tile_code, tile_name, detail_page_url, return_period_years, product_type
    """
    dem_name_es = DEMARCACION_META.get(demarcacion, {}).get("name_es", demarcacion)
    log.info("Scraping CNIG catalog for demarcación: %s (%s)", demarcacion, dem_name_es)

    try:
        resp = http_get_with_retry(session, CNIG_CATALOG_URL)
    except Exception as exc:
        log.error("Failed to fetch CNIG catalog page: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    tiles = []

    # Strategy 1: Look for links containing the demarcación name or CNIG tile codes
    # The CNIG portal structures results as anchor tags with download link patterns
    all_links = soup.find_all("a", href=True)
    log.info("Found %d links on catalog page — scanning for CNIG tile entries", len(all_links))

    seen_codes = set()
    for link in all_links:
        href = link.get("href", "")
        text = link.get_text(strip=True)

        # Look for detail page links (detalleArchivo?sec=NNN pattern)
        if "detalleArchivo" in href or "sec=" in href:
            # Extract sec parameter
            sec_match = re.search(r"sec=(\d+)", href)
            if not sec_match:
                continue
            sec_id = sec_match.group(1)
            if sec_id in seen_codes:
                continue
            seen_codes.add(sec_id)

            detail_url = href if href.startswith("http") else urljoin(CNIG_PORTAL_BASE + "/", href)

            # Infer return period from tile name/code
            rp_years = _infer_return_period(text)
            product_type = _infer_product_type(text, href)

            # Filter by demarcación name presence in tile text (broad match)
            dem_match = (
                dem_name_es.lower() in text.lower()
                or demarcacion.lower() in text.lower()
                or dem_name_es.split("-")[0].lower() in text.lower()
            )

            tiles.append({
                "tile_code":          sec_id,
                "tile_name":          text[:500] if text else f"CNIG-{sec_id}",
                "detail_page_url":    detail_url,
                "return_period_years": rp_years,
                "product_type":       product_type,
                "dem_match":          dem_match,
            })

    # If the catalog page is JavaScript-rendered or returns no links, fall back
    # to constructing known tile entries from the CNIG naming convention
    if not tiles:
        log.warning(
            "No tile links found on catalog page — falling back to "
            "known CNIG tile construction for demarcación %s", demarcacion
        )
        tiles = _build_known_tiles(demarcacion)
    else:
        # Filter to demarcación-relevant tiles if we have dem_match info
        dem_tiles = [t for t in tiles if t.get("dem_match")]
        if dem_tiles:
            log.info("Filtered %d / %d tiles matching demarcación %s", len(dem_tiles), len(tiles), demarcacion)
            tiles = dem_tiles
        else:
            log.info("No demarcación filter match — returning all %d tiles", len(tiles))

    log.info("CNIG catalog scrape complete: %d tile(s) found for %s", len(tiles), demarcacion)
    return tiles


def _infer_return_period(text: str) -> Optional[int]:
    """Infers flood return period in years from a tile name string."""
    text_upper = text.upper()
    for rp, patterns in [
        (10,  ["T010", "T10", "10A", "10 AÑ", "ALTA"]),
        (100, ["T100", "100A", "MEDIA"]),
        (500, ["T500", "500A", "BAJA"]),
    ]:
        if any(p in text_upper for p in patterns):
            return rp
    return None


def _infer_product_type(text: str, href: str) -> str:
    """Infers CNIG product type from tile name / URL."""
    combined = (text + " " + href).lower()
    if "costera" in combined or "coastal" in combined:
        return "peligrosidad_costera"
    if "mdt" in combined or "modelo digital" in combined:
        return "mdt"
    return "peligrosidad_fluvial"


def _build_known_tiles(demarcacion: str) -> list:
    """
    Fallback: constructs synthetic tile entries for the known CNIG product
    structure when the catalog page cannot be scraped (e.g. JavaScript-rendered).

    Returns entries for T=10, T=100, T=500 for the given demarcación.
    These are placeholders — download_url will be resolved in nb04.
    """
    dem_code = DEMARCACION_META.get(demarcacion, {}).get("code", "DH-XX")
    tiles = []
    for rp in RETURN_PERIODS:
        rp_str = f"{rp:03d}"
        tile_code = f"CNIG_{dem_code.replace('-', '')}_{rp_str}"
        tiles.append({
            "tile_code":           tile_code,
            "tile_name":           f"Mapa peligrosidad fluvial T={rp} — {demarcacion}",
            "detail_page_url":     None,
            "return_period_years": rp,
            "product_type":        "peligrosidad_fluvial",
            "dem_match":           True,
        })
    return tiles

# COMMAND ----------

# MAGIC %md ## 8. Download URL Resolution
# MAGIC
# MAGIC The CNIG portal uses an intermediate detail page that renders the actual
# MAGIC download link.  This function fetches that page and extracts the URL.

# COMMAND ----------

def resolve_download_url(session: requests.Session, detail_page_url: str) -> Optional[str]:
    """
    Fetches the CNIG detail page and extracts the direct file download URL.

    The detail page at /detalleArchivo?sec=NNN contains a download button or
    link whose href is the actual GeoTIFF download URL.

    Returns the resolved URL or None if extraction fails.
    """
    if not detail_page_url:
        return None
    try:
        resp = http_get_with_retry(session, detail_page_url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Strategy 1: look for a link/button with 'descarga' or 'download' text
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            if any(kw in text for kw in ("descarga", "download", "descargar")):
                if href.startswith("http"):
                    return href
                return urljoin(CNIG_PORTAL_BASE + "/", href)

        # Strategy 2: look for a form action
        form = soup.find("form", attrs={"method": True})
        if form and form.get("action"):
            action = form.get("action")
            return action if action.startswith("http") else urljoin(CNIG_PORTAL_BASE + "/", action)

        # Strategy 3: look for any link pointing to a .tif or .zip file
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if any(href.lower().endswith(ext) for ext in (".tif", ".tiff", ".zip")):
                return href if href.startswith("http") else urljoin(CNIG_PORTAL_BASE + "/", href)

        log.warning("Could not extract download URL from detail page: %s", detail_page_url)
        return None

    except Exception as exc:
        log.error("resolve_download_url failed for %s: %s", detail_page_url, exc)
        return None


def head_check(session: requests.Session, url: str) -> dict:
    """
    Issues a HEAD request to resolve file size, last-modified, and content type.
    Returns a dict with keys: file_size_bytes, last_modified, content_type, head_status.
    """
    if not url:
        return {"file_size_bytes": None, "last_modified": None, "content_type": None, "head_status": "skipped"}
    try:
        resp = http_get_with_retry(session, url, method="HEAD")
        return {
            "file_size_bytes": int(resp.headers.get("Content-Length", 0)) or None,
            "last_modified":   resp.headers.get("Last-Modified"),
            "content_type":    resp.headers.get("Content-Type"),
            "head_status":     "ok",
        }
    except Exception as exc:
        log.warning("HEAD check failed for %s: %s", url, exc)
        return {"file_size_bytes": None, "last_modified": None, "content_type": None, "head_status": "error"}

# COMMAND ----------

# MAGIC %md ## 9. Stable Tile ID

# COMMAND ----------

def stable_tile_id(demarcacion_code: str, tile_code: str, return_period_years: Optional[int]) -> str:
    """
    Computes a deterministic 32-char hex tile ID by SHA-256 hashing the
    compound key 'es_cnig::{demarcacion_code}::{tile_code}::{return_period_years}'.
    """
    rp_str = str(return_period_years or "unknown")
    key = f"es_cnig::{demarcacion_code or ''}::{tile_code or ''}::{rp_str}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]

# COMMAND ----------

# MAGIC %md ## 10. Load Previous Scrape for Change Detection

# COMMAND ----------

# For skip_unchanged: load previous last_modified values keyed by tile_id
_prev_last_modified: dict = {}
if SKIP_UNCHANGED:
    try:
        prev_df = spark.sql(f"""
            SELECT tile_id, last_modified
            FROM   {FQN_BRONZE}
            WHERE  demarcacion_code = '{DEM_META["code"]}'
              AND  _is_current = TRUE
              AND  last_modified IS NOT NULL
        """)
        for row in prev_df.collect():
            _prev_last_modified[row["tile_id"]] = row["last_modified"]
        log.info("Loaded %d previous last_modified values for skip_unchanged check", len(_prev_last_modified))
    except Exception as exc:
        log.warning("Could not load previous scrape data for change detection: %s", exc)

# COMMAND ----------

# MAGIC %md ## 11. Main Scrape Execution

# COMMAND ----------

AEP_MAP = {10: 10.0, 50: 2.0, 100: 1.0, 500: 0.2}

def run_scrape(session: requests.Session) -> tuple:
    """
    Orchestrates the full scrape for the configured Demarcación.
    Returns (rows: list[dict], counters: dict).
    """
    counters = {"tiles_found": 0, "tiles_skipped": 0, "tiles_resolved": 0, "errors": 0}

    catalog_tiles = scrape_cnig_catalog(session, DEMARCACION)
    if not catalog_tiles:
        log.warning("No tiles discovered — check portal connectivity and demarcación selection")
        return [], counters

    rows = []
    for tile in catalog_tiles:
        tile_code     = tile.get("tile_code", "")
        tile_name     = tile.get("tile_name", "")
        detail_url    = tile.get("detail_page_url")
        rp_years      = tile.get("return_period_years")
        product_type  = tile.get("product_type", "peligrosidad_fluvial")

        tile_id = stable_tile_id(DEM_META["code"], tile_code, rp_years)

        # Skip unchanged check
        if SKIP_UNCHANGED and tile_id in _prev_last_modified:
            log.info("[%s] Tile %s — defer HEAD check (skip_unchanged)", tile_id[:8], tile_code)

        # Resolve download URL from detail page (unless discover_only)
        download_url = None
        head_result = {"file_size_bytes": None, "last_modified": None, "content_type": None, "head_status": "skipped"}
        if not DISCOVER_ONLY and detail_url:
            try:
                download_url = resolve_download_url(session, detail_url)
                if download_url:
                    counters["tiles_resolved"] += 1
                    head_result = head_check(session, download_url)
                    # Skip if unchanged
                    if (
                        SKIP_UNCHANGED
                        and tile_id in _prev_last_modified
                        and head_result.get("last_modified")
                        and head_result["last_modified"] == _prev_last_modified[tile_id]
                    ):
                        log.info("[%s] Tile %s unchanged (Last-Modified matches) — skipping", tile_id[:8], tile_code)
                        counters["tiles_skipped"] += 1
                        continue
            except Exception as exc:
                log.error("[%s] Tile %s — URL resolution error: %s", tile_id[:8], tile_code, exc)
                counters["errors"] += 1

        rows.append({
            "tile_id":              tile_id,
            "tile_code":            tile_code,
            "tile_name":            tile_name,
            "download_url":         download_url,
            "detail_page_url":      detail_url,
            "demarcacion_code":     DEM_META["code"],
            "demarcacion_name":     DEMARCACION,
            "demarcacion_name_es":  DEM_META["name_es"],
            "return_period_years":  rp_years,
            "aep_pct":              AEP_MAP.get(rp_years) if rp_years else None,
            "product_type":         product_type,
            "crs_epsg_likely":      DEM_META["crs_epsg_likely"],
            "file_size_bytes":      head_result.get("file_size_bytes"),
            "last_modified":        head_result.get("last_modified"),
            "content_type":         head_result.get("content_type"),
            "head_status":          head_result.get("head_status"),
        })

    counters["tiles_found"] = len(rows) + counters["tiles_skipped"]
    log.info(
        "Scrape complete: %d tiles found, %d rows prepared, %d skipped, %d errors",
        counters["tiles_found"], len(rows), counters["tiles_skipped"], counters["errors"],
    )
    return rows, counters

# COMMAND ----------

# MAGIC %md ## 12. Bronze Delta Write

# COMMAND ----------

def write_bronze(rows: list, run_id: str, scraped_at, scrape_version: str) -> dict:
    """
    Annotates rows with pipeline provenance, expires superseded records,
    and appends to the bronze Delta table.
    Returns a stats dict.
    """
    if not rows:
        log.warning("write_bronze: no rows to write — skipping")
        return {"rows_written": 0}

    for row in rows:
        row["_run_id"]         = run_id
        row["_scraped_at"]     = scraped_at
        row["_scrape_version"] = scrape_version
        row["_is_current"]     = True
        row["_source"]         = SOURCE_NAME
        row["_country"]        = COUNTRY

    pdf = pd.DataFrame(rows)
    for col in BRONZE_COLUMNS:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[BRONZE_COLUMNS]

    # Coerce integer columns
    for int_col in ("return_period_years", "aep_pct", "file_size_bytes", "crs_epsg_likely"):
        if int_col in pdf.columns:
            pdf[int_col] = pd.to_numeric(pdf[int_col], errors="coerce")

    sdf = spark.createDataFrame(pdf, schema=BRONZE_STRUCT)

    # Expire superseded records for the same tile_ids
    tile_ids = [r["tile_id"] for r in rows if r.get("tile_id")]
    if tile_ids:
        tid_df = spark.createDataFrame([(t,) for t in tile_ids], ["tile_id"])
        tid_df.createOrReplaceTempView("_cnig_current_run_tile_ids")
        spark.sql(f"""
            UPDATE {FQN_BRONZE}
            SET    _is_current = FALSE
            WHERE  tile_id IN (SELECT tile_id FROM _cnig_current_run_tile_ids)
              AND  _is_current  = TRUE
              AND  _run_id     != '{run_id}'
        """)

    sdf.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(FQN_BRONZE)
    log.info("Bronze write complete — %d rows appended to %s", len(rows), FQN_BRONZE)
    return {"rows_written": len(rows)}

# COMMAND ----------

# MAGIC %md ## 13. Audit Logging

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
            "scrape_version":    scrape_version,
        }]
        spark.createDataFrame(record).write.format("delta").mode("append").saveAsTable(FQN_AUDIT)
        log.info("Audit written: stage=%s status=%s", stage, status)
    except Exception as audit_exc:
        log.error("write_audit failed (non-fatal): %s", audit_exc)

# COMMAND ----------

# MAGIC %md ## 14. Execute

# COMMAND ----------

_pipeline_start  = datetime.now(timezone.utc)
_pipeline_status = "failed"
_pipeline_error  = None
_write_stats     = {"rows_written": 0}
_counters        = {}
scrape_version   = SCRAPE_VERSION

try:
    log.info("Starting CNIG scrape | run_id=%s demarcacion=%s", RUN_ID, DEMARCACION)
    cnig_session = make_cnig_session()

    _rows, _counters = run_scrape(cnig_session)
    _write_stats = write_bronze(_rows, RUN_ID, SCRAPED_AT, SCRAPE_VERSION)
    _pipeline_status = "success"

    log.info(
        "Run complete | rows_written=%d tiles_found=%d tiles_skipped=%d errors=%d",
        _write_stats["rows_written"],
        _counters.get("tiles_found", 0),
        _counters.get("tiles_skipped", 0),
        _counters.get("errors", 0),
    )

except Exception as exc:
    _pipeline_error = str(exc)
    log.error("Pipeline failed: %s", exc, exc_info=True)
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
            "demarcacion":       DEMARCACION,
            "demarcacion_code":  DEM_META["code"],
            "tiles_found":       _counters.get("tiles_found", 0),
            "tiles_skipped":     _counters.get("tiles_skipped", 0),
            "tiles_resolved":    _counters.get("tiles_resolved", 0),
            "errors":            _counters.get("errors", 0),
            "discover_only":     DISCOVER_ONLY,
            "skip_unchanged":    SKIP_UNCHANGED,
        },
    )

# COMMAND ----------

# MAGIC %md ## 15. Surface Results

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            tile_code, tile_name, return_period_years, aep_pct,
            product_type, file_size_bytes, last_modified, head_status,
            download_url, _scraped_at
        FROM {FQN_BRONZE}
        WHERE _run_id = '{RUN_ID}'
        ORDER BY return_period_years, tile_code
    """)
)

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
except Exception:
    pass

dbutils.notebook.exit(RUN_ID)
