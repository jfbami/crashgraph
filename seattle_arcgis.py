"""
Seattle open-data ArcGIS loader.
This wraps the query pattern with paging, retry,
and GeoDataFrame output.

Usage:
    from seattle_arcgis import fetch, SERVICES

    cameras = fetch(SERVICES["traffic_cameras"])
    crashes = fetch(SERVICES["collisions"], where="INCDATE >= DATE '2020-01-01'")
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Iterator

import geopandas as gpd
import pandas as pd
import requests

ORG_ID = "ZOyb2t4B0UYuYNYH"
BASE = f"https://services.arcgis.com/{ORG_ID}/ArcGIS/rest/services"
PAGE_SIZE = 2000

# Service names verified 2026-05-10 against the live ArcGIS REST directory and
# Hub search API. Speed limits live as a SPEEDLIMIT field on Seattle_Streets_1,
# not as their own service. Traffic Flow Counts is one service per year, with
# the year as a *prefix* (e.g. 2023_Traffic_Flow_Counts).
#
# Some services don't expose layer 0 — store (service, layer) tuples.
SERVICES: dict[str, tuple[str, int]] = {
    "traffic_cameras": ("Traffic_Cameras_CDL", 0),
    "collisions": ("SDOT_Collisions_All_Years_1", 0),
    "traffic_signals": ("Traffic_Signal_Assemblies_CDL", 0),
    "bike_facilities": ("SDOT_Bike_Facilities", 3),  # 4116 real BKF segments; layer 2 is empty despite its label
    "streets": ("Seattle_Streets_1", 0),  # carries SPEEDLIMIT, ARTDESCRIPT, ARTCLASS
    "enforcement_cameras": ("Automatic_Traffic_Safety_Cameras_(ATSC)_view", 0),
}

AADT_LAYER = 3  # Traffic Flow Counts services expose their layer at index 3


def aadt_service(year: int) -> str:
    """Service name for a given Traffic Flow Counts year (year is a prefix)."""
    return f"{year}_Traffic_Flow_Counts"


def _service_url(service: str, layer: int = 0) -> str:
    return f"{BASE}/{service}/FeatureServer/{layer}/query"


def count(service: str, where: str = "1=1", layer: int = 0) -> int:
    """Total record count without downloading geometry — cheap to call first."""
    r = requests.get(
        _service_url(service, layer),
        params={"where": where, "returnCountOnly": "true", "f": "json"},
        timeout=30,
    )
    r.raise_for_status()
    return int(r.json()["count"])


def _iter_pages(
    service: str,
    where: str,
    out_fields: str,
    layer: int,
) -> Iterator[dict]:
    offset = 0
    url = _service_url(service, layer)
    while True:
        params = {
            "where": where,
            "outFields": out_fields,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "orderByFields": "OBJECTID",
        }
        for attempt in range(4):
            try:
                r = requests.get(url, params=params, timeout=120)
                r.raise_for_status()
                data = r.json()
                break
            except (requests.RequestException, ValueError) as e:
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt)
        features = data.get("features", [])
        if not features:
            return
        yield data
        if len(features) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


def fetch(
    service: str,
    where: str = "1=1",
    out_fields: str = "*",
    layer: int = 0,
) -> gpd.GeoDataFrame:
    """Pull every record matching `where` from a Seattle ArcGIS FeatureServer."""
    frames: list[gpd.GeoDataFrame] = []
    for page in _iter_pages(service, where, out_fields, layer):
        frames.append(gpd.GeoDataFrame.from_features(page["features"], crs="EPSG:4326"))
    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")


def fetch_aadt(years: range) -> gpd.GeoDataFrame:
    """Annual Traffic Flow Counts is one service per year. Stack them."""
    frames = []
    for year in years:
        try:
            gdf = fetch(aadt_service(year), layer=AADT_LAYER)
        except requests.HTTPError:
            continue
        gdf["year"] = year
        frames.append(gdf)
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")


if __name__ == "__main__":
    out = Path(__file__).parent / "data" / "raw"
    out.mkdir(parents=True, exist_ok=True)

    for key, (service, layer) in SERVICES.items():
        n = count(service, layer=layer)
        print(f"{key}: {n} records")
        gdf = fetch(service, layer=layer)
        gdf.to_file(out / f"{key}.geojson", driver="GeoJSON")
        print(f"  -> {out / f'{key}.geojson'}")

    aadt = fetch_aadt(range(2007, 2025))
    aadt.to_file(out / "aadt.geojson", driver="GeoJSON")
    print(f"aadt: {len(aadt)} records -> {out / 'aadt.geojson'}")
