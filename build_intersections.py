"""Build the canonical intersection point set from Seattle_Streets_1.

Pipeline:
    arterial segments -> endpoints -> spatial clusters -> degree filter -> points

Output: data/processed/intersections.parquet
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sklearn.cluster import DBSCAN

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
CRS_STORE = "EPSG:4326"  # lat/lng for disk + display
CRS_WORK = "EPSG:32610"  # UTM 10N (meters) for distance math


@dataclass
class IntersectionBuilder:
    """Turns raw street segments into a clean intersection point set.

    Each method is one step of the pipeline. Call .run() to execute end-to-end,
    or call methods directly when debugging a single stage.
    """

    raw_dir: Path = RAW
    out_dir: Path = PROCESSED
    exclude_artclass: set[int] = field(default_factory=lambda: {4, 5})  # state route + interstate freeways
    cluster_eps_m: float = 8.0  # meters; ~half a lane width
    min_degree: int = 3  # >=3 segments meeting = real intersection, not a mid-block split
    crs_store: str = CRS_STORE
    crs_work: str = CRS_WORK

    def load_streets(self) -> gpd.GeoDataFrame:
        """Read streets.geojson and drop freeway segments.

        We keep residential streets because side-street T-intersections are
        where a lot of bike/ped risk lives — and SDOT splits residential
        segments at every cross street, so they don't inflate the noise.
        Freeways (ARTCLASS 4, 5) get dropped because intersections on them
        are not where crashes happen at a street-grid level.
        """
        path = self.raw_dir / "streets.geojson"
        if not path.exists():
            sys.exit(f"Missing {path}. Run `python seattle_arcgis.py` first.")

        gdf = gpd.read_file(path)
        print(f"loaded {len(gdf)} street segments")

        col = next((c for c in gdf.columns if c.upper() == "ARTCLASS"), None)
        if col is None:
            sys.exit(f"No ARTCLASS column. Columns: {list(gdf.columns)}")

        art = pd.to_numeric(gdf[col], errors="coerce")
        gdf = gdf.loc[~art.isin(self.exclude_artclass)].copy()
        print(f"  -> {len(gdf)} surface streets (excluded ARTCLASS {sorted(self.exclude_artclass)})")
        return gdf

    def extract_endpoints(self, streets: gpd.GeoDataFrame) -> pd.DataFrame:
        """Explode each segment into its two endpoints, in meters.

        Returns a long table (segment_id, x, y). Reprojecting to UTM 10N here
        is what lets us cluster by real distance later.
        """
        streets = streets.to_crs(self.crs_work).explode(
            index_parts=False, ignore_index=True
        )
        streets = streets[streets.geometry.type == "LineString"]
        streets["segment_id"] = streets.index

        rows: list[tuple[int, float, float]] = []
        for seg_id, geom in zip(streets["segment_id"], streets.geometry):
            coords = list(geom.coords)
            rows.append((seg_id, coords[0][0], coords[0][1]))
            rows.append((seg_id, coords[-1][0], coords[-1][1]))
        endpoints = pd.DataFrame(rows, columns=["segment_id", "x", "y"])
        print(f"  -> {len(endpoints)} endpoints")
        return endpoints

    def cluster_endpoints(self, endpoints: pd.DataFrame) -> pd.DataFrame:
        """Group endpoints that fall within `cluster_eps_m` of each other.

        Endpoints from different segments at the same intersection are almost
        never coincident — they're a few cm to a few meters apart depending on
        how the data was digitized. DBSCAN with eps in meters fuses them.
        """
        coords = endpoints[["x", "y"]].to_numpy()
        labels = DBSCAN(eps=self.cluster_eps_m, min_samples=1).fit(coords).labels_
        endpoints = endpoints.assign(cluster=labels)
        print(f"  -> {endpoints['cluster'].nunique()} clusters")
        return endpoints

    def assemble(self, endpoints: pd.DataFrame) -> gpd.GeoDataFrame:
        """Collapse each surviving cluster into one intersection row.

        Drops clusters whose degree (number of distinct segments meeting there)
        is below the threshold. Then takes the centroid, reprojects back to
        lat/lng, and builds a stable string ID from the rounded coordinates.
        """
        degree = endpoints.groupby("cluster")["segment_id"].nunique().rename("degree")
        keep = degree[degree >= self.min_degree].index
        print(f"  -> {len(keep)} intersections (degree >= {self.min_degree})")

        kept = endpoints[endpoints["cluster"].isin(keep)]
        centroids = kept.groupby("cluster")[["x", "y"]].mean().join(degree)

        gdf = gpd.GeoDataFrame(
            centroids.reset_index(drop=True),
            geometry=gpd.points_from_xy(centroids["x"], centroids["y"]),
            crs=self.crs_work,
        ).to_crs(self.crs_store)

        gdf["lng"] = gdf.geometry.x
        gdf["lat"] = gdf.geometry.y
        # 6 decimal degrees ~= 11 cm; stable across reruns of identical input.
        gdf["intersection_id"] = (
            gdf["lat"].map(lambda v: f"{v:.6f}")
            + "_"
            + gdf["lng"].map(lambda v: f"{v:.6f}")
        )
        return gdf[["intersection_id", "lat", "lng", "degree", "geometry"]]

    def write(self, intersections: gpd.GeoDataFrame) -> None:
        """Persist the result as GeoParquet."""
        self.out_dir.mkdir(parents=True, exist_ok=True)
        out = self.out_dir / "intersections.parquet"
        intersections.to_parquet(out, index=False)
        print(f"wrote {len(intersections)} intersections -> {out}")

    def run(self) -> gpd.GeoDataFrame:
        """Execute the full pipeline and write the output file."""
        streets = self.load_streets()
        endpoints = self.extract_endpoints(streets)
        endpoints = self.cluster_endpoints(endpoints)
        intersections = self.assemble(endpoints)
        self.write(intersections)
        return intersections


if __name__ == "__main__":
    IntersectionBuilder().run()
