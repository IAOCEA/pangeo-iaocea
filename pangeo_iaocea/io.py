import json
import pathlib
import stac_geoparquet
import pystac
import shapely
import geopandas as gpd


def dump_to_ndjson(collection, path: pathlib.Path):
    with open(path, "w") as f:
        for item in collection:
            json.dump(item.to_dict(), f, separators=(",", ":"))
            f.write("\n")


def read_ndjson(path: pathlib.Path):
    """read geometries and IDs from a ndjson file"""
    with open(path) as f:
        items = [pystac.Item.from_dict(json.loads(line)) for line in f]

    geometries = [shapely.from_geojson(json.dumps(item.geometry)) for item in items]
    collection_ids = [item.collection_id for item in items]
    item_ids = [item.id for item in items]
    return gpd.GeoDataFrame(
        {"collection": collection_ids, "id": item_ids},
        geometry=geometries,
        crs="epsg:4326",
    )


def write_to_geoparquet(collection, cache_root: pathlib.Path, path: pathlib.Path):
    cache_path = cache_root.joinpath(path.name).with_suffix(".jsonl")
    dump_to_ndjson(collection, cache_path)
    stac_geoparquet.arrow.parse_stac_ndjson_to_parquet(cache_path, path)
