import numpy as np
import cf_xarray  # noqa: F401


def polygon_to_bounds(polygon):
    lon, lat = polygon.exterior.xy

    lon_min = np.min(lon)
    lon_max = np.max(lon)
    lat_min = np.min(lat)
    lat_max = np.max(lat)

    return lon_min, lat_min, lon_max, lat_max


def subset_dataset(ds, bbox):
    lon_min, lat_min, lon_max, lat_max = polygon_to_bounds(bbox)

    condition = (
        (ds.cf["longitude"] >= lon_min)
        & (ds.cf["longitude"] <= lon_max)
        & (ds.cf["latitude"] >= lat_min)
        & (ds.cf["latitude"] <= lat_max)
    )

    return ds.where(condition.compute(), drop=True)
