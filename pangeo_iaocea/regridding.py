import xarray as xr
import xdggs  # noqa: F401
import cf_xarray  # noqa: F401
import numpy as np


def categorize_points(grid_info: xdggs.DGGSInfo, longitude, latitude):
    """categorize point coordinates into DGGS bins

    Parameters
    ----------
    grid_info : xdggs.DGGSInfo
        The grid info object
    longitude : xr.DataArray
        The longitude coordinates (must be 1D)
    latitude : xr.DataArray
        The latitude coordinates (must be 1D)

    Returns
    -------
    cell_ids : xr.DataArray
        The DGGS cell ids
    """
    if longitude.dims != latitude.dims:
        raise ValueError(
            f"coordinate dims don't match: {longitude.dims} != {latitude.dims}"
        )

    if longitude.ndim == 0:
        original_dims = 0
        longitude = longitude.expand_dims("points")
        latitude = latitude.expand_dims("points")
    elif len(longitude.dims) != 1:
        raise ValueError(f"coordinates must be 0D or 1D (got dims: {longitude.dims})")
    else:
        original_dims = 1

    result = xr.apply_ufunc(
        grid_info.geographic2cell_ids,
        longitude.astype("float64"),
        latitude.astype("float64"),
        keep_attrs=False,
        dask="parallelized",
    ).assign_attrs(grid_info.to_dict())

    if original_dims == 0:
        return result.squeeze()
    else:
        return result


def aggregation_regridding(grid_info: xdggs.DGGSInfo, ds: xr.Dataset):
    """
    aggregate data points into DGGS bins

    Parameters
    ----------
    grid_info : xdggs.DGGSInfo
        The grid info object
    ds : xr.Dataset
        The dataset to regrid

    Returns
    -------
    regridded : xr.Dataset
        The regridded dataset
    """
    lon_name = ds.cf.coordinates["longitude"][0]
    lat_name = ds.cf.coordinates["latitude"][0]

    stack_dims = list(dict.fromkeys(list(ds[lon_name].dims) + list(ds[lat_name].dims)))

    stacked = (
        ds.stack(points=stack_dims)
        .drop_indexes(["points"] + stack_dims)
        .drop_vars(stack_dims + ["points"])
    )
    cell_ids = categorize_points(grid_info, stacked[lon_name], stacked[lat_name])

    all_cell_ids = np.unique(cell_ids.data).compute()
    cell_grouper = xr.groupers.UniqueGrouper(labels=all_cell_ids)

    return (
        stacked.assign_coords({"cell_ids": cell_ids})
        .groupby(cell_ids=cell_grouper, eagerly_compute_group=False)
        .mean()
        .rename_dims({"cell_ids": "cells"})
        .assign_coords({"cell_ids": ("cells", all_cell_ids)})
        .dggs.decode(grid_info)
    )
