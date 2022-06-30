import xarray as xr
import rioxarray


# ----------------------------------------------------- #
# * Parse GRIB files
# ----------------------------------------------------- #
def parse_grib(grib, engine:str='cfgrib'):
    ds = xr.load_dataset(grib, engine=engine)
    ds = ds.rio.write_crs("epsg:4326", inplace=True)
    return ds