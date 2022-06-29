import xarray as xr
import rioxarray


# Parse Grib Files
def parse_grib(grib, engine:str='cfgrib'):
    ds = xr.load_dataset(grib)
    ds = ds.rio.write_crs("epsg:4326", inplace=True)
    return ds