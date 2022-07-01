# ===================================================== #
# * Contains all transformation functions
# ----------------------------------------------------- #
# 
#   Place all functions inside transform_pipeline()
#
# ===================================================== #


import logging
import os 
import pandas as pd
from glob import glob
from scripts.services import parse_grib, mkdir
from .staging import moveto
from .config import STAGED_GRIB_DIR, TRANSFORMED_GRIB_DIR

import re

# ----------------------------------------------------- #
# * Renamed column names
# ----------------------------------------------------- #
RENAME_COLS = {'tp': 'total_precipitation'}

# ----------------------------------------------------- #
# * Check valid requests by id matching
# ----------------------------------------------------- #
def transform_clean(df: pd.DataFrame) -> pd.DataFrame:
    df.dropna(inplace=True)
    df.reset_index(inplace=True)
    df.rename(columns = RENAME_COLS, inplace=True)

    df['longitude'] = df.longitude.astype('float32')
    df['latitude'] = df.latitude.astype('float32')
    return df

# ----------------------------------------------------- #
# * Create row ids
# ----------------------------------------------------- #
def create_ids(df: pd.DataFrame, variable: str=None) -> pd.DataFrame:
    def sub(t):
        t=re.sub('\s|:','-',t)
        return t
    # get climate variable code
    if variable is None:
        variable='tp'
    # create variables
    valid_time=df.valid_time.astype('str').apply(lambda x: sub(x))
    latlong=df.latitude.astype('str') + '-' + df.longitude.astype('str')
    # generate a unique id
    df['id'] = variable+'-'+valid_time+'-'+latlong
    return df

# ----------------------------------------------------- #
# * Define transform pipeline
# ----------------------------------------------------- #
def transform_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = transform_clean(df)
    df = create_ids(df)
    return df

# ----------------------------------------------------- #
# * Move grib to transformed directory
# ----------------------------------------------------- #
def moveto_transformed(filepath: str):
    logging.info(f"[info] Moving to transformed directory: '{filepath}'")

    # move file
    dest_filepath = moveto(src_filepath=filepath, src_base=STAGED_GRIB_DIR, 
        dest_base=TRANSFORMED_GRIB_DIR, method='transformed')

    logging.info('  -- OK')

    # return parquet filepath
    dest_filepath=dest_filepath.replace('.grb','.parquet')
    return dest_filepath

# ----------------------------------------------------- #
# * Main transformation 
# ----------------------------------------------------- #
def main_transform(filepath:str):
    logging.info(f"[transform] Transforming '{filepath}'.. ")

    # read dataframe
    df = parse_grib(filepath).to_dataframe()

    # remove index 
    idx = glob(f"{filepath}.*")
    [os.remove(i) for i in idx]

    # do transformations
    df = transform_pipeline(df)

    # get parquet filepath
    dest_filepath = filepath.replace(STAGED_GRIB_DIR,TRANSFORMED_GRIB_DIR).replace('.grib', '.parquet')
    fn = dest_filepath.split('/')[-1]

    # save parquet file
    mkdir(dest_filepath.replace(fn,''))
    df.to_parquet(dest_filepath, index=False, engine='fastparquet')
    logging.info("[transform] -- OK")
    
    # move grib file to transform folder
    # moveto_transformed(filepath=filepath)
    
    # remove staged file 
    os.remove(filepath)

    # return transformed filepath
    return dest_filepath
