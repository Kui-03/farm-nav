
from scripts.services import parse_grib, mkdir
from .config import EXTRACT_GRIB_DIR, STAGED_GRIB_DIR, TRANSFORMED_GRIB_DIR, REQUESTS_DIR

import logging
from shutil import move
from glob import glob
import pandas as pd
import os

# ----------------------------------------------------- #
# * Verify GRIB file
# ----------------------------------------------------- #
def verify_grib(filepath:str) -> bool:
    """Verify if corrupted GRIB download
    return: bool, if download is invalid, returns False
    """
    logging.info("  Verifying data..")
    
    if ".grib" not in filepath:
        filepath=filepath+".grib"
    try: 
        df = parse_grib(filepath).to_dataframe().reset_index()
        idx = glob(f"{filepath}.*")
        [os.remove(i) for i in idx]

        if df.shape[0] > 1 or df.shape[1] > 1:
            logging.info("  -- OK")
            return True
        else:
            logging.critical('  Empty Dataframe! %s ' % filepath)
            return None

    except Exception as e:
        logging.error("  Data Verification failed! %s " % filepath)
        return None

# ----------------------------------------------------- #
# * Move to directory
# ----------------------------------------------------- #
def moveto(src_filepath:str, src_base:str, dest_base:str, method:str):

    try:
        # create destination path
        dest_filepath = src_filepath.replace(src_base, dest_base)

        # check if file exists
        if len(glob(src_filepath)) < 1: 
            logging.info(f"[skip] There is nothing to stage in: '{src_filepath}'.")
            return dest_filepath
        
        # get filename, path
        fn = dest_filepath.split('/')[-1]
        path = dest_filepath.replace(fn,'')
        # get request filepath, modify state value
        req_fn = f"{REQUESTS_DIR}/{fn.replace('.grib','.csv')}"
        req_df = pd.read_csv(req_fn)
        req_df['state'] = method
        # create new directory
        mkdir(path)
        move(src_filepath, dest_filepath)
        
        req_df.to_csv(req_fn, index=False)
        mes=f"[{method}] file {fn} successfully {method}!"
        logging.info(mes)
    except Exception as e:
        raise Exception(e)

# ----------------------------------------------------- #
# * Move to staging directory
# ----------------------------------------------------- #
def moveto_staged(filepath: str):
    moveto(src_filepath=filepath, src_base=EXTRACT_GRIB_DIR, 
        dest_base=STAGED_GRIB_DIR, method='staged')

# ----------------------------------------------------- #
# * Move to transformed directory
# ----------------------------------------------------- #
def moveto_transformed(filepath: str):
    moveto(src_filepath=filepath, src_base=STAGED_GRIB_DIR, 
        dest_base=TRANSFORMED_GRIB_DIR, method='transformed')

