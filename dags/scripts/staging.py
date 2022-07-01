# ===================================================== #
# * Contains all staging functions
# ----------------------------------------------------- #
# 
# 
# 
# 
# 
#
# ===================================================== #


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
def moveto(src_filepath:str, src_base:str, dest_base:str, method:str) -> str:
    """
    Move files from one stage to another, then update local request record.

    Parameters
    -----------

    src_filepath: str, source file path
    src_base: str, directory to be replaced
    dest_base: str, directory to be used as replacement to modify the destination directory.

    """
    try:
        # create destination path
        dest_filepath = src_filepath.replace(src_base, dest_base)

        # check if file exists
        if len(glob(src_filepath)) < 1: 
            logging.info(f"[skip] There is nothing to move in: '{src_filepath}'.")
            return dest_filepath
        
        # get filename, path
        fn = dest_filepath.split('/')[-1]
        path = dest_filepath.replace(fn,'')
        
        # get request df filepath, modify state value
        req_fn = f"{REQUESTS_DIR}/{fn.replace('.grib','.csv')}"
        req_df = pd.read_csv(req_fn)
        req_df['state'] = method
        
        # move file
        mkdir(path)
        move(src_filepath, dest_filepath)
        
        # -- disabled to enable date based batch processing in future
        # src_dir = src_filepath.replace(fn,'') 
        # rmtree(src_dir)
        
        # save state
        req_df.to_csv(req_fn, index=False)
        mes=f"[{method}] file {fn} successfully {method}!"
        logging.info(mes)

        return dest_filepath
    except Exception as e:
        raise Exception(e)

# ----------------------------------------------------- #
# * Move to staging directory
# ----------------------------------------------------- #
def moveto_staged(filepath: str):
    # move file
    dest_filepath = moveto(src_filepath=filepath, src_base=EXTRACT_GRIB_DIR, 
        dest_base=STAGED_GRIB_DIR, method='staged')
    # return filepath
    return dest_filepath



