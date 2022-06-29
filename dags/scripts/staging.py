
import logging
from scripts.services import parse_grib
from scripts.services import get_str_date, generate_path
from .config import STAGED_GRIB_DIR, STAGED_META_DIR


# ----------------------------------------------------- #
# * Verify GRIB file
# ----------------------------------------------------- #
def verify_grib(filepath:str):
    """Verify if corrupted GRIB download
    """
    logging.info("  Verifying data..")
    try: 
        df = parse_grib(filepath).to_dataframe().reset_index()
        if df.shape[0] > 1 or df.shape[1] > 1:
            pass
        else:
            logging.critical('  Empty Dataframe! %s ' % filepath)
            return False
    except Exception as e:
        logging.error("  Data Verification failed! %s " % filepath)
        raise Exception(e)

    logging.info("  -- OK")
    return filepath

def moveto_staged(src_filepath: str):
    
    pass

