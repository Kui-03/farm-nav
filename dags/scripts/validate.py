# ===================================================== #
# * Contains all validation functions
# ----------------------------------------------------- #
#   
#   Place all functions inside validation_pipeline()
#
# ===================================================== #


import logging
import pandas as pd

# ----------------------------------------------------- #
# * Check for empty df, missing values in columns, 
#   non unique ids
# ----------------------------------------------------- #
def check_nan_unique(df: pd.DataFrame, cols: list) -> bool:
    
    # check if df is empty
    if df.empty:
        logging.error('[validate] DataFrame is empty!')
        raise Exception('[validate] DataFrame is empty!')
    
    nan_cols = df[cols].isna().any()
    if nan_cols.any():
        mes = f"[validate] Missing values found in {nan_cols}"
        logging.error(mes)
        raise Exception(mes)

    # check if id is unique
    if not df['id'].is_unique:
        mes = "[validate] Primary key values [id] are not unique!"
        logging.info(df[['id']])
        logging.error('mes')
        raise Exception(mes)

    return True

# ----------------------------------------------------- #
# * Validation pipeline
# ----------------------------------------------------- #
def validate_pipeline(df:pd.DataFrame, variable:str) -> bool:
    # check for missing, unique values
    cols=['id', variable]
    check_nan_unique(df, cols=cols)

    return True

# ----------------------------------------------------- #
# * Main validation functions goes here
# ----------------------------------------------------- #
def main_validate(filepath: str, request_dict: dict) -> bool:
    logging.info('[validate] Validating.. ')
    
    # read parquet file, retrieve request variable
    df = pd.read_parquet(filepath, engine='fastparquet')
    variable = request_dict['variable']

    # check for missing column names
    if variable not in df.columns.tolist():
        raise Exception(f"Request variable: '{variable}' not in {filepath}")

    
    valid = validate_pipeline(df=df, variable=variable)
    if valid is False:
        raise Exception(f'[validate] Failed to validate file: {filepath}')

    # return ok
    logging.info('[validate] -- OK')
    return True
