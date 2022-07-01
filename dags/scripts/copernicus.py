# ===================================================== #
#    List of Valid Variables: 
#
#    'variable': [
#                '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
#                '2m_temperature', 'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans',
#                'evaporation_from_the_top_of_canopy', 'evaporation_from_vegetation_transpiration', 'forecast_albedo',
#                'lake_bottom_temperature', 'lake_ice_depth', 'lake_ice_temperature',
#                'lake_mix_layer_depth', 'lake_mix_layer_temperature', 'lake_shape_factor',
#                'lake_total_layer_temperature', 'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation',
#                'potential_evaporation', 'runoff', 'skin_reservoir_content',
#                'skin_temperature', 'snow_albedo', 'snow_cover',
#                'snow_density', 'snow_depth', 'snow_depth_water_equivalent',
#                'snow_evaporation', 'snowfall', 'snowmelt',
#                'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
#                'soil_temperature_level_4', 'sub_surface_runoff', 'surface_latent_heat_flux',
#                'surface_net_solar_radiation', 'surface_net_thermal_radiation', 'surface_pressure',
#                'surface_runoff', 'surface_sensible_heat_flux', 'surface_solar_radiation_downwards',
#                'surface_thermal_radiation_downwards', 'temperature_of_snow_layer', 'total_evaporation',
#                'total_precipitation', 'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2',
#                'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4',
#            ]
# ===================================================== #


from scripts.config import (EXTRACT_GRIB_DIR, STAGED_GRIB_DIR, 
        TRANSFORMED_GRIB_DIR, REQUESTS_DIR,
        GET_SCHEDULE, MONTH_DELAY, COMPLETED_REQUESTS_DIR)

# ----------------------------------------------------- #
# * Export Functions
# ----------------------------------------------------- #

__all__=["create_request"]

# ----------------------------------------------------- #
# * Import packages
# ----------------------------------------------------- #
from codecs import ignore_errors
from datetime import datetime, timedelta

from sqlalchemy import column
import urllib3
import re

from urllib.request import urlopen
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


from scripts.services import get_str_date, generate_path, mkdir, submit_request
from scripts.services import load_to_gcs, ls_bucket

import os, time, logging
import pandas as pd

from shutil import copyfile, copytree, rmtree, move
from glob import glob
from dateutil.relativedelta import relativedelta

# ----------------------------------------------------- #
# * Create Directories
# ----------------------------------------------------- #
def create_dirs():
    for i in [EXTRACT_GRIB_DIR, STAGED_GRIB_DIR,
        TRANSFORMED_GRIB_DIR, COMPLETED_REQUESTS_DIR, REQUESTS_DIR]:
        try: 
            mkdir(i)
            logging.info('Successfully created directory: %s' % i)
        except Exception as e:
            raise Exception(e)

# ----------------------------------------------------- #
# * Delete Directories
# ----------------------------------------------------- #
def delete_dirs():
    for i in [EXTRACT_GRIB_DIR, STAGED_GRIB_DIR, TRANSFORMED_GRIB_DIR]:
        try: 
            rmtree(i)
            logging.info('Deleted directory: %s' % i)
        except Exception as e:
            raise Exception(e)

# ----------------------------------------------------- #
# * Create Request
# ----------------------------------------------------- #
def create_request(date: datetime, span: timedelta=timedelta(days=30), 
    dataset: str='reanalysis-era5-land', variable: any=None, 
    time: list=None):

    """ Create Data Request from Copernicus API

    Parameters
    ----------

    date: datetime, start date of the request
    span: timedelta, coverage of request via timedelta(days=30)
    dataset: str, dataset name from Copernicus API
    variable: str/list, variable requested from Copernicus API
    time: list, specify list of time in 'HH:mm' string format
        example ['12:00', '1:00']
        - leave empty if none.

    """

    # Perform default variable checks
    if variable is None:
        raise Exception("Please specify argument 'variable'")

    # Provide default time values if None
    if time is None:
        time = [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ]

    # Add delay from current date
    get_date = date - relativedelta(months=MONTH_DELAY)
    # Get coverage date
    coverage = get_coverage(date=get_date, span=span)

    # Create request params
    params = {
        "variable": [variable],
        "year": coverage['year'],
        "month": coverage['month'],
        "day": coverage['day'],
        'time': time,
        'area': [
            21, 115, 3,
            127,
        ],
        'format': 'grib',
    }

    # Submit Request
    request_id = submit_request(dataset=dataset, params=params)
    params["request_id"] = [request_id]

    write_metadata(params=params)
    return params

# ----------------------------------------------------- #
# * Write Metadata
# ----------------------------------------------------- #
def write_metadata(params: dict):
    # Create df with metadata
    df = pd.DataFrame(data = params["request_id"], columns=["request_id"])
    df["variable"] = params["variable"]
    df["params"] = str(params)
    df["date"] = datetime.now()
    df['state'] = 'extract'
    
    # Save dataframe
    try:
        # CREATE REQUEST DATA
        date = get_str_date()
        get_path = generate_path(base_path=EXTRACT_GRIB_DIR, variable=params["variable"], str_date=date, mode=4)
        fn = get_path["fn"]+".csv"

        path = f"{REQUESTS_DIR}/"
        dest = f"{path}/{fn}"

        # write data to request queue folder
        mkdir(path)
        df.to_csv(dest, index=False)
        logging.info("  Created request data in %s" % path)

    except Exception as e:
        logging.error(e)
        raise Exception(e)

# ----------------------------------------------------- #
# * Get Updates
#   Updates are monthly --beta
# ----------------------------------------------------- #
def get_updates():
    get_date = datetime(datetime.now().year, GET_SCHEDULE[0], GET_SCHEDULE[1])
    now = datetime.now()
    
    if now < get_date:
        return now == get_date
    
# ----------------------------------------------------- #
# * Get Coverage Date in Dict
# ----------------------------------------------------- #
def get_coverage(date: datetime, span: timedelta) -> dict:
    """ Calculates Days Covered

    Parameters
    ----------
    date: start date in datetime format
    span: span in days via timedelta(days=n)

    """
    # Get date coverage
    date_list = [date - timedelta(days=x) for x in range(span.days)]
    year = list({ i.year for i in date_list })
    month = list({ i.month for i in date_list })
    day = list({ i.day for i in date_list })
    
    # Convert to string values
    year = [str(i) for i in year]
    month = [str(i) for i in month]
    day = [str(i) for i in day]

    get={"year": year, "month": month, "day": day}
    return get
