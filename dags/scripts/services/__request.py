# ===================================================== #
# * Script: handles all requests
# ----------------------------------------------------- #

__all__ = ["get_user_requests", "get_request_status", "get_request_result",
        "delete_request", "handle_request_status","submit_request", "check_request_api",
        "check_request_local"
        ]


from time import sleep
from glob import glob
import logging

from .__key_client import request_keys
from .__file import generate_path
from .__datetime import get_str_date
from .__copernicus_client import Client_Secure

from ..message import send_message
from ..config import REQUESTS_DIR, EXTRACT_GRIB_DIR

import cdsapi
import pandas as pd

from .__file import mkdir

# ----------------------------------------------------- #
# * Get User Requests
# ----------------------------------------------------- #
def get_user_requests() -> list:
    """Retrieve User Requests
    """
    c = Client_Secure(wait_until_complete=False, delete=False)
    task_url = "%s/tasks/" % (c.url)
    results = c.session.get(task_url, verify=c.verify).json()

    return results

# ----------------------------------------------------- #
# * Get Request Status
# ----------------------------------------------------- #
def get_request_status(request_id: str) -> str:
    """Retrieve Request Status
    request_id: str
    """
    return get_request_result(request_id=request_id).reply["state"]

# ----------------------------------------------------- #
# * Get Request Result
# ----------------------------------------------------- #
def get_request_result(request_id: str) -> dict:
    """Retrieve Request Info
    request_id: str
    """
    c = Client_Secure(wait_until_complete=False, delete=False)
    reply = dict(request_id=request_id)
    result = cdsapi.api.Result(c, reply)
    result.update()

    return result

# ----------------------------------------------------- #
# * Delete Request --debug
# ----------------------------------------------------- #
def delete_request(request_id: str) -> dict:
    """Delete a Request
    request_id: str
    """
    result = get_request_result(request_id=request_id)
    result.delete()
    result.update()

    return result

# ----------------------------------------------------- #
# * Handle Requests --debug
# ----------------------------------------------------- #
def handle_request_status(request_id):
    # get status
    status=get_request_status(request_id)

    # pass if completed
    if "completed" in status:
        mes=f"[completed] Status for request: {request_id} is {status}."
        logging.info(mes)
        return True
    # wait if queued or running
    elif status in ("queued", "running"):
        mes=f"[{status}] Current status for request: {request_id} is {status}."
        logging.info(mes)
        return False
    # decide fail
    elif status in ("failed",):
        mes=f"[FAILED] Status for request: {request_id} has {status}."
        logging.info(mes)
        send_message(mes)
        raise Exception(mes)

# ----------------------------------------------------- #
# * Wait for Request
# ----------------------------------------------------- #
def sleep_update(request_id:str, interval:int=3):
    r = get_request_result(request_id=request_id)

    while True:
        r.update()
        reply = r.reply
        r.info("Request ID: %s, state: %s" % (reply["request_id"], reply["state"]))

        if reply["state"] == "completed":
            break
        elif reply["state"] in ("queued", "running"):
            r.info("Request ID: %s, sleep: %s", reply["request_id"], interval)
            sleep(interval)
        elif reply["state"] in ("failed",):
            r.error("Message: %s", reply["error"].get("message"))
            r.error("Reason:  %s", reply["error"].get("reason"))
            for n in (
                reply.get("error", {}).get("context", {}).get("traceback", "").split("\n")
            ):
                if n.strip() == "":
                    break
                r.error("  %s", n)
            raise Exception(
                "%s. %s." % (reply["error"].get("message"), reply["error"].get("reason"))
            )

    return True

# ----------------------------------------------------- #
# * Submit Request
# ----------------------------------------------------- #
# https://github.com/ecmwf/cdsapi/issues/2
# https://github.com/ecmwf/cdsapi/blob/master/examples/example-era5-update.py
# ----------------------------------------------------- #
def submit_request(dataset: str, params: dict) -> str:
    """ Request function for Copernicus API.
    
    Parameters
    ----------
    dataset: name of dataset
    params: dictionay containing request parameters.
    """

    try:
        # print("  Requesting Data.. This may take a while.")
        c = Client_Secure(wait_until_complete=False, delete=False)
        r = c.retrieve(dataset, params)
        r.update()
        request_id = r.reply['request_id']
        logging.info("  Submitted request with request_id: %s" % request_id)

    except Exception as e:
        raise Exception(e)

    return request_id

# ----------------------------------------------------- #
# * Download Finished Request
# ----------------------------------------------------- #
# https://github.com/ecmwf/cdsapi/issues/25
# https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
# ----------------------------------------------------- #
def download_request(request_id: str, variable:str) -> str:
    """Download a Finished Request
    request_id: str
    """

    # Create client
    c = Client_Secure(wait_until_complete=False, delete=False)
    reply = dict(request_id=request_id)
    result = cdsapi.api.Result(c, reply)
    
    # Get status
    result.update()
    status = result.reply["state"]
    location = result.reply["location"]
    # Verify status
    if status in ("queued", "running"):
        logging.warning(f"  Request with id: %s is currently %s" % (request_id, status))
    elif status == "completed":
        logging.info(f"  File pulling to local..")
    else:
        logging.critical(f"  CRITICAL!: Request with id: %s is currently %s" % (request_id, status))

    # Download Data
    try:
        # Get filename
        ##
        date = get_str_date()
        get_path = generate_path(EXTRACT_GRIB_DIR, str_date=date, variable=variable, mode=4)
        
        # path = "{0}/{1}/{1}_{2}.grib".format(EXTRACT_GRIB_DIR, get_str_date(), variable)
        fn = get_path["fn"]+".grib"
        path = get_path["path"]
        dest_fn = get_path["all"]+".grib"
        
        # Download data to local
        mkdir(path)
        data = c.session.get(location, verify=c.verify).content
        with open(dest_fn,'wb') as f:
            f.write(data)
        logging.info("  Download Success!: File downloaded to %s" % dest_fn)
        
        # Return the path
        return path
        
    except Exception as e:
        logging.error(e)
        raise Exception(e)

# ----------------------------------------------------- #
# * Check local requests
# ----------------------------------------------------- #
def check_request_local() -> list:
    """Checks for requests stored in local directory
    Returns a list of paths
    """
    get = glob(f"{REQUESTS_DIR}/*.csv")
    return get

# ----------------------------------------------------- #
# * Check api requests
# ----------------------------------------------------- #
def check_request_api() -> list:
    """Check for requests stored in API
    Returns a list of dict values.
    """
    get = get_user_requests()
    return get

# ----------------------------------------------------- #
# * Check valid requests by id matching
# ----------------------------------------------------- #
def check_request_valid_ids(ls_local:list, mode:int=0) -> set:
    """Returns valid requests found by comparing local request files to the requests found in the API. 
    mode: int
        0: return list of request_ids
        1: return a pandas dataframe 
    """
    # get local csv paths
    ls_local = check_request_local()
    mes=f"Total of {len(ls_local)} pending local requests found."
    logging.info(mes)

    # load single row for each csvs, acquire request_ids
    ls_local_df = pd.concat([pd.read_csv(i).head(1) for i in ls_local], axis=0)
    ls_local_ids = set(ls_local_df.request_id.tolist())

    # get api request_ids
    ls_api_ids = {i["request_id"] for i in check_request_api() if i["state"] not in ("failed",)}
    mes=f"Total of {len(ls_api_ids)} pending api requests found."
    logging.info(mes)
    
    ls_valid_ids = ls_local_ids.union(ls_api_ids)
    mes=f"Total of {len(ls_api_ids)} pending valid requests found."
    logging.info(mes)

    if mode == 0:
        return ls_valid_ids
    elif mode == 1:
        return ls_local_df[ls_local_df.request_id.isin(ls_valid_ids)]



