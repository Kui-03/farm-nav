


# ----------------------------------------------------- #
# * Import packages
# ----------------------------------------------------- #
import os
import subprocess as sp
import json

from google.cloud import secretmanager
from ..config import DEFAULT_PROJECT_NAME

os.environ["PATH"] = "%s:%s/bin" % (os.environ["PATH"], 'opt/google-cloud-sdk')

# ----------------------------------------------------- #
# * Export Functions
# ----------------------------------------------------- #
__all__ = ["request_keys"]

# ----------------------------------------------------- #
# * Request Keys
# ----------------------------------------------------- #
def request_keys(key: str, version: str='latest') -> str:
    info = get_project_info()
    client = secretmanager.SecretManagerServiceClient()
    
    if DEFAULT_PROJECT_NAME == "":
        project_id = info["project_id"]
    else:
        project_id = DEFAULT_PROJECT_NAME

    location = f"projects/{project_id}/secrets/{key}/versions/{version}"
    # request = {"name": location}

    key = client.access_secret_version(name=location).payload.data.decode("utf-8")
    return key

# ----------------------------------------------------- #
# * Get Project Info
# ----------------------------------------------------- #
def get_project_info() -> dict:
    """ Get VM project info
    Returns: 
        A dictionary containing 'project_id', 'project_number'
    """

    # Capture shell out
    project_id = capture_shell("gcloud config get-value project")
    cmd=f"gcloud projects list --filter='project_id:{project_id}'  --format='value(project_number)'"

    project_number = capture_shell(cmd)

    # Check 
    if len(project_id) < 1: raise Exception('No project_id detected! Check gcloud account activation.')
    if len(project_number) < 1: raise Exception('No project_number detected! Check gcloud account activation.')

    # Return as dictionary
    info = {"project_id":project_id, "project_number":project_number}
    return info
    
# ----------------------------------------------------- #
# * Capture Shell Output
# ----------------------------------------------------- #
def capture_shell(cmd: str) -> str:
    """Subprocess capture shell output
    cmd: str, command string
    """
    get = sp.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()
    return get