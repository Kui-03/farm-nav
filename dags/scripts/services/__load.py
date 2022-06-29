from fileinput import filename
import logging
import os, json

from io import StringIO
import boto3

from . import request_keys
import subprocess as sp
import pandas as pd

from ..config import SECRET_CLOUD_ACCESS_NAME, SECRET_CLOUD_ACCESS_VERSION

__all__=["load_to_gcs", "ls_bucket"]

# ----------------------------------------------------- #
# * Load Object to GCS bucket
# ----------------------------------------------------- #
def load_to_gcs(bucket_name:str, src_path:str=None, uploaded_filename:str=None, io:any=None) -> bool:
    """
    Load file/ object to GCS Bucket
    
    Params
    -----------
    bucket_name: str, bucket name
    src_path: str, source path to file '/dir/file.f'
    uploaded_filename: str, cloud path to destination file '/dest/' 
    io: StringIO, data in StringIO data type
    
    """
    key = request_keys(key=SECRET_CLOUD_ACCESS_NAME, version = SECRET_CLOUD_ACCESS_VERSION).split(":")
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=key[0],
        aws_secret_access_key=key[1],
    )

    try:
        if io is None:
            gcs_resource.meta.client.upload_file(src_path, bucket_name, uploaded_filename)
        else:
            gcs_resource.Object(bucket_name, uploaded_filename).put(Body=io.getvalue())
        return True
    except Exception as e:
        raise Exception(e)
        
# ----------------------------------------------------- #
# * List Files in GCP Bucket
# ----------------------------------------------------- #
def ls_bucket(bucket_name:str, query:str) -> list:
    import re
    """
    List files in GCP Bucket
    
    Parameters
    -----------
    bucket_name: str, bucket name
    query: str, path search similar to glob syntax '/path/to/files*txt'

    Returns
    -----------
    A list containing file paths.
    
    """
    cmd = "gsutil ls gs://{0}/{1}".format(bucket_name, query)
    get = sp.run(cmd,capture_output=True, shell=True).stdout.decode().split("\n")
    r = "gs://|{0}/".format(bucket_name)
    get = [re.sub(r, "", i).strip() for i in get if len(i) > 1]
    return get
