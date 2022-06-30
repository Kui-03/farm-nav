from datetime import datetime, timedelta
import logging
import os
from time import sleep

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

from airflow.sensors.python import PythonSensor
from airflow.models import Variable as var

from scripts.services import (handle_request_status, check_request_local, check_request_valid_ids,
    download_request as __download_request, get_str_datetime, get_request_local_state
    )

from scripts.copernicus import create_request as __create_request
from scripts.staging import verify_grib, moveto_staged

from scripts.message import send_message
from scripts.services import get_str_date, mkdir, locate

from scripts.config import DELETE_INVALID_DOWNLOAD

from scripts.transform import do_transformations

import pandas as pd

# ================================================= #
# * Configurations
# ------------------------------------------------- #
default_args={
        'owner': 'kui',
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5),
}

# ================================================= #
# * Define Copernicus DAG Pipeline
#
# ================================================= #
with DAG(
    default_args=default_args,
    dag_id="copernicus_data_builder_v02",
    start_date=datetime(2022, 6, 6),
    schedule_interval='@monthly',
    catchup=False
) as dag:
# ================================================= #
# * Tasks
# ------------------------------------------------- #

    # ------------------------------------------------- #
    # START
    # ------------------------------------------------- #
    def __start():
        """Commencer le dag~ """
        date = get_str_datetime()
        return send_message(f"[start] Started DAG operations at {date}")
    # :: operator variable ::
    start = PythonOperator(
        task_id='start',
        python_callable=__start
    )

    # ------------------------------------------------- #
    # STOP
    # ------------------------------------------------- #
    @task(task_id="stop")
    def stop():
        date = date = get_str_datetime()
        return send_message(f"[finished] Started DAG operations at {date}")

    # ------------------------------------------------- #
    # [branch] CHECK REQUEST QUEUE
    # ------------------------------------------------- #
    def check_request_queue(ti=None):
        """
        Checks the local directories whether there is already a submitted request
        
        [xcom_push] 
            queue: list, csv paths found
            status: str, the next function to return
        """
        # get list of requests
        ls = get_request_local_state('extract')
        
        mes=f"[status] Currently have {len(ls)} request(s) in queue"
        logging.info(mes)

        # create request
        if len(ls) < 1:
            ti.xcom_push(key='queue',value=ls)
            ti.xcom_push(key='status',value='create_request')
            return 'create_request'
        # push queue params
        else:
            ti.xcom_push(key='queue',value=ls)
            ti.xcom_push(key='status',value='pull_request_data')
            return 'pull_request_data'
    # ------------------------------------------------- #
    # :: operator variable ::
    var_check_request_queue=BranchPythonOperator(
        task_id='check_request_queue', 
        python_callable=check_request_queue,
        )

    # ------------------------------------------------- #
    # CREATE REQUEST
    # variable: climate variable
    # ------------------------------------------------- #
    @task(task_id='create_request')
    def create_request(ti=None):
        # return True
        """
        Creates and submits data pull request via Copernicus API
        """
        variable='total_precipitation'

        date=datetime.now()
        request_params = __create_request(date=date, variable=variable)
        request_id = request_params["request_id"][0]
        
        mes=f"[update] Submitted request for {variable} at {date}, with request_id of {request_id}."
        print(mes)
        send_message(mes)
        return {'request_id':request_id, 'variable':variable, 'date': date, 'path':path}

    # ------------------------------------------------- #
    # PULL REQUEST DATA
    # ------------------------------------------------- #
    @task(task_id='pull_request_data')
    def pull_request_data(ti=None) -> dict:
        """
        Pulls request info from the API and compares to local
        
        [xcom_pull]
            check_request_queue[queue]: list, list of csv paths found in **/request directory
        
        return: dict, request_id, variable, date
        """
        ls = ti.xcom_pull(key="queue", task_ids='check_request_queue')
        df_valid = check_request_valid_ids(ls, mode=1)
        
        # process a single request atm, may expand in parallel in the future
        df = df_valid.head(1)
        request_id = df.request_id.squeeze()
        variable = df.variable.squeeze()
        date = get_str_date(df.get_date.astype('datetime64[ns]').squeeze())

        # crucial: request_id, variable (for naming & classification)
        return {'request_id':request_id, 'variable':variable, 'date':date}

    # ------------------------------------------------- #
    # WAIT FOR REQUEST
    # ------------------------------------------------- #
    def wait_request(ti=None) -> bool:
        """
        Waits for a request to complete
        
        [xcom_pull]
            check_request_queue[status]: str, returned task from check_request_queue
            request: dict, dict containing request_id, variable, returned from either branch

        [xcom_push]
            request_id: str, unqiue id provided by the API
            variable: str, requested climate variable
        
        [vars]
            source: str, returned task name decided in the branch operator to use for pulling 
                    other params
            request: dict, contains request_id, variable params

        return: bool, if request is completed
        """
        source = ti.xcom_pull(key="status", task_ids='check_request_queue')
        request = ti.xcom_pull(task_ids=source)
        
        # get params
        request_id = request["request_id"]
        variable = request["variable"]
        date = request["date"]

        # get status
        decide = handle_request_status(request_id=request_id)
        
        if decide is True:
            # push values to xcomm
            ti.xcom_push(key="request_id", value=request_id)
            ti.xcom_push(key="variable", value=variable)
            ti.xcom_push(key="date", value=date)
            return True
        
        return decide
    # ------------------------------------------------- #
    # :: operator variable ::
    var_wait_request=PythonSensor(
        task_id = "wait_request",
        python_callable = wait_request,
        trigger_rule="none_failed_or_skipped",
        poke_interval = 15,
        timeout = 60*60* 12,
        soft_fail = False, # if exceeding timeout, skips to next
        mode = 'reschedule' # avoid deadlocks
    )
    
    # ------------------------------------------------- #
    # DOWNLOAD REQUEST
    # ------------------------------------------------- #
    @task(task_id="download_request")
    def download_request(ti=None):
        """Download a completed request from api
        [xcom_pull] 
            request_id: str, unqiue id provided by the API
            variable: str, requested climate variable
        
        return: str, path to downloaded file
        """
        request_id=ti.xcom_pull(key="request_id", task_ids="wait_request")
        variable=ti.xcom_pull(key="variable", task_ids="wait_request")
        date=ti.xcom_pull(key="date", task_ids="wait_request")

        dl_path = __download_request(request_id=request_id, variable=variable, date=date)
        return dl_path

    # ------------------------------------------------- #
    # VERIFY DOWNLOAD
    # ------------------------------------------------- #
    @task(task_id="verify_download")
    def verify_download(ti=None):
        """Checks whether download is corrupted
        [xcom_pull]
            str, download path
        
        return: str, path to downloaded file
        """
        filepath = ti.xcom_pull(task_ids="download_request") 
        
        try:
            valid = verify_grib(filepath)
        except Exception as e:
            mes = f"[ERROR] Raised exception from verify_download, pipeline has stopped."
            send_message(mes)
            raise Exception(e)
        
        # if invalid file
        if valid is False:
            mes = f"[ERROR] Download from {filepath} is corrupted."
            send_message(mes)
            # if deleting
            if DELETE_INVALID_DOWNLOAD is True:
                os.remove(filepath)
                mes = f"[critical] Invalid file from {filepath} was deleted, See config."
                send_message(mes)
                raise Exception("[exception] Invalid file detected and was deleted, restart DAG.")
        return filepath

    # ------------------------------------------------- #
    # STAGE DOWNLOAD
    # ------------------------------------------------- #
    @task(task_id="stage_download")
    def stage_download(ti=None):
        filepath = ti.xcom_pull(task_ids="verify_download")
        dest = moveto_staged(filepath)
        return dest
        
    # ------------------------------------------------- #
    # UPLOAD STAGED 
    # ------------------------------------------------- #
    @task(task_id="upload_staged")
    def upload_from_staged(ti=None):
        try:
            path = ti.xcom_pull('check_transform')
            upload_staged([path])
            mes = f"[completed] Hooray! Staged file {path} was uploaded to the cloud!"
            send_message(mes, var.get("DC_WEBHOOK"))
        except Exception as e:
            raise Exception(e)

    # ------------------------------------------------- #
    # TRANSFORM DOWNLOAD
    # ------------------------------------------------- #
    def transform_grib(ti=None):
        grib=ti.xcom_pull(task_ids='stage_download')
        v = do_transformations(grib=grib)

    var_transform_grib=PythonOperator(
        task_id='transform_grib',
        python_callable=transform_grib
    )

    # # ------------------------------------------------- #
    # # CHECK TRANSFORM 
    # # ------------------------------------------------- #
    # @task(task_id = 'check_transform')
    # def check_transform(ti=None):
        
    #     path = ti.xcom_pull('transform_grib')
    #     df = pd.read_csv(path)
    #     # Perform last verifications here
    #     if "total_precipitation" not in df.columns.tolist():
    #         mes=f"[ERROR] columns total_precipitation not found in {path}!"
    #         send_message(mes)
    #         raise Exception(mes)
    #     if check_valid_data(df) is True:
    #         return path

    # # ------------------------------------------------- #
    # # UPLOAD TRANSFORMED
    # # ------------------------------------------------- #
    # @task(task_id = 'upload_transformed')
    # def upload_from_transformed(ti=None):
        
    #     try:
    #         path = ti.xcom_pull('check_transform')
    #         upload_transformed([path])
    #         mes = f"[completed] Hooray! Transformed {path} was uploaded to the cloud!"
    #     except Exception as e:
    #         raise Exception(e)



    @task(task_id="test")
    def test():
        from scripts.services import check_request_local_state
        check_request_local_state()

    # start >> create_request()
    start >> var_check_request_queue >> [create_request(), pull_request_data()] >> var_wait_request
    var_wait_request >> download_request() >> verify_download() >> stage_download() >> [var_transform_grib,upload_from_staged()] 
    # var_transform_grib >> check_transform() >> upload_transformed()

    test()