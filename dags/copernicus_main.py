from datetime import datetime, timedelta
import logging
from os import mkdir
from time import sleep

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

from airflow.sensors.python import PythonSensor
from airflow.models import Variable as var


from scripts.services import (handle_request_status, check_request_local, check_request_valid_ids,
    download_request as __download_request, get_str_datetime
    )

from scripts.copernicus import create_request as __create_request
from scripts.staging import verify_grib, moveto_staged

from scripts.message import send_message
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
        """Checks the local directories whether there is already a submitted request
        [xcom_push] 
            queue: list, csv paths found
            status: str, the next function to return
        """
        # get list of requests
        ls = check_request_local()
        
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
            ti.xcom_push(key='status',value='resume_request')
            return 'resume_request'
    # ------------------------------------------------- #
    # :: operator variable ::
    var_check_request_queue=BranchPythonOperator(
        task_id='check_request_queue', 
        python_callable=check_request_queue
        )

    # ------------------------------------------------- #
    # CREATE REQUEST
    # ------------------------------------------------- #
    @task(task_id='create_request')
    def create_request(ti=None):
        return True
        """Creates and submits data pull request via Copernicus API
        """
        variable="total_precipitation"

        date=datetime.now()
        request_params = __create_request(date=date, variable=variable)
        request_id = request_params["request_id"][0]
        
        mes=f"[update] Submitted request for {variable} at {date}, with request_id of {request_id}."
        print(mes)
        send_message(mes)
        return {'request_id':request_id, 'variable':variable}

    # ------------------------------------------------- #
    # RESUME REQUEST/S
    # ------------------------------------------------- #
    @task(task_id='resume_request')
    def resume_request(ti=None) -> dict:
        """Resumes a valid request by pulling information from the API
        [xcom_pull]
            check_request_queue[queue]: list, list of csv paths found in **/request directory
        
        return: dict, request_id and variable
        """
        ls = ti.xcom_pull(key="queue", task_ids='check_request_queue')
        ls_valid = check_request_valid_ids(ls, mode=1)
        
        # process a single request atm, may expand in parallel in the future
        df = ls_valid.head(1)
        request_id = df.request_id.squeeze()
        variable = df.variable.squeeze()

        # crucial: request_id, variable (for naming & classification)
        return {'request_id':request_id, 'variable':variable}

    # ------------------------------------------------- #
    # WAIT FOR REQUEST
    # ------------------------------------------------- #
    def wait_request(ti=None) -> bool:
        """Waits for a request to complete
        [xcom_pull]
            check_request_queue[status]: str, returned task from check_request_queue
            request: dict, dict containing request_id, variable, returned from either branch
        [xcom_push]
            request_id: str, unqiue id provided by the API
            variable: str, requested climate variable
        [vars]
            source: str, returned task name decided in the branch operator to use for pulling 
                    other params.
            request: dict, contains request_id, variable params.

        return: bool, if request is completed
        """
        source = ti.xcom_pull(key="status", task_ids='check_request_queue')
        request = ti.xcom_pull(task_ids=source)
        
        # get params
        request_id = request["request_id"]
        variable = request["variable"]

        # get status
        decide = handle_request_status(request_id=request_id)
        
        if decide is True:
            # push values to xcomm
            ti.xcom_push(key="request_id", value=request_id)
            ti.xcom_push(key="variable", value=variable)
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
        
        return: str, download path to file.
        """
        request_id=ti.xcom_pull(key="request_id", task_ids="wait_request")
        variable=ti.xcom_pull(key="variable", task_ids="wait_request")

        dl_path = __download_request(request_id=request_id, variable=variable)
        return dl_path

    # ------------------------------------------------- #
    # VERIFY DOWNLOAD
    # ------------------------------------------------- #
    @task(task_id="verify_download")
    def verify_download(ti=None):
        """Checks whether download is corrupted
        [xcom_pull]
            str, download path
        
        return: str, download path
        """
        path = ti.xcom_pull(task_ids="download_request") 
        ti.xcom_push(key="path",value=path)
        
        try:
            path = verify_grib(path)
        except Exception as e:
            mes = f"[ERROR] Raised exception from verify_download, pipeline has stopped."
            send_message(mes)
            raise Exception(e)
            
        if path is False:
            mes = f"[ERROR] Download from {path} might be corrupted."
            send_message(mes)

    # ------------------------------------------------- #
    # STAGE DOWNLOAD
    # ------------------------------------------------- #
    @task(task_id="stage_download")
    def stage_download(ti=None):
        path = ti.xcom_pull(key="path", task_ids="verify_download")
        dest = moveto_staged(path)

        return dest

    # # ------------------------------------------------- #
    # # TRANSFORM DOWNLOAD
    # # ------------------------------------------------- #
    # def transform_grib(ti=None):
    #     ls=ti.xcom_pull(task_ids='stage_download')
    #     path="2022-06-27_total_precipitation"
        
    #     ls=[path]
    #     v = do_transformations(ls)
    #     if v is True:
    #         return ls

    # var_transform_grib=PythonOperator(
    #     task_id='transform_grib',
    #     python_callable=transform_grib
    # )

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

    # # ------------------------------------------------- #
    # # UPLOAD STAGED 
    # # ------------------------------------------------- #
    # @task(task_id="upload_staged")
    # def upload_from_staged(ti=None):
    #     try:
    #         path = ti.xcom_pull('check_transform')
    #         upload_staged([path])
    #         mes = f"[completed] Hooray! Staged file {path} was uploaded to the cloud!"
    #         send_message(mes, var.get("DC_WEBHOOK"))
    #     except Exception as e:
    #         raise Exception(e)

    @task(task_id="test")
    def test():
        from scripts.services import check_request_api, check_request_valid_ids
        print(check_request_api())



    # start >> create_request()
    start >> var_check_request_queue >> [create_request(), resume_request()] >> var_wait_request
    var_wait_request >> download_request() >> verify_download() #>> stage_download() >> [var_transform_grib,upload_staged()] 
    # var_transform_grib >> check_transform() >> upload_transformed()

    # test()