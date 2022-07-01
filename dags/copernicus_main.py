from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator, PythonOperator

from airflow.sensors.python import PythonSensor
from airflow.models import Variable as var

from scripts.services import (handle_request_status, check_request_local, check_request_valid_ids,
    download_request as __download_request, get_str_datetime, get_request_local_state,
    load_to_gcs
    )

from scripts.copernicus import create_request as __create_request, delete_dirs
from scripts.copernicus import create_dirs
from scripts.staging import verify_grib, moveto_staged

from scripts.message import send_message
from scripts.services import get_str_date, complete_request

from scripts.config import (EXTRACT_BUCKET, TRANSFORMED_BUCKET, DELETE_INVALID_DOWNLOAD, 
    STAGED_GRIB_DIR, TRANSFORMED_GRIB_DIR, CLEAN_DIRECTORIES, 
    DELETE_COMPLETED_DOWNLOAD)

from scripts.transform import main_transform
from scripts.validate import main_validate

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
        create_dirs()
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
        return send_message(f"[finished] Completed all DAG operations at {date}")

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
        ls = check_request_local() #_state('extract')
        
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
        return True
        """
        Creates and submits data pull request via Copernicus API
        """
        variable='soil_temperature_level_2'

        date=datetime.now()
        request_params = __create_request(date=date, variable=variable)
        request_id = request_params["request_id"][0]
        
        mes=f"[update] Submitted request for {variable} at {date}, with request_id of {request_id}."
        print(mes)
        send_message(mes)
        return {'request_id':request_id, 'variable':variable, 'date': get_str_date(date)}

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
        date = get_str_date(df.date.astype('datetime64[ns]').squeeze())

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
        """
        Download a completed request from api

        [xcom_pull] 
            request_id: str, unqiue id provided by the API
            variable: str, requested climate variable
        
        return: str, path to downloaded file
        """
        request_id=ti.xcom_pull(key="request_id", task_ids="wait_request")
        variable=ti.xcom_pull(key="variable", task_ids="wait_request")
        date=ti.xcom_pull(key="date", task_ids="wait_request")

        dl_path = __download_request(request_id=request_id, variable=variable, date=date)
        mes = f"[completed] Requested '{variable}' dated '{date}' with request_id '{request_id}' has finished!"
        send_message(mes=mes)
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
            mes = f"[ERROR] Raised exception from task: verify_download, pipeline has stopped."
            send_message(mes)
            raise Exception(e)
        
        # if invalid file
        if valid is False:
            # send error 
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
        # pull variables
        filepath = ti.xcom_pull(task_ids="verify_download")
        # stage file
        dest = moveto_staged(filepath)
        return dest

    # ------------------------------------------------- #
    # UPLOAD STAGED 
    # ------------------------------------------------- #
    @task(task_id="upload_staged")
    def upload_from_staged(ti=None):
        try:
            # pull variables
            src_path = ti.xcom_pull(task_ids='stage_download')
            dest_filepath = src_path.replace(STAGED_GRIB_DIR,"grib")
            dest_filepath = f"extract/{dest_filepath}"

            # load to bucket
            load_to_gcs(bucket_name=EXTRACT_BUCKET, src_filepath=src_path, dest_filepath=dest_filepath)
            
            # send message
            mes = f"[upload] Hooray! staged file: {dest_filepath} was uploaded to the cloud!"
            send_message(mes)
            logging.info(mes)    
        except Exception as e:
            raise Exception(e)

    # ------------------------------------------------- #
    # TRANSFORM DOWNLOAD
    # ------------------------------------------------- #
    def transform_grib(ti=None):
        filepath=ti.xcom_pull(task_ids='stage_download')
        
        dest = main_transform(filepath)
        logging.info(f"[transformed] transformed '{dest.split('/')[-1]}'")


        return dest
    # ------------------------------------------------- #
    # :: operator variable ::
    var_transform_grib=PythonOperator(
        task_id='transform_grib',
        python_callable=transform_grib
    )

    # ------------------------------------------------- #
    # CHECK TRANSFORM 
    # ------------------------------------------------- #
    @task(task_id = 'check_transform')
    def check_transform(ti=None):
        # pull variables
        source = ti.xcom_pull(key="status", task_ids='check_request_queue')
        request = ti.xcom_pull(task_ids=source)
        
        # get filepath
        filepath = ti.xcom_pull('transform_grib')

        # do validation
        try:     
            valid = main_validate(filepath=filepath, request_dict=request)
        except:
            mes = f"[ERROR] Error found in validating transformed file: '{filepath}', pipeline has stopped."
            send_message(mes)
            raise Exception(mes)
        
        # return filepath on successful validation for uploading
        if valid is True:
            return filepath

    # ------------------------------------------------- #
    # UPLOAD TRANSFORMED
    # ------------------------------------------------- #
    @task(task_id = 'upload_transformed')
    def upload_from_transformed(ti=None):
        try:
            # pull variables
            transformed_path = ti.xcom_pull('check_transform')
            cloud_filepath = transformed_path.replace(TRANSFORMED_GRIB_DIR,"parquet")
            cloud_filepath = f"transform/{cloud_filepath}"

            req_fn = transformed_path.split('/')[-1].replace('.parquet','.csv')
            
            # load to bucket
            load_to_gcs(bucket_name=TRANSFORMED_BUCKET, src_filepath=transformed_path, dest_filepath=cloud_filepath)
            
            # process completed requests
            req_fn = transformed_path.split('/')[-1].replace('.parquet','.csv')
            complete_request(req_fn)
            
            # cleanup
            if CLEAN_DIRECTORIES is True:
                delete_dirs()
            elif DELETE_COMPLETED_DOWNLOAD is True:
                os.remove(transformed_path)

            # send message
            mes = f"[upload] Hooray! Transformed file {cloud_filepath} was uploaded to the cloud!"
            send_message(mes)
        except Exception as e:
            raise Exception(e)


    @task(task_id="test")
    def test():
        from scripts.services import check_request_api
        get=check_request_api()
        print(get)

    # start >> create_request()
    start >> var_check_request_queue >> [create_request(), pull_request_data()] >> var_wait_request
    var_wait_request >> download_request() >> verify_download() >> stage_download() >> [var_transform_grib,upload_from_staged()] 
    var_transform_grib >> check_transform() >> upload_from_transformed() >> stop()
