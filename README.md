# farm-nav_copernicus-data-builder

This repo contains an example DAG that pulls climate data via CDS API, performs necessary transformations, then performs validations before loading it to google cloud storage buckets. 
 
### Use Case
The example highlights the concept of using an EL-T approach via Airflow DAG.

### Other Features
- Reports errors via Discord Webhook
- Store and access secrets via Google Cloud Secret Manager

### Tasks:
1. **check_request_queue** - This checks local records whether an existing request has been submitted to the API. 
2. **create_request** - This creates a new data request and submits is to the API.
3. **pull_request_data** - This pulls a data from the API to check whether the request exists, then uses the information to monitor the status of a request.
4. **wait_request** - This task waits for the request's 'complete' state before proceeding to download.
5. **download_request** - Downloads a completed request.
6. **verify_download** - Checks whether a download is corrupted.
7. **stage_download** - Moves a downloaded file to staged directory, for transformation and uploading.
8. **upload_staged** - Uploads the validated raw data to the cloud.
9. **transform_grib** - Performs necessary transformations to the downloaded grib file to readable dataframe, exports in compressed parquet format.
10. **check_transform** - Perform final data validations.
11. **upload_transformed** - Uploads transformed data to the google cloud storage buckets.  

### Getting Started

 1. [Install the Docker engine](https://docs.docker.com/engine/) and [Google cloud CLI](https://cloud.google.com/sdk/docs/install)
 2. [Setup Airflow in docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
 3. Create Copernicus account and obtain your [CDS API key](https://cds.climate.copernicus.eu/api-how-to) 
 4. Setup a Google Cloud Project.
 5. Setup your Secrets via [Google Cloud Secret Manager](https://cloud.google.com/secret-manager)
 6. Clone this repo locally and navigate to it in your terminal.
 7. Setup the Secrets section in the config file (See documentation).
 7. Start Airflow by running `docker compose up`
 8. Navigate to localhost:8080 in your browser.

