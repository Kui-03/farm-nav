
# GOOGLE SECRET SETTINGS
DEFAULT_PROJECT_NAME=""
SECRET_COPERNICUS_API_NAME="copernicus-api-key_sub"
SECRET_COPERNICUS_API_VERSION="2"

SECRET_CLOUD_ACCESS_NAME="cloud-farmnav"
SECRET_CLOUD_ACCESS_VERSION="1"

SECRET_SLACK_WEBHOOK_NAME = "slack-webhook"
SECRET_SLACK_WEBHOOK_VERSION = "1"

# Google Cloud Home
GCLOUD_HOME="/home/meteora/google-cloud-sdk/"

# Delete corrupted downloads 
DELETE_INVALID_DOWNLOAD = True
EXTRACT_GRIB_DIR = "/opt/airflow/data/extract/"
STAGED_GRIB_DIR = "/opt/airflow/data/staged/"
TRANSFORMED_GRIB_DIR = "/opt/airflow/data/transformed/"

# Store requests in this directory
REQUESTS_DIR = "/opt/airflow/data/requests"

# Bucket Names
EXTRACT_BUCKET = "farmnav_extract"
TRANSFORMED_BUCKET = "farmnav_transform"

# Scraping Schedule: month, date
GET_SCHEDULE = [6, 6]
# Delay in number of months, default is 3, based on copernicus era-5 dataset
MONTH_DELAY = 3
