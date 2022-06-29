

# Secrets URL path
from datetime import timedelta


DEFAULT_PROJECT_NAME=""
SECRET_COPERNICUS_API_NAME="copernicus-api-key_sub"
SECRET_COPERNICUS_API_VERSION="2"

SECRET_CLOUD_ACCESS_NAME="cloud-farmnav"
SECRET_CLOUD_ACCESS_VERSION="1"

SECRET_SLACK_WEBHOOK_NAME = "slack-webhook"
SECRET_SLACK_WEBHOOK_VERSION = "1"

GCLOUD_HOME="/home/meteora/google-cloud-sdk/"



# Extract Directories
EXTRACT_GRIB_DIR = "/opt/airflow/data/extract/grib"
EXTRACT_META_DIR = "/opt/airflow/data/extract/metadata"

# Staging Directories
STAGED_GRIB_DIR = "/opt/airflow/data/staged/grib"
STAGED_META_DIR = "/opt/airflow/data/staged/metadata"

# Transformation directories
TRANSFORMED_GRIB_DIR = "/opt/airflow/data/transformed/grib"
TRANSFORMED_META_DIR = "/opt/airflow/data/transformed/metadata"

REQUESTS_DIR = "/opt/airflow/data/requests"

# Bucket Names
EXTRACT_BUCKET = "farmnav_extract"
TRANSFORMED_BUCKET = "farmnav_transform"


# Scraping Schedule: month, date
GET_SCHEDULE = [6, 6]
MONTH_DELAY = 3
