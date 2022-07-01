
# ===================================================== #
# * Configuration
# ----------------------------------------------------- #


# ----------------------------------------------------- #
# GOOGLE SECRET SETTINGS
# Your gcloud project name/ or project_id, 
# from command: gcloud projects list
# ----------------------------------------------------- #

DEFAULT_PROJECT_NAME=""

# Provide secret name and version from glcoud secret manager
SECRET_COPERNICUS_API_NAME="copernicus-api-key_sub"
SECRET_COPERNICUS_API_VERSION="2"

SECRET_CLOUD_ACCESS_NAME="cloud-farmnav"
SECRET_CLOUD_ACCESS_VERSION="1"

SECRET_SLACK_WEBHOOK_NAME = "slack-webhook"
SECRET_SLACK_WEBHOOK_VERSION = "1"

# ----------------------------------------------------- #
# Cleaning
# ----------------------------------------------------- #
# Delete corrupted downloads 
DELETE_INVALID_DOWNLOAD = True
# Delete completed download
DELETE_COMPLETED_DOWNLOAD = True
# Remove all directory at completion?
CLEAN_DIRECTORIES = True

# ----------------------------------------------------- #
# Directory Settings
# ----------------------------------------------------- #
EXTRACT_GRIB_DIR = "/opt/airflow/data/extract"
STAGED_GRIB_DIR = "/opt/airflow/data/staged"
TRANSFORMED_GRIB_DIR = "/opt/airflow/data/transform"

# Store requests in this directory
REQUESTS_DIR = "/opt/airflow/data/requests"
COMPLETED_REQUESTS_DIR = "/opt/airflow/data/requests/completed"

# ----------------------------------------------------- #
# Bucket Names
# ----------------------------------------------------- #
EXTRACT_BUCKET = "farmnav_extract"
TRANSFORMED_BUCKET = "farmnav_transform"

# ----------------------------------------------------- #
# Scraping Schedule: month, date --not implemented yet
# ----------------------------------------------------- #
GET_SCHEDULE = [6, 6]

# ----------------------------------------------------- #
# Delay in number of months, default is 3, based on 
# copernicus era-5 dataset
# ----------------------------------------------------- #
MONTH_DELAY = 3
