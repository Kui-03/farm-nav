from datetime import datetime


# ----------------------------------------------------- #
# * Get datetime as str
# ----------------------------------------------------- #
def get_str_date() -> str:
    return datetime.now().strftime('%Y-%m-%d')

# ----------------------------------------------------- #
# * Get datetime_hr as str
# ----------------------------------------------------- #
def get_str_datetime() -> str:
    return datetime.now().strftime("%m/%d/%Y, %H:%M:%S")