from datetime import datetime


# ----------------------------------------------------- #
# * Get datetime as str
# ----------------------------------------------------- #
def get_str_date(date: datetime=None) -> str:
    if date is None: date = datetime.now()
    return date.strftime('%Y-%m-%d')

# ----------------------------------------------------- #
# * Get datetime_hr as str
# ----------------------------------------------------- #
def get_str_datetime(date: datetime=None) -> str:
    if date is None: date = datetime.now()
    return date.strftime("%m/%d/%Y_%H:%M:%S")