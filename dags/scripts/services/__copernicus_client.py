

import os
import logging
import requests
from cdsapi import Client as cdsapiClient
from . import request_keys
import json
from ..config import SECRET_COPERNICUS_API_NAME, SECRET_COPERNICUS_API_VERSION


__all__ = ["Client_Secure"]

# ----------------------------------------------------- #
# * Client Secure
#   - Read Keys from another source.
#   - Overwrites __init__
# ----------------------------------------------------- #
class Client_Secure(cdsapiClient):
    logger = logging.getLogger("cdsapi")
    
    def set_keys(self):
        key = json.loads("{%s}" % request_keys(SECRET_COPERNICUS_API_NAME, SECRET_COPERNICUS_API_VERSION).replace("\n",","))
        self.url = key['url']
        self.key = key['key']

    def __init__(
        self,
        url=os.environ.get("CDSAPI_URL"),
        key=os.environ.get("CDSAPI_KEY"),
        quiet=False,
        debug=False,
        verify=None,
        timeout=60,
        progress=True,
        full_stack=False,
        delete=True,
        retry_max=500,
        sleep_max=120,
        wait_until_complete=True,
        info_callback=None,
        warning_callback=None,
        error_callback=None,
        debug_callback=None,
        metadata=None,
        forget=False,
        session=requests.Session(),
    ):

        if not quiet:

            if debug:
                level = logging.DEBUG
            else:
                level = logging.INFO

            logging.basicConfig(
                level=level, format="%(asctime)s %(levelname)s %(message)s"
            )

        self.url = url
        self.key = key
        self.set_keys()

        self.quiet = quiet
        self.progress = progress and not quiet

        self.verify = True if verify else False
        self.timeout = timeout
        self.sleep_max = sleep_max
        self.retry_max = retry_max
        self.full_stack = full_stack
        self.delete = delete
        self.last_state = None
        self.wait_until_complete = wait_until_complete

        self.debug_callback = debug_callback
        self.warning_callback = warning_callback
        self.info_callback = info_callback
        self.error_callback = error_callback

        self.session = session
        self.session.auth = tuple(self.key.split(":", 2))

        self.metadata = metadata
        self.forget = forget

        self.debug(
            "CDSAPI %s",
            dict(
                url=self.url,
                key=self.key,
                quiet=self.quiet,
                verify=self.verify,
                timeout=self.timeout,
                progress=self.progress,
                sleep_max=self.sleep_max,
                retry_max=self.retry_max,
                full_stack=self.full_stack,
                delete=self.delete,
                metadata=self.metadata,
                forget=self.forget,
            ),
        )