

CURL_REQUEST = "curl -X POST -H 'Content-type: application/json' --data '{\"text\": \"@mes\"}' \"@link\" "

import subprocess as sp
from .config import SECRET_SLACK_WEBHOOK_NAME, SECRET_SLACK_WEBHOOK_VERSION
from .services import request_keys


# ------------------------------------------------- #
#  * Send a message to discord webhook
# ------------------------------------------------- #
def send_message(mes:str, url:str=None):
    send_mes = CURL_REQUEST.replace("@mes",mes)
    
    shell_out = sp.run(
        send_mes.replace("@link", 
            request_keys(SECRET_SLACK_WEBHOOK_NAME, SECRET_SLACK_WEBHOOK_VERSION)
            ), 
        shell=True, 
        capture_output=True, 
        text=True
        )
    return shell_out.stdout
