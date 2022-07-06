import logging
from datetime import datetime
import os

LOG_DIR="housing_logs"

CURRENT_TIME_STAMP=  f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"

def get_log_file_name(current_time_stamp):
    return f"log_{current_time_stamp}.log"

LOG_FILE_NAME=get_log_file_name(current_time_stamp=CURRENT_TIME_STAMP)


os.makedirs(LOG_DIR,exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR,LOG_FILE_NAME)

logging.basicConfig(filename=LOG_FILE_PATH,
filemode="w",
format='[%(asctime)s] \t\t %(processName)s \t\t %(threadName)s \t\t %(name)s \t\t %(lineno)d \t\t %(filename)s \t\t %(funcName)s \t\t %(levelname)s \t\t %(message)s',
level=logging.INFO
)