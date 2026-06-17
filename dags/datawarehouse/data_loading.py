import json
from datetime import date
import logging 

logger = logging.getLogger(__name__)

#locate the file path of the json file to be loaded into the database
def load_data():

    file_path = f"./data/yt_data_{date.today()}.json"

#log the loading of the data and handle the case where the file is not found
    try:
        logger.info(f"Loading data from yt_data_{date.today()}")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)

            return data
    except FileNotFoundError:
        logger.error(f"File not found {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file {file_path}")
        raise