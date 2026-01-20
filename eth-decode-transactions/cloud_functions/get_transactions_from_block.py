import logging
import os

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import get_logs, ingest_logs, table_exists, get_block_number



if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    )

logger = logging.getLogger(__name__)

required_columns= {'address', 'blockTimestamp'}


def handler(event, context):
    try:
        table_exists()
        block_number= get_block_number()
        
        if block_number== None:
            block_number= 1010 #default block number for first time
            print(block_number)
    except Exception as e:
        logger.exception("Failed to get last processed block number")
        return
    try:
        to_block_number= int(block_number)+5
        print(to_block_number)
        logs = get_logs(block_number, to_block_number)
        logger.info("Fetched %d log entries", len(logs))
    except:
        logger.exception("Failed to get block data")
    if(not bool(required_columns- set(logs.columns)) and not(logs.empty)):
    # Faster for massive dataframes
        logs['blockTimestamp'] = logs['blockTimestamp'].str.replace('0x', '', regex=False).apply(int, base=16)
        logs['blockTimestamp'] = pd.to_datetime(logs['blockTimestamp'], unit='s')
        try:
            result = ingest_logs(logs)
            logger.info("Ingestion result: %s", result)
        except Exception as e:
            logger.exception("Failed to ingest block data")
    else:
        logger.info("Required columns missing in block data")

        return
    logger.info("Exiting without ingesting records")
    
 

if __name__ == "__main__":
    handler(None, None)