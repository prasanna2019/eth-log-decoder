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
fromBlock = 17816428
toBlock = 17816439


def handler(event, context):
    
    logger.info("Starting handler for blocks %s to %s", fromBlock, toBlock)
    try:
        table_exists()
        number= get_block_number()
        block_number= get_block_number()
        to_block_number= int(block_number)+5
        logs = get_logs(block_number, to_block_number)
    except Exception as e:
        logs = get_logs(fromBlock, toBlock)
    

    logger.info("Fetched %d log entries", len(logs))
    if(not bool(required_columns- set(logs.columns)) and not(logs.empty)):
        # Faster for massive dataframes
        logs['blockTimestamp'] = logs['blockTimestamp'].str.replace('0x', '', regex=False).apply(int, base=16)
        logs['blockTimestamp'] = pd.to_datetime(logs['blockTimestamp'], unit='s')
        result = ingest_logs(logs)
        logger.info("Ingestion result: %s", result)
        return
    logger.info("Exiting without ingesting records")
    
 

if __name__ == "__main__":
    handler(None, None)