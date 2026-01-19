import logging
import os

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from web3 import Web3

logger = logging.getLogger(__name__)


load_dotenv()
url = os.getenv("infura_url") + os.getenv("secret")
project_id = os.getenv("project")
dataset_id = os.getenv("dataset")
table_id = os.getenv("table")
key_path = os.getenv("bq_key")

w3 = Web3(Web3.HTTPProvider(url))
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=project_id)


def get_logs(fromBlock, toBlock):
    logger.info("Requesting logs from %s to %s", fromBlock, toBlock)
    logs = w3.eth.get_logs(
        {
            "fromBlock": fromBlock,
            "toBlock": toBlock,
        }
    )
    df = pd.DataFrame(logs)
    logger.info("Retrieved %d logs", len(df))
    return df


def ingest_logs(df):
    logger.info(
        "Starting ingestion to %s.%s.%s (rows=%d)",
        project_id,
        dataset_id,
        table_id,
        len(df),
    )
    try:
        job = client.load_table_from_dataframe(
            df, f"{project_id}.{dataset_id}.{table_id}"
        )
        result = job.result()
        logger.info("Ingestion completed: %s", result)
        logger.info("Ingestion completed: %s",job.output_rows)
        return result
    except Exception as e:
        logger.exception("Ingestion failed")
        return e

def table_exists():
    client.get_table(f"{project_id}.{dataset_id}.{table_id}")

def get_block_number():
    query= f"""
    SELECT max(blockNumber) from `{project_id}.{dataset_id}.{table_id}` 
    """
    query_job = client.query(query)
    results = query_job.result()
    return next(results)[0]
