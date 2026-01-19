import logging
import os
import time

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
table_raw = os.getenv("table_raw")
key_path = os.getenv("bq_key")
table_decoded = os.getenv("table_decoded")
table_erc20= os.getenv("table_erc20")
table_erc721= os.getenv("table_erc721")
table_max_block= os.getenv("table_max_block")
from eth_utils import keccak, to_hex 

w3 = Web3(Web3.HTTPProvider(url))
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=project_id)

event_signature= "Transfer(address,address,uint256)"

job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

def ingest_logs(df):
    logger.info(
        "Starting ingestion to %s.%s.%s (rows=%d)",
        project_id,
        dataset_id,
        table_decoded,
        len(df),
    )
    try:
        job = client.load_table_from_dataframe(
            df, f"{project_id}.{dataset_id}.{table_decoded}"
        )
        result = job.result()
        logger.info("Ingestion completed: %s", result)
        logger.info("Ingestion completed: %s",job.output_rows)
        return result
    except Exception as e:
        logger.exception("Ingestion failed")
        return e

def transfer_hash():
    event_signature = "Transfer(address,address,uint256)"
    return "0x"+ keccak(text= event_signature).hex()

def table_exists():
    client.get_table(f"{project_id}.{dataset_id}.{table_max_block}")

def get_block_number():
    query= f"""
    SELECT max(blockNumber) FROM `{project_id}.{dataset_id}.{table_max_block}` WHERE processed_timestamp= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    """
    query_job = client.query(query)
    results = query_job.result()
    return next(results)[0]

def get_blocks(block_number, to_block_number):
    query= f"""
    SELECT blockHash, blockNumber,blockTimestamp, data, topics, transactionHash, transactionIndex FROM `{project_id}.{dataset_id}.{table_raw}`
    WHERE blockNumber >= {block_number}  AND blockNumber <= {to_block_number}"""
    query_job= client.query(query)
    query_result= list(query_job.result())
    return query_result

def ingest_decoded_logs( df, token):
    if token=='erc20':
        table= table_erc20
    elif token=='erc721':
        table= table_erc721
    job= client.insert_rows_from_dataframe(table= f"{project_id}.{dataset_id}.{table}", dataframe=df, job_config= job_config)
    return list(job.result())

def ingest_max_block(block_data):
    job= client.insert_rows_from_dataframe(table= f"{project_id}.{dataset_id}.{table_max_block}", dataframe=block_data, job_config= job_config)
    return list(job.result())