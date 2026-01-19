import os
from google.cloud import bigquery

def initialize_bq():
    # 1. Pull values from environment variables
    project_id = os.getenv("project_id")
    dataset_id = os.getenv("dataset_id")
    table_raw = os.getenv("table_raw")
    table_20 = os.getenv("table_erc20")
    table_721 = os.getenv("table_erc721")
    table_max_block= os.getenv("table_max_block")


    if not all([project_id, dataset_id, table_raw, table_20, table_721]):
        print("Error: Missing environment variables!")
        return

    client = bigquery.Client(project=project_id)

    # 2. Use DDL for easy "Create if not exists"
    # We use BYTES for addresses and CLUSTER BY for speed
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_raw}` (
    address STRING OPTIONS(description="The contract address that emitted the log"),
    blockHash BYTES OPTIONS(description="Hash of the block containing the log"),
    blockNumber INTEGER OPTIONS(description="The number of the block containing the log"),
    blockTimestamp DATETIME OPTIONS(description="The timestamp of the block"),
    data BYTES OPTIONS(description="The non-indexed data of the log"),
    logIndex INTEGER OPTIONS(description="The index of the log within the block"),
    removed BOOLEAN OPTIONS(description="True if the log was removed due to a chain reorganization"),
    topics ARRAY<BYTES> OPTIONS(description="The indexed log topics"),
    transactionHash BYTES OPTIONS(description="Hash of the transaction that generated the log"),
    transactionIndex INTEGER OPTIONS(description="The index of the transaction within the block")
    )
    PARTITION BY DATE(blockTimestamp)
    CLUSTER BY address, transactionHash;
    """

    print(f"Executing setup for {table_raw}...")
    query_job = client.query(sql)
    query_job.result()  # Wait for table creation to finish

    sql1= f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_max_block}` (
    blockNumber NUMERIC	NOT NULL,
    processed_timestamp TIMESTAMP NOT NULL
    )
    PARTITION BY DATE(processed_timestamp)
    """

    print(f"Executing setup for {table_max_block}...")
    query_job = client.query(sql1)
    query_job.result()

    sql2= """
    block_number INTEGER NOT NULL,
    block_timestamp DATETIME NOT NULL,
    transaction_hash BYTES NOT NULL,
    from_address BYTES NOT NULL,
    to_address BYTES NOT NULL,
    id NUMERIC,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    PARTITION BY DATE(block_timestamp)
    """

    print(f"Executing setup for {table_721}...")
    query_job = client.query(sql2)
    query_job.result()

    sql3= """
    block_number INTEGER NOT NULL,
    block_timestamp DATETIME NOT NULL,
    transaction_hash BYTES NOT NULL,
    from_address BYTES NOT NULL,
    contract_address BYTES NOT NULL,
    to_address BYTES NOT NULL,
    value NUMERIC,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    PARTITION BY DATE(block_timestamp)

    """

if __name__ == "__main__":
    initialize_bq()