from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import logging
from utils.helpers import transfer_hash, table_exists, get_block_number, get_blocks, ingest_decoded_logs, ingest_max_block
import web3 as Web3
import pandas as pd
import time

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    )

logger = logging.getLogger(__name__)

# Cache for environment variables
_env_vars_loaded = False
_env_vars = {}


def _load_env_variables():
    """Load and validate environment variables. Caches results after first call."""
    global _env_vars_loaded, _env_vars
    
    if _env_vars_loaded:
        return _env_vars
    
    try:
        # Load .env file
        load_dotenv()
        
        infura_url = os.getenv("infura_url")
        secret = os.getenv("secret")
        
        if not infura_url or not secret:
            raise ValueError("Missing infura_url or secret environment variables")
        
        url = infura_url + secret
        project_id = os.getenv("project")
        dataset_id = os.getenv("dataset")
        table_raw = os.getenv("table")
        key_path = os.getenv("bq_key")
        table_max_block = os.getenv("table_max_block")
        
        # Validate all required environment variables
        required_vars = {
            "project": project_id,
            "dataset": dataset_id,
            "table": table_raw,
            "bq_key": key_path,
            "table_max_block": table_max_block
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        _env_vars = {
            "url": url,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_raw": table_raw,
            "key_path": key_path,
            "table_max_block": table_max_block
        }
        
        _env_vars_loaded = True
        logger.info("Environment variables loaded and validated successfully")
        return _env_vars
        
    except Exception as e:
        logger.exception(f"Failed to load environment variables: {e}")
        raise



def handler(event, context):
    logger.info("Starting decode_logs handler")
    
    # Load environment variables (will be cached after first call)
    try:
        env_vars = _load_env_variables()
        # Make env vars available as module-level variables for backwards compatibility
        
        globals().update({
            "url": env_vars["url"],
            "project_id": env_vars["project_id"],
            "dataset_id": env_vars["dataset_id"],
            "table_raw": env_vars["table_raw"],
            "key_path": env_vars["key_path"],
            "table_max_block": env_vars["table_max_block"]
        })
    except Exception as e:
        logger.exception("Failed to initialize environment variables")
        return {"status": "error", "message": str(e), "error_type": type(e).__name__}
    
    # Validate table exists
    try:
        table_exists()
        logger.info("Table validation successful")
    except Exception as e:
        logger.exception("Table not found or validation failed")
        return {"status": "error", "message": str(e), "error_type": type(e).__name__}
    
    # Get starting block number
    try:
        block_number = get_block_number()
        if block_number is None:
            block_number = int(17816428)  # default block number to start decoding from
            logger.info(f"Using default block number: {block_number}")
        else:
            logger.info(f"Retrieved block number from checkpoint: {block_number}")
    except Exception as e:
        logger.exception("Failed to get block number from checkpoint, using default")
        block_number = int(17816428)
        logger.info(f"Using default block number: {block_number}")

    # Calculate block range and retrieve logs
    to_block_number = block_number + 5
    logger.info(f"Processing blocks from {block_number} to {to_block_number}")
    
    try:
        logs = get_blocks(block_number, to_block_number)
        logger.info(f"Retrieved {len(logs)} logs from BigQuery")
    except Exception as e:
        logger.exception(f"Failed to retrieve logs from blocks {block_number} to {to_block_number}")
        return {"status": "error", "message": f"Failed to retrieve logs: {str(e)}", "error_type": type(e).__name__}
    
    if not logs:
        logger.warning(f"No logs found for blocks {block_number} to {to_block_number}")
        return {"status": "success", "message": "No logs to process", "processed_count": 0}
    
    # Process logs
    logger.debug(f"Processing {len(logs)} logs")
    erc20_data = []
    erc721_data = []
    transfer_hash_val = transfer_hash()  # Get transfer hash once
    
    for idx, l in enumerate(logs):
        try:
            
            if len(l.topics) > 0  and len(l.topics) == 4:
                try:
                    decoded_row = {
                        "block_number": l.blockNumber,
                        "block_timestamp": l.blockTimestamp,
                        "transaction_hash": l.transactionHash,
                        "from_address": "0x" + l.topics[1][-20:],
                        "to_address": "0x" + l.topics[2][-20:],
                        "id": int(l.data.hex(), 16) if l.data else 0,
                        "processed_timestamp": time.time()
                    }
                    erc721_data.append(decoded_row)
                except (ValueError, IndexError, AttributeError, TypeError) as e:
                    logger.warning(
                        f"Failed to decode ERC721 log at index {idx}: {e}. "
                        f"Transaction: {getattr(l, 'transactionHash', 'unknown')}"
                    )
                    continue
            elif len(l.topics) == 3 and ("0x"+ l.topics[0].hex().lower()) == transfer_hash_val :
                try:
                    decoded_row = {
                        "from_address": "0x" + l.topics[1][-20:],
                        "to_address": "0x" + l.topics[2][-20:],
                        "value": int(l.data.hex(), 16) if l.data else 0,  # Value is in data
                        "contract_address": l.address,
                        "transaction_hash": l.transactionHash,
                        "block_number": l.blockNumber,
                        "processed_timestamp": time.time(),
                        "block_timestamp": l.blockTimestamp
                    }
                    erc20_data.append(decoded_row)
                except (ValueError, IndexError, AttributeError, TypeError) as e:
                    logger.warning(
                        f"Failed to decode ERC20 log at index {idx}: {e}. "
                        f"Transaction: {getattr(l, 'transactionHash', 'unknown')}"
                    )
                    continue
        except Exception as e:
            logger.exception(f"Unexpected error processing log at index {idx}: {e}")
            continue

    logger.info(f"Decoded {len(erc20_data)} ERC20 tokens and {len(erc721_data)} ERC721 tokens")

    # Ingest ERC20 data
    if erc20_data:
        try:
            logger.info(f"Ingesting {len(erc20_data)} ERC20 records to BigQuery")
            ingest_decoded_logs(pd.DataFrame(erc20_data), "erc20")
            logger.info("Successfully ingested ERC20 data")
        except Exception as e:
            logger.exception(f"Failed to ingest ERC20 data: {e}")
            return {
                "status": "partial_error",
                "message": f"ERC20 ingestion failed: {str(e)}",
                "erc721_processed": len(erc721_data),
                "error_type": type(e).__name__
            }
       
    # Ingest ERC721 data
    if erc721_data:
        try:
            logger.info(f"Ingesting {len(erc721_data)} ERC721 records to BigQuery")
            ingest_decoded_logs(pd.DataFrame(erc721_data), "erc721")
            logger.info("Successfully ingested ERC721 data")
        except Exception as e:
            logger.exception(f"Failed to ingest ERC721 data: {e}")
            return {
                "status": "partial_error",
                "message": f"ERC721 ingestion failed: {str(e)}",
                "erc20_processed": len(erc20_data),
                "error_type": type(e).__name__
            }
    
    all_processed = erc20_data + erc721_data

    # Calculate checkpoint
    if all_processed:
        try:
            blocks = [item['block_number'] for item in all_processed]
            if blocks:
                max_block = max(blocks)
                logger.info(f"Maximum processed block: {max_block}")
                
                checkpoint_row = {
                    "last_processed_block": max_block,
                    "updated_at": pd.Timestamp.now()
                }
                logger.debug(f"Checkpoint data prepared: {checkpoint_row}")
                try:
                    ingest_max_block(checkpoint_row)
                except Exception as e:
                    logger.info(f"Failed to ingest decoded block number")

                
            else:
                logger.warning("No block numbers found in processed data")
                return {
                    "status": "success",
                    "message": "Processed logs but no block numbers found",
                    "processed_count": len(all_processed)
                }
        except (KeyError, ValueError) as e:
            logger.exception(f"Failed to determine max block: {e}")
            return {
                "status": "partial_error",
                "message": f"Checkpoint update failed: {str(e)}",
                "processed_count": len(all_processed),
                "error_type": type(e).__name__
            }
    else:
        logger.warning("No data was processed in this run")
    
    logger.info(f"Handler completed successfully. Processed {len(all_processed)} total records")
    return {
        "status": "success",
        "processed_count": len(all_processed),
        "erc20_count": len(erc20_data),
        "erc721_count": len(erc721_data)
    }


    

if __name__== '__main__':
    handler(None, None)