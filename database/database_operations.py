"""
This module contains the DatabaseOperations class to perform database operations using DuckDB.
"""
import duckdb
import pandas as pd
import logging
import threading
import gc
from concurrent.futures import ThreadPoolExecutor
from utils.generic_operations import clean_duplicates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseOperations:
    """
    Class to perform database operations using DuckDB.
    """

    def __init__(self):
        self.db_path = "english_wikipedia.db"
        self.write_lock = threading.Lock()  # Lock to serialize writes
        self.conn = duckdb.connect(self.db_path)  # Single shared connection

    def load_data_parallel(self, table_name, data, max_workers=20):
        """
        Load data into DuckDB in parallel.

        Args:
            table_name (str): The name of the table to load data into.
            data (list of dict): The data to load.
            max_workers (int): Number of threads to use for parallel processing.
        """
        # Clear memory before execution
        logger.info(f"Cleaning memory before starting the operation for {table_name}...")
        gc.collect()  # Trigger garbage collection to free up unused memory

        # Calculate dynamic chunk size
        chunk_size = max(1, len(data) // max_workers)
        logger.info(f"Dynamic chunk size: {chunk_size} for {table_name}")

        # Infer schema
        df = pd.DataFrame(data)
        logger.info(f"Cleaning data for {table_name}...")
        df_clean = clean_duplicates(df)

        # Persist the table structure during creation
        try:
            # Serialize table creation
            with self.write_lock:
                df_clean.iloc[:chunk_size].to_sql(table_name, self.conn, if_exists="replace", index=False)
            logger.info(f"Created table {table_name}")

        except Exception as error:
            logger.error(f"Error creating table {table_name}: {error}")
            self.conn.close()
            raise error

        # Split data into chunks
        data_chunks = [df_clean.iloc[i:i + chunk_size] for i in range(0, len(df_clean), chunk_size)]

        # Load each chunk in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self._load_chunk, [(chunk, table_name) for chunk in data_chunks])

        # Close the connection after all chunks are loaded
        self.conn.close()

    def _load_chunk(self, args):
        """
        Load a single chunk of data into DuckDB.
        Args:
            args (tuple): A tuple containing the data chunk and table name.
        """
        chunk, table_name = args

        try:
            logger.info(f"Loading chunk into table {table_name}")
            # Convert chunk to DataFrame and append to the table
            df = pd.DataFrame(chunk)
            df.to_sql(table_name, self.conn, if_exists="append", index=False)
            logger.info(f"Loaded {len(chunk)} rows into {table_name}")
        except Exception as error:
            logger.error(f"Error loading chunk into table {table_name}: {error}")

    def close_connection(self):
        """Manually close the connection once all operations are done."""
        self.conn.close()
