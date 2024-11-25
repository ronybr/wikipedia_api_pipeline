import duckdb
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
import gc  # Garbage collector module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseOperations:

    def __init__(self):
        self.db_path = "english_wikipedia.db"
        #self.conn = duckdb.connect(self.db_path)  # Connect to DuckDB (creates a new file if it doesn't exist)

    def load_data_parallel(self, table_name, data, max_workers=10):
        """
        Load data into DuckDB in parallel.

        Args:
            table_name (str): The name of the table to load data into.
            data (list of dict): The data to load.
            chunk_size (int): The number of rows per chunk.
            max_workers (int): Number of threads to use for parallel processing.
        """
        # Clear memory before execution
        logger.info("Cleaning memory before starting the operation...")
        gc.collect()  # Trigger garbage collection to free up unused memory

        # Create a connection
        conn = duckdb.connect(self.db_path)

        # Calculate dynamic chunk size
        chunk_size = max(1, len(data) // max_workers)
        logger.info(f"Dynamic chunk size: {chunk_size}")

        # Infer schema and create the table if it doesn't exist
        first_chunk = data[:chunk_size]
        df = pd.DataFrame(first_chunk)
        # Replace NaN values with empty strings
        df.fillna("", inplace=True)

        # Persist the table structure during creation
        try:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            logger.info(f"Created table {table_name}")
            #conn.close()
        except Exception as error:
            logger.error(f"Error creating table {table_name}: {error}")
            conn.close()
            raise error

        conn.close()  # Close the connection after creating the table

        # Split data into chunks
        data_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        # Load each chunk in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self._load_chunk, [(chunk, table_name) for chunk in data_chunks])

    def _load_chunk(self, args):
        """
        Load a single chunk of data into DuckDB.

        Args:
            args (tuple): A tuple containing the data chunk and table name.
        """
        chunk, table_name = args
        conn = duckdb.connect(self.db_path)
        try:
            # Convert chunk to DataFrame and append to the table
            df = pd.DataFrame(chunk)
            df.to_sql(table_name, conn, if_exists="append", index=False)
            logger.info(f"Loaded {len(chunk)} rows into {table_name}")
        except Exception as error:
            logger.error(f"Error loading chunk into table {table_name}: {error}")
        finally:
            conn.close()
