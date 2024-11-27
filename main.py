"""
This script is the main entry point for the Wikipedia API data collection and storage process.
"""
import logging
from datetime import datetime
from wikipedia_api.wikipedia_operations import WikipediaApi
from database.database_operations import DatabaseOperations

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 4. Main Execution Pipeline
def main():

    start_process = datetime.now()

    # Define the date range
    start_date = datetime(2024, 10, 31, 0, 0, 0)
    end_date = datetime(2024, 10, 31, 23, 59, 59)

    # Initialize Wikipedia API client
    wiki_api = WikipediaApi()

    logger.info("Fetching recent changes...")
    recent_changes = wiki_api.fetch_recent_changes_parallel(start_date=start_date, end_date=end_date)

    logger.info(f"Fetched {len(recent_changes)} recent changes.")
    # Print the time taken in minutes to collect all changes
    logger.info(f"Time taken to collect all changes: {datetime.now() - start_process}")

    logger.info("Extracting unique page IDs...")
    page_ids = {change["pageid"] for change in recent_changes if "pageid" in change}
    logger.info(f"Found {len(page_ids)} unique page IDs.")

    logger.info("Fetching page details in parallel...")
    pages = wiki_api.fetch_pages_details(page_ids)
    logger.info(f"Fetched details for {len(pages)} pages.")

    logger.info("Loading data into DuckDB...")

    db_ops = DatabaseOperations()

    # Write recent changes to DuckDB
    db_ops.load_data_parallel(table_name="recent_changes", data=recent_changes)

    # Write recent changes to DuckDB
    db_ops.load_data_parallel(table_name="pages", data=pages)

    db_ops.close_connection()

    logger.info("Data successfully loaded into DuckDB")


if __name__ == "__main__":
    main()
