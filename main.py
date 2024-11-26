import logging
from datetime import datetime
from wikipedia_api.wikipedia_operations import WikipediaApi
from database.database_operations import DatabaseOperations


# 4. Main Execution Pipeline
def main():
    start_process = datetime.now()
    # Define the date range
    start_date = datetime(2024, 10, 31, 0, 0, 0)
    end_date = datetime(2024, 10, 31, 23, 59, 59)

    # Initialize Wikipedia API client
    wiki_api = WikipediaApi()

    print("Fetching recent changes...")
    recent_changes = wiki_api.fetch_recent_changes_parallel(start_date=start_date, end_date=end_date)
    print(f"Fetched {len(recent_changes)} recent changes.")
    # Print the time taken in minutes to collect all changes
    print(f"Time taken to collect all changes: {datetime.now() - start_process}")

    print("Extracting unique page IDs...")
    page_ids = {change["pageid"] for change in recent_changes if "pageid" in change}
    print(f"Found {len(page_ids)} unique page IDs.")

    print("Fetching page details in parallel...")
    pages = wiki_api.fetch_pages_details(page_ids)
    print(f"Fetched details for {len(pages)} pages.")

    print("Loading data into DuckDB...")

    db_ops = DatabaseOperations()

    # Write recent changes to DuckDB
    #recent_changes_db_write = db_ops.load_data_parallel(table_name="recent_changes", data=recent_changes)

    # Write recent changes to DuckDB
    pages_db_write_response = db_ops.load_data_parallel(table_name="pages", data=pages)

    db_ops.close_connection()

    print("Data successfully loaded into DuckDB")


if __name__ == "__main__":
    main()
