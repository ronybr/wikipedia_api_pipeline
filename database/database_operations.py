import duckdb
import pandas as pd

class DatabaseOperations:

    def __init__(self):
        self.db_path = "wikipedia.db"

    # 3. Load Data into DuckDB
    def load_to_duckdb(self, recent_changes, pages):
        """
        Load recent changes and page details into a DuckDB database.
        """
        con = duckdb.connect(self.db_path)

        # Create tables if not exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS recent_changes (
                rc_id INT,
                rc_title TEXT,
                rc_timestamp TIMESTAMP,
                rc_user TEXT,
                rc_type TEXT
            );
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                page_id INT PRIMARY KEY,
                page_title TEXT,
                page_url TEXT
            );
        """)

        # Insert recent changes
        rc_df = pd.DataFrame(recent_changes)
        con.execute("INSERT INTO recent_changes SELECT * FROM rc_df")

        # Insert pages
        pages_df = pd.DataFrame(pages.values())
        con.execute("INSERT INTO pages SELECT * FROM pages_df")