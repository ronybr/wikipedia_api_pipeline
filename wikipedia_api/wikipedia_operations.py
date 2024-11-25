import requests
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WikipediaApi:
    def __init__(self):
        self.api_url = 'https://en.wikipedia.org/w/api.php'

    def fetch_recent_changes_chunk(self, start_date: datetime, end_date: datetime, limit: int =500):
        """
        Fetch recent changes from Wikipedia API within the specified date range.
        Utilizes pagination for large results.
        Params:
            start_date (datetime): Start date for fetching changes
            end_date (datetime): End date for fetching changes
            limit (int): Maximum number of changes to fetch per request
        """
        params = {
            "action": "query",
            "format": "json",
            "list": "recentchanges",
            "rcstart": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rcend": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rclimit": limit,
            "rcdir": "newer"
        }
        chunk_changes = []
        try:
            while True:
                response = requests.get(self.api_url, params=params)
                response.raise_for_status()  # Ensure successful response
                data = response.json()
                chunk_changes.extend(data["query"]["recentchanges"])

                if "continue" not in data:
                    break
                logger.info(f"Fetched {len(chunk_changes)} changes so far...")
                logger.info(f"Continue to page {data['continue']}...")
                params.update(data["continue"])
            return chunk_changes
        except requests.RequestException as error:
            logger.error(f"Error fetching recent changes: {error}")
            raise error

    def fetch_recent_changes_parallel(self, start_date: datetime, end_date: datetime,
                                      limit: int = 500, num_threads: int = 10):
        """
        Fetch recent changes in parallel by splitting the date range into smaller chunks.
        Params:
            start_date (datetime): Start date for fetching changes
            end_date (datetime): End date for fetching changes
            limit (int): Maximum number of changes to fetch per request
            num_threads (int): Number of parallel threads
        """
        # Split the date range into smaller chunks (e.g., 1-hour intervals)
        interval = timedelta(hours=1)
        time_ranges = []
        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + interval, end_date)
            time_ranges.append((current_start, current_end))
            current_start = current_end

        # Use ThreadPoolExecutor to fetch data in parallel
        changes = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_range = {
                executor.submit(self.fetch_recent_changes_chunk, start, end, limit): (start, end)
                for start, end in time_ranges
            }
            for future in as_completed(future_to_range):
                try:
                    result = future.result()
                    changes.extend(result)
                    logger.info(f"Fetched {len(result)} changes for range {future_to_range[future]}")
                except Exception as e:
                    logger.error(f"Error fetching range {future_to_range[future]}: {e}")
        return changes

    def fetch_pages_batch(self, batch):
        params = {
            "action": "query",
            "format": "json",
            "pageids": "|".join(map(str, batch)),
            "prop": "info|revisions",
            "inprop": "url",
        }
        try:
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()
            return response.json()["query"]["pages"]
        except requests.RequestException as error:
            logger.error(f"Error fetching page in batch: {error}")
            raise error

    def fetch_pages_details(self, page_ids, num_threads: int = 10, batch_size: int = 50):
        """
        Fetch page details for a list of page IDs using parallel requests.
        Params:
            page_ids (list): List of page IDs to fetch
            num_threads (int): Number of parallel threads
            batch_size (int): Number of page IDs to fetch per request.
                              Wikipedia API limits 50 page IDs per request
        """
        try:
            # Split page IDs into smaller batches
            page_batches = [list(page_ids)[i:i + batch_size] for i in range(0, len(page_ids), batch_size)]

            # Fetch batches in parallel
            pages = {}
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                results = executor.map(self.fetch_pages_batch, page_batches)
                for result in results:
                    pages.update(result)

            return pages
        except Exception as error:
            logger.error(f"Error fetching page details: {error}")
            raise error
