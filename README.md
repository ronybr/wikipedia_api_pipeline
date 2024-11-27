# wikipedia_api_pipeline
This project is designed to extract data from the Wikipedia using the MediaWiki API and load it to a database.

### DBT Project for Wikipedia Changes

### Overview
This DBT project aggregates changes made to Wikipedia pages in 15-minute slots, identifying which slots had the most changes.

### Key Features
- Extracts recent changes and page metadata from the Wikipedia API.
- Loads data into DuckDB for lightweight, efficient querying.
- Transforms raw data using dbt to answer analytical questions.
- Implements a rolling 30-minute slot analysis to find the most active editing periods on Wikipedia.

### Methodology
1. The data is extracted from a DuckDB database containing two tables: `pages` and `recent_changes`.
2. The `recent_changes` table is used to determine the number of changes made within each 15-minute period.
3. The DBT model `recent_changes_aggregated` processes this data, rounding timestamps to 15-minute intervals and counting the changes.

### Design Decisions
- We use `date_trunc()` to round the timestamp down to the nearest 15-minute interval.
- The model aggregates changes per slot and outputs the count for each time period.

### Description of Files/Folders
- **`dbt_project.yml`**: Configuration file for dbt project settings.
- **`dbt_project`**: Contains the dbt project configuration. 
- **`models`**: Contains dbt SQL files to clean, transform, and aggregate data.
- **`requirements.txt`**: Lists Python dependencies for the extraction scripts.
