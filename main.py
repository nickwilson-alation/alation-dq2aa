import os
import sys
import requests
import csv
import snowflake.connector
from dotenv import load_dotenv
import logging
import datetime

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_api():
    """Fetch all data from the Alation Data Quality API."""
    base_url = os.getenv('ALATION_API_BASE_URL')
    token = os.getenv('ALATION_API_TOKEN')
    headers = {'token': token}
    params = {
        'order_by': '-value_last_updated',
        'limit': 1000  # Adjust as needed based on API limits
    }

    page = 1
    total_records = 0
    while True:
        logging.info(f"Fetching page {page}")
        response = requests.get(f"{base_url}/integration/v1/data_quality/values/", headers=headers, params=params)
        if response.status_code != 200:
            logging.error(f"Error fetching data from API: {response.text}")
            sys.exit(1)

        data = response.json()
        if not data:
            break

        records_fetched = len(data)
        total_records += records_fetched
        logging.info(f"Fetched {records_fetched} records from page {page}, total records so far: {total_records}")

        yield data  # Yield the data for this page

        # Check if there's a next page
        next_page_url = response.headers.get('x-next-page')
        if not next_page_url:
            break

        # Prepare for the next page
        params = {}  # Reset params for next page if required
        page += 1

import datetime

def write_data_to_csv(data, fieldnames, file_counter):
    """Write data to a CSV file."""
    file_name = f'data_files/dq_health_values_data_{file_counter}.csv'
    try:
        records_written = 0
        with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                # Ensure all required fields are present
                for field in fieldnames:
                    row.setdefault(field, '')
                writer.writerow(row)
                records_written += 1
        logging.info(f"Wrote {records_written} records to {file_name}")
        return file_name
    except Exception as e:
        logging.error(f"Error writing data to CSV: {e}")
        sys.exit(1)

def upload_file_to_snowflake(cur, file_path):
    """Upload the CSV file to a Snowflake stage."""
    try:
        put_sql = f"PUT file://{os.path.abspath(file_path)} @~/dq_health_values_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cur.execute(put_sql)
        logging.info(f"File {file_path} uploaded to Snowflake stage successfully.")
    except Exception as e:
        logging.error(f"Error uploading file to Snowflake: {e}")
        sys.exit(1)

def truncate_table(cur):
    """Truncate the DQ_HEALTH_VALUES table."""
    try:
        cur.execute("TRUNCATE TABLE DQ_HEALTH_VALUES")
        logging.info("DQ_HEALTH_VALUES table truncated successfully.")
    except Exception as e:
        logging.error(f"Error truncating table: {e}")
        sys.exit(1)

def load_data_into_snowflake(cur):
    """Load data from the Snowflake stage into the DQ_HEALTH_VALUES table."""
    try:
        copy_sql = """
        COPY INTO DQ_HEALTH_VALUES
        FROM @~/dq_health_values_stage
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE
        """
        cur.execute(copy_sql)
        logging.info("Data loaded into DQ_HEALTH_VALUES table successfully.")
    except Exception as e:
        logging.error(f"Error loading data into Snowflake table: {e}")
        sys.exit(1)

def cleanup_snowflake_stage(cur):
    """Remove files from the Snowflake stage."""
    try:
        cur.execute("REMOVE @~/dq_health_values_stage")
        logging.info("Snowflake stage cleaned up.")
    except Exception as e:
        logging.error(f"Error cleaning up Snowflake stage: {e}")
        sys.exit(1)

def main():
    # Connect to Snowflake
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        cur = conn.cursor()
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        sys.exit(1)

    # Truncate the DQ_HEALTH_VALUES table
    truncate_table(cur)

    # Field names matching the Snowflake table columns
    fieldnames = [
        'object_key', 'object_name', 'otype', 'oid',
        'source_object_key', 'source_object_name', 'source_otype', 'source_oid',
        'value_id', 'value_value', 'value_quality', 'value_last_updated',
        'value_external_url', 'field_key', 'field_name', 'field_description'
    ]

    # Directory to store CSV files
    os.makedirs('data_files', exist_ok=True)

    file_counter = 1

    # Fetch data and process page by page
    try:
        for data_chunk in fetch_data_from_api():
            # Write data to CSV
            csv_file_path = os.path.join('data_files', f'dq_health_values_data_{file_counter}.csv')
            csv_file_path = write_data_to_csv(data_chunk, fieldnames, file_counter)

            # Upload the CSV file to Snowflake
            upload_file_to_snowflake(cur, csv_file_path)

            # Remove local CSV file to save space
            os.remove(csv_file_path)
            logging.info(f"Local CSV file {csv_file_path} removed.")

            file_counter += 1

        # Load data into Snowflake
        load_data_into_snowflake(cur)

        # Cleanup Snowflake stage
        cleanup_snowflake_stage(cur)

        # Commit and close connection
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Data load process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        cur.close()
        conn.close()
        sys.exit(1)

if __name__ == '__main__':
    main()