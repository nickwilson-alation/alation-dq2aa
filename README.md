# alation-dq2aa
Supplements Alation Analytics by adding Data Quality information from the API to a Snowflake database which also has access to the Alation Analytics Snowlflake share.

# Alation Data Quality API Data Loader

This script fetches data from the Alation Data Quality API and loads it into a Snowflake table named `DQ_HEALTH_VALUES`. It handles large datasets efficiently by:

- Fetching all records from the API with pagination support.
- Writing data to CSV files incrementally to manage memory usage.
- Uploading CSV files to Snowflake using the `PUT` command.
- Loading data into Snowflake using the `COPY INTO` command.
- Truncating the target table before loading new data to reflect the current state of the API data.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Snowflake Table Setup](#snowflake-table-setup)
- [Running the Script](#running-the-script)
- [License](#license)

---

## Prerequisites

Before running the script, ensure you have the following:

1. **Python 3.6 or higher** installed on your system.
2. **Snowflake account** with the necessary permissions to create tables, stages, and load data.
3. **Alation Data Quality API access** with a valid API token.
4. **Required Python packages** installed (see [Installation](#installation)).

---

## Installation

1. **Clone the Repository or Download the Script**

   Save the script file (e.g., `alation_data_loader.py`) to your desired directory.

2. **Install Required Packages**

   ```bash
   pip3 install -r requirements.txt
   ```

## Configuration

1. **Environment Variables**

   Create a `.env` file in the same directory as the script to securely store your credentials and configurations.

   Example `.env` File:
   ```bash
   # Snowflake credentials
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ACCOUNT=your_account_identifier  # e.g., 'abc12345.us-east-1'
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   SNOWFLAKE_ROLE=your_role  # If applicable

   # Alation API credentials
   ALATION_API_BASE_URL=https://your_alation_instance.com
   ALATION_API_TOKEN=your_alation_api_token
   ```

   Note:
   - Replace the placeholders with your actual credentials.
   - Ensure that the .env file is not committed to any version control system to keep your credentials secure.

2. **Adjust Script Parameters (Optional)**
   - API Request Parameters:
      - You can adjust the limit parameter in the API request to control the number of records fetched per page.
      - Default is set to 1000.
   - File Paths:
      - The script writes CSV files to a directory named data_files.
      - You can change the directory name or path in the script if needed.

## Snowflake Table Setup
Before running the script, create the necessary table in Snowflake.
1. **Connect to Snowflake**

   Use your preferred method to connect to Snowflake (SnowSQL CLI, Web UI, or any SQL client).

2. **Create the `DQ_HEALTH_VALUES` Table**

   Execute the following SQL statement to create the table:

   Example `.env` File:
   ```sql
   CREATE OR REPLACE TABLE DQ_HEALTH_VALUES (
    OBJECT_KEY VARCHAR(250) NOT NULL,
    OBJECT_NAME VARCHAR(250) NOT NULL,
    OTYPE VARCHAR(250) NOT NULL,
    OID NUMBER(38,0) NOT NULL,
    SOURCE_OBJECT_KEY VARCHAR(250) NOT NULL,
    SOURCE_OBJECT_NAME VARCHAR(250) NOT NULL,
    SOURCE_OTYPE VARCHAR(250) NOT NULL,
    SOURCE_OID NUMBER(38,0) NOT NULL,
    VALUE_ID NUMBER(38,0) NOT NULL,
    VALUE_VALUE VARCHAR(250),
    VALUE_QUALITY VARCHAR(250),
    VALUE_LAST_UPDATED TIMESTAMP_TZ(6) NOT NULL,
    VALUE_EXTERNAL_URL VARCHAR(250),
    FIELD_KEY VARCHAR(250),
    FIELD_NAME VARCHAR(250),
    FIELD_DESCRIPTION VARCHAR(250)
   );
   ```
   Note:
   - Ensure that the table is created in the database and schema specified in your .env file.

## Running the Script
1. **Ensure All Configurations Are Set**
   - Confirm that the .env file contains all the necessary credentials.
   - Verify that the DQ_HEALTH_VALUES table exists in Snowflake.
2. **Run the Script**

   Execute the script using Python:

   ```bash
   python3 ./main.py
   ```
2. **Monitor the Output**
   - The script will output logs indicating the progress of data fetching, writing, uploading, and loading.
   - Any errors encountered will be logged with details.

## Script Overview
### Functionality
- #### Data Retrieval:
   - Fetches all data from the Alation Data Quality API.
   - Handles pagination using the x-next-page header.
   - Fetches data in chunks to manage memory usage.
- #### Data Storage:
   - Writes fetched data to CSV files incrementally.
   - Each page of data is saved as a separate CSV file in the data_files directory.
- #### Data Upload to Snowflake:
   - Uploads CSV files to a Snowflake internal stage using the PUT command.
   - Compresses files during upload to save space and improve upload speed.
- #### Data Loading into Snowflake:
   - Truncates the DQ_HEALTH_VALUES table before loading new data.
   - Uses the COPY INTO command to bulk load data from the stage into the table.
- #### Cleanup:
   - Removes local CSV files after they are uploaded to conserve disk space.
   - Cleans up the Snowflake stage by removing files after loading.
### Script Structure
- #### Imports and Configuration:
   - Imports required libraries and loads environment variables.
   - Configures logging for progress tracking.
- #### Function Definitions:
   - fetch_data_from_api(): Fetches data from the API.
   - write_data_to_csv(): Writes data chunks to CSV files.
   - upload_file_to_snowflake(): Uploads CSV files to Snowflake stage.
   - truncate_table(): Truncates the target table in Snowflake.
   - load_data_into_snowflake(): Executes the COPY INTO command to load data.
   - cleanup_snowflake_stage(): Removes files from the Snowflake stage.
- #### Main Execution Flow:
   - Connects to Snowflake using the provided credentials.
   - Truncates the DQ_HEALTH_VALUES table.
   - Fetches data from the API and processes it page by page.
   - Uploads CSV files to Snowflake and removes local copies.
   - Loads data into the table and cleans up the stage.
   - Commits the transaction and closes the connection.

## Disclaimer
This project and all the code contained within this repository is provided "as is" without warranty of any kind, either expressed or implied, including, but not limited to, the implied warranties of merchantability and fitness for a particular purpose. The entire risk as to the quality and performance of the project is with you.

The author, including Alation, shall not be responsible for any damages whatsoever, including direct, indirect, special, incidental, or consequential damages, arising out of or in connection with the use or performance of this project, even if advised of the possibility of such damages.

Please understand that this project is provided as a **sample** for educational and informational purposes only. Always ensure proper testing, validation and backup before implementing any code or program in a production environment.

By using this project, you accept full responsibility for any and all risks associated with its usage.