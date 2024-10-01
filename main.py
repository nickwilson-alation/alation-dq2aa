from dotenv import load_dotenv
import os
import snowflake.connector

# Load environment variables from .env file
load_dotenv()


# Database connection
conn = snowflake.connector.connect(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    account=os.getenv("DB_ACCOUNT"),
    warehouse=os.getenv("DB_WAREHOUSE"),
    database=os.getenv("DB_DATABASE"),
    schema=os.getenv("DB_SCHEMA")
)

cur = conn.cursor()

try:
    # Step 1: Truncate the table
    truncate_sql = "TRUNCATE TABLE DQ_HEALTH_VALUES;"
    cur.execute(truncate_sql)
    print("Table truncated successfully.")

    # Data to be inserted
    data = [
        ('19.mw_ideal.mw_ideal.projects', 'projects', 'table', 169, '9.mw_ideal.mw_ideal.projects', 'projects', 'table', 169, 6, 12, 'GOOD', '2024-09-27T15:12:00.771030Z', '', 'employee.id.distinct.count', 'Distinct Count', 'The count of unique employee IDs in the table.'),
    ]
    
    # SQL query to insert data
    sql = "INSERT INTO DQ_HEALTH_VALUES (object_key, object_name, otype, oid, source_object_key, source_object_name, source_otype, source_oid, value_id, value_value, value_quality, value_last_updated, value_external_url, field_key, field_name, field_description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    # Execute the query for each row of data
    cur.executemany(sql, data)

    # Commit the transaction
    conn.commit()
    print("Data inserted successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()