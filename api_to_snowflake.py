import requests
import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection parameters
snowflake_config = {
    'user': os.getenv('snowflake_user'),
    'password': os.getenv('snowflake_password'),
    'account': os.getenv('snowflake_account'),
    'warehouse': os.getenv('snowflake_warehouse'),
    'database': os.getenv('snowflake_database'),
    'schema': os.getenv('snowflake_schema')
}

base_url = 'https://api.census.gov/data/2020/acs/acs5'

# Define the parameters for the API request
params = {
    'get': 'NAME,B01003_001E',
    'for': 'state:*'
}

def fetch_data(url, params):
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def create_table_if_not_exists(cursor, table_name):
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {table_name} (
        NAME STRING,
        B01003_001E INTEGER,
        state STRING
    );
    """
    cursor.execute(create_table_sql)

def insert_data(cursor, table_name, data):
    insert_sql = f"""
    INSERT INTO {table_name} (NAME, B01003_001E, state) 
    VALUES (%s, %s, %s);
    """
    # Prepare data for bulk insert
    values = [(item[0].replace("'", "''"), int(item[1]), item[2]) for item in data[1:]]
    cursor.executemany(insert_sql, values)

def main():
    try:
        # Fetch data from the API
        data = fetch_data(base_url, params)

        # Establish a connection to Snowflake
        conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            warehouse=snowflake_config['warehouse'],
            database=snowflake_config['database'],
            schema=snowflake_config['schema']
        )

        cursor = conn.cursor()

        table_name = 'MY_TABLE'
        
        # Create table if not exists
        # create_table_if_not_exists(cursor, table_name)

        # Insert data into the table
        insert_data(cursor, table_name, data)

        # Commit the transaction
        conn.commit()

    except snowflake.connector.errors.OperationalError as e:
        print(f"OperationalError: {e}")
    except snowflake.connector.errors.DatabaseError as e:
        print(f"DatabaseError: {e}")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"ProgrammingError: {e}")
    except Exception as e:
        print(f"General Error: {e}")
    finally:
        # Ensure the cursor and connection are closed
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

    print("Data has been inserted into Snowflake")

if __name__ == "__main__":
    main()
