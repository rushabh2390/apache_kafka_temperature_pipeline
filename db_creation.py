import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Retrieve the connection parameters from environment variables
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Example query to create a table
    create_table_query = """
    CREATE TABLE temperature (
        id SERIAL PRIMARY KEY,
        date VARCHAR(50),
        morning DOUBLE PRECISION,
        afternoon DOUBLE PRECISION,
        evening DOUBLE PRECISION,
        createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updateddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    print("Table 'temperature' created successfully")

    # Close the cursor and connection
    cursor.close()
    conn.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(f"Error: {error}")