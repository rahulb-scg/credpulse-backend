from io import StringIO
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
import pandas as pd

def db_connection():
    conn = psycopg2.connect(
        host="localhost",
        dbname="credpulse",
        user="rahul",
        password="00000"
        )

    return conn


def df_to_db(df = 'df', engine='engine', tableName = 'masterTable'):
    
    conn = db_connection()

    # Convert DataFrame to CSV format
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)  # Skip headers if the table is pre-created
    buffer.seek(0)

    print('DF to CSV complete..')
    
    try:
        # Insert data into the table
        df.head(0).to_sql(tableName, engine, if_exists='replace', index=False)

        # Execute the COPY command to import data
        with conn.cursor() as cursor:
            copy_sql = f"COPY {tableName} FROM stdin WITH CSV"
            cursor.copy_expert(copy_sql, buffer)
            conn.commit()

        conn.close()

    
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")
        
        # Handle table creation or other actions
        print(f"Creating table {tableName}...")
        df.to_sql(tableName, engine, if_exists='replace', index=False, chunksize=10000)
        print(f"Table {tableName} created and data inserted.")
