import pandas as pd 
import pyodbc, oracledb
from sqlalchemy import create_engine

def db_handler(connection_params):
    """
    Connects to the client's database, executes a query, and fetches data as a DataFrame.
    
    Parameters:
    - db_type: The type of the client database (e.g., "postgresql", "mysql", "oracle", "mssql", "sqlite")
    - connection_params: Dictionary of connection parameters (host, port, user, password, database)
    - query: SQL query to fetch data
    
    Returns:
    - Data as a pandas DataFrame
    """
    # Define the engine format for each database type
    engine_format = {
        "postgresql": "postgresql://{user}:{password}@{host}:{port}/{database}",
        "mysql": "mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        "oracle": "oracle+oracledb://{user}:{password}@{host}:{port}/{database}",
        "mssql": "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
        "sqlite": "sqlite:///{database}"  # SQLite does not require host/port/user/password
    }

    db_type = connection_params['engine']

    try:
        if db_type not in engine_format:
            raise ValueError(f"Unsupported database type: {db_type}")

        # Format the engine string based on connection parameters
        engine_string = engine_format[db_type].format(
            user=connection_params.get('user', ''),
            password=connection_params.get('password', ''),
            host=connection_params.get('host', ''),
            port=connection_params.get('port', ''),
            database=connection_params.get('database', '')
        )
        
        # Create the SQLAlchemy engine
        engine = create_engine(engine_string)
        
        # Execute the query and fetch data into a DataFrame
        query = f'SELECT * FROM {connection_params['table']}'
        data = pd.read_sql(query, engine)
        return data

    except Exception as e:
        print(f"An error occurred while fetching data from {db_type}: {e}")
        return None
