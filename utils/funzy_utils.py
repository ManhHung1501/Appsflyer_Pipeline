from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from config.funzy_db import funzy_user, funzy_password, funzy_host, funzy_port, funzy_db

def connect_funzy_db():
    conn_str = f"mssql+pyodbc://{funzy_user}:{funzy_password}@{funzy_host}:{funzy_port}/{funzy_db}?driver=ODBC+Driver+17+for+SQL+Server"
    try:
        # Create the SQLAlchemy engine
        engine = create_engine(conn_str)
        connection = engine.connect()
        print("Connect to Funzy Database successful!")
        return connection 
    except SQLAlchemyError as e:
        print(f"Error connecting to database: {e}")
        return None


