import psycopg2
import pandas as pd
from typing import List, Dict, Any
from config.database import DatabaseConfig


class PostgresExtractor:
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.connection = None
    
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.username,
                password=self.db_config.password
            )
            print(f"Connected to database: {self.db_config.database}")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from database")
    
    def extract_table(self, table_name: str, limit: int = None) -> pd.DataFrame:
        if not self.connection:
            self.connect()
        
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            df = pd.read_sql_query(query, self.connection)
            print(f"Extracted {len(df)} rows from {table_name}")
            return df
        except Exception as e:
            print(f"Error extracting from {table_name}: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.connection:
            self.connect()
        
        try:
            df = pd.read_sql_query(query, self.connection)
            print(f"Query executed successfully, returned {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        if not self.connection:
            self.connect()
        
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            
            table_info = []
            for col in columns:
                table_info.append({
                    'column_name': col[0],
                    'data_type': col[1],
                    'is_nullable': col[2],
                    'column_default': col[3]
                })
            
            cursor.close()
            return table_info
        except Exception as e:
            print(f"Error getting table info for {table_name}: {e}")
            raise