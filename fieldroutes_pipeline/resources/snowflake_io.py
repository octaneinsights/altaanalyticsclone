import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dagster import ConfigurableResource, Field, StringSource

class SnowflakeIO(ConfigurableResource):
    """Resource for interacting with Snowflake"""
    account: str = Field(default=None, description="Snowflake account")
    user: str = Field(default=None, description="Snowflake username")
    password: str = Field(default=StringSource, description="Snowflake password")
    warehouse: str = Field(default="ALTA_COMPUTE_WH", description="Snowflake warehouse")
    role: str = Field(default="ALTA_ETL_ROLE", description="Snowflake role")
    
    def get_connection(self, database=None):
        """Get a Snowflake connection"""
        conn_params = {
            "user": self.user,
            "password": self.password,
            "account": self.account,
            "warehouse": self.warehouse,
            "role": self.role
        }
        
        if database:
            conn_params["database"] = database
            
        return connect(**conn_params)
    
    def create_database_if_not_exists(self, database):
        """Create a database if it doesn't exist"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        finally:
            conn.close()
    
    def create_schema_if_not_exists(self, database, schema):
        """Create a schema if it doesn't exist"""
        conn = self.get_connection(database)
        try:
            cursor = conn.cursor()
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
        finally:
            conn.close()
    
    def execute_sql(self, sql, database=None, params=None):
        """Execute a SQL statement"""
        conn = self.get_connection(database)
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params or {})
            return cursor.fetchall()
        finally:
            conn.close()
    
    def load_dataframe(self, df, database, schema, table, mode="overwrite"):
        """Load a pandas DataFrame to Snowflake"""
        conn = self.get_connection(database)
        
        try:
            # Ensure the schema exists
            self.create_schema_if_not_exists(database, schema)
            
            # Handle mode (overwrite or append)
            if mode == "overwrite":
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE IF EXISTS {database}.{schema}.{table}")
                cursor.close()
            
            # Load the data
            success, num_chunks, num_rows, output = write_pandas(
                conn=conn,
                df=df,
                table_name=table,
                database=database,
                schema=schema,
                auto_create_table=True
            )
            
            return {
                "success": success,
                "rows_loaded": num_rows,
                "chunks": num_chunks
            }
            
        finally:
            conn.close()
            
    def run_merge(self, database, schema, target_table, source_table, join_keys, update_columns):
        """Run a Snowflake MERGE operation for SCD-1 updates"""
        join_condition = " AND ".join([f"target.{k} = source.{k}" for k in join_keys])
        update_clause = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
        insert_columns = ", ".join(join_keys + update_columns)
        insert_values = ", ".join([f"source.{col}" for col in join_keys + update_columns])
        
        merge_sql = f"""
        MERGE INTO {database}.{schema}.{target_table} AS target
        USING {database}.{schema}.{source_table} AS source
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
        
        self.execute_sql(merge_sql, database)