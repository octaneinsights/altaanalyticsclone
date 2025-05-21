from dagster import asset, AssetExecutionContext, Output, AssetIn, AssetKey
import pandas as pd

@asset(
    group_name="fieldroutes_staging",
    compute_kind="Snowflake SQL",
    io_manager_key="snowflake_io",
    required_resource_keys={"snowflake_io"},
    deps=[AssetKey("customer_dim")]
)
def staging_customer_dim(context: AssetExecutionContext, snowflake_io):
    """Transform raw customer data to staging format"""
    
    # SQL transformation approach
    transformation_sql = """
    CREATE OR REPLACE TABLE staging.fieldroutes.customers AS 
    SELECT 
        id as customer_id,
        id as customer_key,  -- Create surrogate key
        office_id as office_key,
        region_id as region_key,
        source_id as customer_source_key,
        status,
        CASE WHEN commercial = 1 THEN TRUE ELSE FALSE END as commercial_flag,
        date_added as date_added,
        date_cancelled as date_cancelled,
        balance,
        latitude,
        longitude,
        address as address,
        city,
        state,
        zip_code as zip,
        current_timestamp() as staging_timestamp
    FROM raw.fieldroutes.customers
    """
    
    # Execute the transformation
    snowflake_io.execute_sql(transformation_sql)
    
    # Get count for metadata
    count_result = snowflake_io.execute_sql(
        "SELECT COUNT(*) FROM staging.fieldroutes.customers"
    )
    record_count = count_result[0][0] if count_result else 0
    
    return Output(
        value=record_count,
        metadata={
            "record_count": record_count,
            "schema": "staging",
            "table": "customers"
        }
    )

# Similar staging assets for other tables...
