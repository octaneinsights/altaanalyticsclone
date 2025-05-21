from dagster import AssetExecutionContext, asset
import pandas as pd
from datetime import datetime

def process_entity(
    context: AssetExecutionContext,
    field_routes_client,
    snowflake_io,
    field_routes_config,
    entity_name,
    database="raw",
    schema="fieldroutes",
    table=None,
    incremental=True,
    predict_small_dataset=False
):
    """Common processing logic for FieldRoutes entities"""
    if table is None:
        table = entity_name.lower()
        
    # Get all offices
    all_offices = field_routes_config.get_all_offices()
    
    all_records = []
    run_time = datetime.utcnow()
    
    # Process each office
    for creds, metadata in all_offices:
        context.log.info(f"Processing {entity_name} for office {creds.office_id}")
        
        # Get time window for incremental load
        if incremental:
            time_window = metadata.get_window(run_time)
        else:
            # For full refresh, don't use time filters
            time_window = {}
            
        try:
            # Extract the data
            records = field_routes_client.extract_entity(
                creds, 
                entity_name, 
                time_window,
                predict_size=predict_small_dataset
            )
            
            # Add metadata
            for record in records:
                record["_office_id"] = creds.office_id
                record["_extract_timestamp"] = run_time.isoformat()
                
            all_records.extend(records)
            
            # Update last successful run
            if incremental:
                field_routes_config.update_last_run(
                    creds.office_id, 
                    time_window["end_datetime"]
                )
                
        except Exception as e:
            context.log.error(
                f"Failed to process {entity_name} for office {creds.office_id}: {str(e)}"
            )
            raise
            
    # Convert to DataFrame
    if all_records:
        df = pd.DataFrame(all_records)
        
        # Save to Snowflake
        result = snowflake_io.load_dataframe(
            df, 
            database, 
            schema, 
            table, 
            mode="append" if incremental else "overwrite"
        )
        
        context.log.info(
            f"Loaded {result['rows_loaded']} {entity_name} records to {database}.{schema}.{table}"
        )
    else:
        context.log.info(f"No {entity_name} records found to load")
        
    return len(all_records)
