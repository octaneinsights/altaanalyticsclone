from dagster import asset, AssetExecutionContext, Output, AssetIn, AssetKey
from .common import process_entity

@asset(
    group_name="fieldroutes_dimensions",
    compute_kind="FieldRoutes API",
    io_manager_key="snowflake_io",
    required_resource_keys={"field_routes_client", "snowflake_io", "field_routes_config"}
)
def customer_dim(context: AssetExecutionContext, field_routes_client, snowflake_io, field_routes_config):
    """Extract Customer dimension from FieldRoutes"""
    record_count = process_entity(
        context,
        field_routes_client,
        snowflake_io,
        field_routes_config,
        "customer",
        database="raw",
        schema="fieldroutes",
        table="customers",
        incremental=True,
        predict_small_dataset=True  # Per playbook, customers usually < 1000 per day
    )
    
    return Output(
        value=record_count,
        metadata={
            "record_count": record_count,
            "schema": "fieldroutes",
            "table": "customers"
        }
    )
    
@asset(
    group_name="fieldroutes_dimensions",
    compute_kind="FieldRoutes API",
    io_manager_key="snowflake_io",
    required_resource_keys={"field_routes_client", "snowflake_io", "field_routes_config"}
)
def employee_dim(context: AssetExecutionContext, field_routes_client, snowflake_io, field_routes_config):
    """Extract Employee dimension from FieldRoutes"""
    record_count = process_entity(
        context,
        field_routes_client,
        snowflake_io,
        field_routes_config,
        "employee",
        database="raw",
        schema="fieldroutes",
        table="employees",
        incremental=False,  # Full refresh fine for low volume
        predict_small_dataset=True
    )
    
    return Output(
        value=record_count,
        metadata={
            "record_count": record_count,
            "schema": "fieldroutes",
            "table": "employees"
        }
    )

@asset(
    group_name="fieldroutes_dimensions",
    compute_kind="FieldRoutes API",
    io_manager_key="snowflake_io",
    required_resource_keys={"field_routes_client", "snowflake_io", "field_routes_config"}
)
def office_dim(context: AssetExecutionContext, field_routes_client, snowflake_io, field_routes_config):
    """Extract Office dimension from FieldRoutes"""
    record_count = process_entity(
        context,
        field_routes_client,
        snowflake_io,
        field_routes_config,
        "office",
        database="raw",
        schema="fieldroutes",
        table="offices",
        incremental=False,  # Always full refresh for 17 static records
        predict_small_dataset=True
    )
    
    return Output(
        value=record_count,
        metadata={
            "record_count": record_count,
            "schema": "fieldroutes",
            "table": "offices"
        }
    )

# Similar asset functions for other dimensions:
# - region_dim
# - service_type_dim
# - product_dim
# - payment_method_dim
# ... etc. for all dims in the schema
