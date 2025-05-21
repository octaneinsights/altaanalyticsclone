from dagster import asset, AssetExecutionContext, Output, AssetIn, AssetKey
from .common import process_entity

# Define dependencies on dimension tables
dimension_dependencies = [
    AssetKey("customer_dim"),
    AssetKey("employee_dim"),
    AssetKey("office_dim"),
    AssetKey("service_type_dim")
]

@asset(
    group_name="fieldroutes_facts",
    compute_kind="FieldRoutes API",
    io_manager_key="snowflake_io",
    required_resource_keys={"field_routes_client", "snowflake_io", "field_routes_config"},
    deps=dimension_dependencies
)
def appointment_fact(context: AssetExecutionContext, field_routes_client, snowflake_io, field_routes_config):
    """Extract Appointment fact from FieldRoutes"""
    record_count = process_entity(
        context,
        field_routes_client,
        snowflake_io,
        field_routes_config,
        "appointment",
        database="raw",
        schema="fieldroutes",
        table="appointments",
        incremental=True,
        predict_small_dataset=False  # High volume, do not use includeData
    )
    
    return Output(
        value=record_count,
        metadata={
            "record_count": record_count,
            "schema": "fieldroutes",
            "table": "appointments"
        }
    )

@asset(
    group_name="fieldroutes_facts",
    compute_kind="FieldRoutes API",
    io_manager_key="snowflake_io",
    required_resource_keys={"field_routes_client", "snowflake_io", "field_routes_config"},
    deps=dimension_dependencies
)
def subscription_fact(context: AssetExecutionContext, field_routes_client, snowflake_io, field_routes_config):
    """Extract Subscription fact from FieldRoutes"""
    record_count = process_entity(
        context,
        field_routes_client,
        snowflake_io,
        field_routes_config,
        "subscription",
        database="raw",
        schema="fieldroutes",
        table="subscriptions",
        incremental=True,
        predict_small_dataset=False
    )
    
    return Output(
        value=record_count,
        metadata={
            "record_count": record_count,
            "schema": "fieldroutes",
            "table": "subscriptions"
        }
    )

@asset(
    group_name="fieldroutes_facts",
    compute_kind="FieldRoutes API",
    io_manager_key="snowflake_io",
    required_resource_keys={"field_routes_client", "snowflake_io", "field_routes_config"},
    deps=dimension_dependencies
)
def payment_fact(context: AssetExecutionContext, field_routes_client, snowflake_io, field_routes_config):
    """Extract Payment fact from FieldRoutes"""
    record_count = process_entity(
        context,
        field_routes_client,
        snowflake_io,
        field_routes_config,
        "payment",
        database="raw",
        schema="fieldroutes",
        table="payments",
        incremental=True,
        predict_small_dataset=False  # High volume per playbook
    )
    
    return Output(
        value=record_count,
        metadata={
            "record_count": record_count,
            "schema": "fieldroutes",
            "table": "payments"
        }
    )

# Similar asset functions for other facts:
# - ticket_fact
# - ticket_item_fact
# - applied_payment_fact
# - route_fact
# - note_fact
# - task_fact
# ... etc. for all facts in the schema
