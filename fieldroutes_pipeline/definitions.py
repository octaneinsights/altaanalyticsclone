from dagster import Definitions, define_asset_job, AssetSelection, ScheduleDefinition

from .assets.dimensions import (
    customer_dim, employee_dim, office_dim,
    # Import other dimension assets
)
from .assets.facts import (
    appointment_fact, subscription_fact, payment_fact,
    # Import other fact assets
)

from .assets.config import FieldRoutesConfig
from .resources.fieldroutes_client import FieldRoutesClient
from .resources.snowflake_io import SnowflakeIO

# Define jobs that group assets
# 1. All dimensions - can run more frequently or first
dimension_assets = AssetSelection.groups("fieldroutes_dimensions")
dimension_job = define_asset_job(
    name="fieldroutes_load_dimensions",
    selection=dimension_assets
)

# 2. All facts - depends on dimensions
fact_assets = AssetSelection.groups("fieldroutes_facts")
fact_job = define_asset_job(
    name="fieldroutes_load_facts",
    selection=fact_assets
)

# 3. All staging transformations
staging_assets = AssetSelection.groups("fieldroutes_staging")
staging_job = define_asset_job(
    name="fieldroutes_transform_staging",
    selection=staging_assets
)

# Define schedules
nightly_schedule = ScheduleDefinition(
    job=fact_job,
    cron_schedule="0 1 * * *",  # 1 AM daily
    execution_timezone="America/Denver"
)

# Hot tables could get an hourly schedule
hourly_hot_tables = ScheduleDefinition(
    job=define_asset_job(
        name="fieldroutes_hot_tables",
        selection=AssetSelection.assets(appointment_fact, payment_fact)
    ),
    cron_schedule="5 * * * *",  # 5 minutes past every hour
    execution_timezone="America/Denver"
)

defs = Definitions(
    assets=[
        # All assets
        customer_dim, employee_dim, office_dim,
        appointment_fact, subscription_fact, payment_fact,
        # Add all other assets here
    ],
    resources={
        "field_routes_client": FieldRoutesClient(),
        "snowflake_io": SnowflakeIO(),
        "field_routes_config": FieldRoutesConfig()
    },
    schedules=[nightly_schedule, hourly_hot_tables],
    jobs=[dimension_job, fact_job, staging_job]
)
