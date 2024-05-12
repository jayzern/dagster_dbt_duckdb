from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from pathlib import Path

from ..resources import dbt_resource
from ..partitions import daily_partition
import json
import os

from .constants import DBT_DIRECTORY

# dbt_resource.cli(["--quiet", "parse"], target_path=Path("target")).wait()
# dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")

# dbt_manifest_path = (
#     dbt_resource.cli(
#         ["--quiet", "parse"],
#         target_path=Path("target"),
#     )
#     .wait()
#     .target_path.joinpath("manifest.json")
# )

INCREMENTAL_SELECTOR = "config.materialized:incremental"

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source":
            return AssetKey(f"taxi_{name}")
        else:
            return super().get_asset_key(dbt_resource_props)
    
    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1]


dbt_resource.cli(["--quiet", "parse"]).wait()

if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(["--quiet", "parse"]).wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")

@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude=INCREMENTAL_SELECTOR, # Add this here
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# For incremental partitioning
@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select=INCREMENTAL_SELECTOR,     # select only models with INCREMENTAL_SELECTOR
    partitions_def=daily_partition   # partition those models using daily_partition
)
def incremental_dbt_models(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):
    time_window = context.partition_time_window
    dbt_vars = {
        "min_date": time_window.start.strftime('%Y-%m-%d'),
        "max_date": time_window.end.strftime('%Y-%m-%d')
    }
    # yield from dbt.cli(["build"], context=context).stream()
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()
