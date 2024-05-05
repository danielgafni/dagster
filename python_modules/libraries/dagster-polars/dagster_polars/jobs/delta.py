from dagster_polars.ops.delta import optimize_deltalake_table_op

from dagster import job


@job(description="Optimize DeltaLake table by performing vacuum, compact or z_order")
def optimize_deltalake_table():
    optimize_deltalake_table_op()
