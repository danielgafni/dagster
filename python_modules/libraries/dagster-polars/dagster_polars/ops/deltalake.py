from dagster import op, Config, OpExecutionContext
from deltalake import DeltaTable


class OptmizeDeltaLakeTableConfig(Config):
    path: str
    vacuum: bool = False
    compact: bool = False
    z_order_by: Optional[List[str]] = None


@op(name="optimize_deltalake_table")
def optimize_deltalake_table_op(context: OpExecutionContext, config: OptmizeDeltalakeTableConfig):
    table = DeltaTable.forPath(context.resources.spark, config.path)
    
    z_order_by = config.z_order_by or []
    
    if config.vacuum:
        context.log.info(f"Vacuuming DeltaLake table {config.path}")
        table.vacuum(dry_run=False)

    if config.compact:
        context.log.info(f"Compacting small files in DeltaLake table {config.path}")
        table.optimize.compact()

    if z_order_by:
        context.log.info(f"Perofrming z_order in DeltaLake table {config.path} by {z_order_by}")
        table.optimize.z_order(z_order_by)
