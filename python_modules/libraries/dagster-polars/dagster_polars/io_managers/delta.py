import json
from collections import defaultdict
from enum import Enum
from pprint import pformat
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

import polars as pl
from dagster import InputContext, MetadataValue, MultiPartitionKey, OutputContext
from dagster._annotations import experimental
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.storage.upath_io_manager import is_dict_type

from dagster_polars.io_managers.base import BasePolarsUPathIOManager
from dagster_polars.types import DataFrameWithMetadata, LazyFrameWithMetadata, StorageMetadata

try:
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError
except ImportError as e:
    raise ImportError("Install 'dagster-polars[deltalake]' to use DeltaLake functionality") from e

if TYPE_CHECKING:
    from upath import UPath


DAGSTER_POLARS_STORAGE_METADATA_SUBDIR = ".dagster_polars_metadata"

SINGLE_LOADING_TYPES = (pl.DataFrame, pl.LazyFrame, LazyFrameWithMetadata, DataFrameWithMetadata)


class DeltaWriteMode(str, Enum):
    error = "error"
    append = "append"
    overwrite = "overwrite"
    ignore = "ignore"


@experimental
class PolarsDeltaIOManager(BasePolarsUPathIOManager):
    """Implements writing and reading DeltaLake tables.

    Features:
     - All features provided by :py:class:`~dagster_polars.BasePolarsUPathIOManager`.
     - All read/write options can be set via corresponding metadata or config parameters (metadata takes precedence).
     - Supports native DeltaLake partitioning by storing different asset partitions in the same DeltaLake table.
       To enable this behavior, set the `partition_by` metadata value or config parameter **and** use a non-dict type annotation when loading the asset.
       The `partition_by` value will be used in `delta_write_options` of `pl.DataFrame.write_delta` and `pyarrow_options` of `pl.scan_detla`).
       When using a one-dimensional `PartitionsDefinition`, it should be a single string like "column`. When using a `MultiPartitionsDefinition`,
       it should be a dict with dimension to column names mapping, like `{"dimension": "column"}`.
     - Supports writing/reading custom metadata to/from `.dagster_polars_metadata/<version>.json` file in the DeltaLake table directory.

    Install `dagster-polars[delta]` to use this IOManager.

    Examples:
        .. code-block:: python

            from dagster import asset
            from dagster_polars import PolarsDeltaIOManager
            import polars as pl

            @asset(
                io_manager_key="polars_delta_io_manager",
                key_prefix=["my_dataset"]
            )
            def my_asset() -> pl.DataFrame:  # data will be stored at <base_dir>/my_dataset/my_asset.delta
                ...

            defs = Definitions(
                assets=[my_table],
                resources={
                    "polars_parquet_io_manager": PolarsDeltaIOManager(base_dir="s3://my-bucket/my-dir")
                }
            )


        Appending to a DeltaLake table:

        .. code-block:: python

            @asset(
                io_manager_key="polars_delta_io_manager",
                metadata={
                    "mode": "append"
                },
            )
            def my_table() -> pl.DataFrame:
                ...

        Using native DeltaLake partitioning by storing different asset partitions in the same DeltaLake table:

        .. code-block:: python

            from dagster import AssetExecutionContext, DailyPartitionedDefinition
            from dagster_polars import LazyFramePartitions

            @asset(
                io_manager_key="polars_delta_io_manager",
                metadata={
                    "partition_by": "partition_col"
                },
                partitions_def=StaticPartitionsDefinition(["a, "b", "c"])
            )
            def upstream(context: AssetExecutionContext) -> pl.DataFrame:
                df = ...

                # column with the partition_key must match `partition_by` metadata value
                return df.with_columns(pl.lit(context.partition_key).alias("partition_col"))

            @asset
            def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
                ...

        When using `MuiltiPartitionsDefinition`, `partition_by` metadata value should be a dictionary mapping dimensions to column names.

        .. code-block:: python

            from dagster import AssetExecutionContext, DailyPartitionedDefinition, MultiPartitionsDefinition, StaticPartitionsDefinition
            from dagster_polars import LazyFramePartitions

            @asset(
                io_manager_key="polars_delta_io_manager",
                metadata={
                    "partition_by": {"time": "date", "clients": "client"}  # dimension->column mapping
                },
                partitions_def=MultiPartitionsDefinition(
                    {
                        "date": DailyPartitionedDefinition(...),
                        "clients": StaticPartitionsDefinition(...)
                    }
                )
            )
            def upstream(context: AssetExecutionContext) -> pl.DataFrame:
                df = ...

                partition_keys_by_dimension = context.partition_key.keys_by_dimension

                return df.with_columns(
                    pl.lit(partition_keys_by_dimension["time"]).alias("date"),  # time dimension matches date column
                    pl.lit(partition_keys_by_dimension["clients"]).alias("client")  # clients dimension matches client column
                )


            @asset
            def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
                ...

    """

    extension: str = ".delta"
    mode: DeltaWriteMode = DeltaWriteMode.overwrite.value  # type: ignore
    overwrite_schema: bool = False
    version: Optional[int] = None
    vacuum_every: Optional[int] = Field(default=None, description="Run VACUUM over the table every Nth version. More info: https://delta-io.github.io/delta-rs/python/usage.html#vacuuming-tables")
    vacuum_retention_hours: Optional[int] = Field(default=None, description="The retention period in hours for vacuumed files. More info: https://delta-io.github.io/delta-rs/python/usage.html#vacuuming-tables")
    optimize_compact_every: Optional[int] = Field(default=None, description="Run OPTIMIZE over the table every Nth version. More info: https://delta-io.github.io/delta-rs/python/usage.html#optimizing-tables")
    optimize_z_order_every: Optional[int] = Field(default=None, description="Run OPTIMIZE ZORDER BY over the table every Nth version. More info: https://delta-io.github.io/delta-rs/python/usage.html#optimizing-tables")
    optimize_z_order_by: Optional[List[str]] = Field(default=None, description="Columns to optimize ZORDER BY. More info: https://delta-io.github.io/delta-rs/python/usage.html#optimizing-tables")

    def sink_df_to_path(
        self,
        context: OutputContext,
        df: pl.LazyFrame,
        path: "UPath",
        metadata: Optional[StorageMetadata] = None,
    ):
        context_metadata = context.definition_metadata or {}
        streaming = context_metadata.get("streaming", False)
        return self.write_df_to_path(context, df.collect(streaming=streaming), path, metadata)

    def write_df_to_path(
        self,
        context: OutputContext,
        df: pl.DataFrame,
        path: "UPath",
        metadata: Optional[StorageMetadata] = None,  # why is metadata passed
    ):
        context_metadata = context.definition_metadata or {}
        delta_write_options = context_metadata.get(
            "delta_write_options"
        )  # This needs to be gone and just only key value on the metadata

        if context.has_asset_partitions:
            delta_write_options = delta_write_options or {}
            partition_by = context_metadata.get(
                "partition_by"
            )  # this could be wrong, you could have partition_by in delta_write_options and in the metadata

            if partition_by is not None:
                assert (
                    context.partition_key is not None
                ), 'can\'t set "partition_by" for an asset without partitions'

                if isinstance(partition_by, dict) and isinstance(
                    context.partition_key, MultiPartitionKey
                ):
                    delta_write_options["partition_by"] = list(partition_by.values())
                elif isinstance(partition_by, str) and isinstance(context.partition_key, str):
                    delta_write_options["partition_by"] = partition_by
                else:
                    raise ValueError(
                        "partitio_by metadata value must be a string for single-partitioned assets or a dictionary for multi-partitioned assets"
                    )

                delta_write_options["partition_filters"] = self.get_partition_filters(context)

        if delta_write_options is not None:
            context.log.debug(f"Writing with delta_write_options: {pformat(delta_write_options)}")

        storage_options = self.storage_options

        df.write_delta(
            str(path),
            mode=context_metadata.get("mode") or self.mode.value,
            overwrite_schema=context_metadata.get("overwrite_schema") or self.overwrite_schema,
            storage_options=storage_options,
            delta_write_options=delta_write_options,
        )

        dt = DeltaTable(
            str(path), storage_options=storage_options, without_files=True
        )
        current_version = dt.version()
        context.add_output_metadata({"version": current_version})

        if metadata is not None:
            metadata_path = self.get_storage_metadata_path(path, current_version)
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            metadata_path.write_text(json.dumps(metadata))

        vacuum_every = context_metadata.get("vacuum_every") or self.vacuum_every
        if vacuum_every is not None and current_version % vacuum_every == 0:
            res = dt.vacuum(dry_run=False,
                        retention_hours=context_metadata.get("vacuum_retention_hours") or self.vacuum_retention_hours
            )
            context.log.info(f"Vacuumed DeltaLake table {path}:\n{res}")

        optimize_compact_every = context_metadata.get("optimize_compact_every") or self.optimize_compact_every
        if optimize_compact_every is not None and current_version % optimize_compact_every == 0:
            res = dt.optimize.compact()
            context.log.info(f"Compacted small files in DeltaLake table {path}:\n{res}")

        optimize_z_order_every = context_metadata.get("optimize_z_order_every") or self.optimize_z_order_every
        if optimize_z_order_every is not None and current_version % optimize_z_order_every == 0:
            optimize_z_order_by = context_metadata.get("optimize_z_order_by") or self.optimize_z_order_by
            res = dt.optimize.z_order(optimize_z_order_by)
            context.log.info(f"Perfromed ZORDER BY {optimize_z_order_by} in DeltaLake table {path}:\n{res}")

    @overload
    def scan_df_from_path(
        self, path: "UPath", context: InputContext, with_metadata: Literal[None, False]
    ) -> pl.LazyFrame: ...

    @overload
    def scan_df_from_path(
        self, path: "UPath", context: InputContext, with_metadata: Literal[True]
    ) -> LazyFrameWithMetadata: ...

    def scan_df_from_path(
        self,
        path: "UPath",
        context: InputContext,
        with_metadata: Optional[bool] = False,
    ) -> Union[pl.LazyFrame, LazyFrameWithMetadata]:
        """This method scans a DeltaLake table into a `polars.LazyFrame`.
        It can be called in 3 different situations:
        1. with an unpartitioned asset
        2. with a partitioned asset without native partitioning enabled - multiple times on nested .delta tables
        3. with a partitioned asset and with native partitioning enabled - a single time on the .delta table.

        In the (3) optin we apply partition filters to only load mapped partitions
        """
        assert context.upstream_output is not None
        assert context.upstream_output.definition_metadata is not None

        context_metadata = context.definition_metadata or {}

        version = self.get_delta_version_to_load(path, context)

        context.log.debug(f"Reading Delta table with version: {version}")

        pyarrow_options = context_metadata.get("pyarrow_options", {})

        partition_by = context.upstream_output.definition_metadata.get("partition_by")

        # we want to apply partition filters when loading some partitions, but not all partitions
        if (
            partition_by
            and len(context.asset_partition_keys) > 0
            and context.upstream_output.asset_info is not None
            and context.upstream_output.asset_info.partitions_def is not None
            and context.asset_partition_keys
            != set(
                context.upstream_output.asset_info.partitions_def.get_partition_keys(
                    dynamic_partitions_store=context.instance
                )
            )
        ):
            pyarrow_options["partitions"] = self.get_partition_filters(context)

        if pyarrow_options:
            context.log.debug(f"Reading with pyarrow_options: {pyarrow_options}")

        delta_table_options = context_metadata.get("delta_table_options")

        if delta_table_options:
            context.log.debug("Reading with delta_table_optilns: {delta_table_options}")

        ldf = pl.scan_delta(
            str(path),
            version=version,
            delta_table_options=delta_table_options,
            pyarrow_options=pyarrow_options,
            storage_options=self.storage_options,
        )

        if with_metadata:
            version = self.get_delta_version_to_load(path, context)
            metadata_path = self.get_storage_metadata_path(path, version)
            if metadata_path.exists():
                metadata = json.loads(metadata_path.read_text())
            else:
                metadata = {}
            return ldf, metadata

        else:
            return ldf

    def load_partitions(self, context: InputContext):
        assert context.upstream_output is not None
        # any partition would work as they all are stored in the same DeltaLake table
        path = self._get_path_without_extension(context)
        if context.upstream_output.definition_metadata.get("partition_by") and not is_dict_type(
            context.dagster_type.typing_type
        ):
            # user enabled native partitioning and wants a `pl.DataFrame` or `pl.LazyFrame`
            return self.load_from_path(context, self._with_extension(path))
        else:
            # default behaviour
            return super().load_partitions(context)

    def get_path_for_partition(
        self, context: Union[InputContext, OutputContext], path: "UPath", partition: str
    ) -> "UPath":
        if isinstance(context, InputContext):
            if (
                context.upstream_output is not None
                and context.upstream_output.definition_metadata is not None
                and context.upstream_output.definition_metadata.get("partition_by") is not None
            ):
                # upstream asset has "partition_by" metadata set, so partitioning for it is handled by DeltaLake itself
                return path

        if isinstance(context, OutputContext):
            if (
                context.definition_metadata is not None
                and context.definition_metadata.get("partition_by") is not None
            ):
                # this asset has "partition_by" metadata set, so partitioning for it is handled by DeltaLake itself
                return path

        return path / partition  # partitioning is handled by the IOManager

    @staticmethod
    def get_partition_filters(
        context: Union[InputContext, OutputContext],
    ) -> Sequence[Tuple[str, str, Any]]:
        if isinstance(context, OutputContext):
            partition_by = context.definition_metadata.get("partition_by")
        elif isinstance(context, InputContext) and context.upstream_output is not None:
            partition_by = context.upstream_output.definition_metadata.get("partition_by")
        else:
            raise DagsterInvariantViolationError("Invalid context type: type(context)")

        if partition_by is None or not context.has_asset_partitions:
            filters = []
        elif isinstance(partition_by, dict):
            all_keys_by_dim = defaultdict(list)
            for partition_key in context.asset_partition_keys:
                assert isinstance(
                    partition_key, MultiPartitionKey
                ), f"received dict `partition_by` metadata value {partition_by}, but the partition_key is not a `MultiPartitionKey`: {partition_key}"
                for dim, key in partition_key.keys_by_dimension.items():
                    all_keys_by_dim[dim].append(key)

            filters = [(partition_by[dim], "in", keys) for dim, keys in all_keys_by_dim.items()]

        elif isinstance(partition_by, str):
            assert not isinstance(
                context.asset_partition_keys[0], MultiPartitionKey
            ), f"receiveds string `partition_by` metadata value {partition_by}, but the partition_key is not a `MultiPartitionKey`: {context.asset_partition_keys[0]}"
            filters = [(partition_by, "in", context.asset_partition_keys)]

        else:
            raise NotImplementedError("Unsupported `partitio_by` metadata value: {partition_by}")

        return filters

    def get_metadata(
        self, context: OutputContext, obj: Union[pl.DataFrame, pl.LazyFrame, None]
    ) -> Dict[str, MetadataValue]:
        context_metadata = context.definition_metadata or {}

        metadata = super().get_metadata(context, obj)

        if context.has_asset_partitions:
            partition_by = context_metadata.get("partition_by")
            if partition_by is not None:
                metadata["partition_by"] = partition_by

        if context_metadata.get("mode") == "append":
            # modify the medatata to reflect the fact that we are appending to the table

            if context.has_asset_partitions:
                # paths = self._get_paths_for_partitions(context)
                # assert len(paths) == 1
                # path = list(paths.values())[0]

                # FIXME: what to about row_count metadata do if we are appending to a partitioned table?
                # we should not be using the full table length,
                # but it's unclear how to get the length of the partition we are appending to
                pass
            else:
                metadata["append_row_count"] = metadata["row_count"]

                path = self._get_path(context)
                # we need to get row_count from the full table
                metadata["row_count"] = MetadataValue.int(
                    DeltaTable(str(path), storage_options=self.storage_options)
                    .to_pyarrow_dataset()
                    .count_rows()
                )

        return metadata

    def get_delta_version_to_load(self, path: "UPath", context: InputContext) -> int:
        context_metadata = context.definition_metadata or {}
        version_from_metadata = context_metadata.get("version")

        version_from_config = self.version

        version: Optional[int] = None

        if version_from_metadata is not None and version_from_config is not None:
            context.log.warning(
                f"Both version from metadata ({version_from_metadata}) "
                f"and config ({version_from_config}) are set. Using version from metadata."
            )
            version = int(version_from_metadata)
        elif version_from_metadata is None and version_from_config is not None:
            version = int(version_from_config)
        elif version_from_metadata is not None and version_from_config is None:
            version = int(version_from_metadata)

        if version is None:
            return DeltaTable(
                str(path), storage_options=self.storage_options, without_files=True
            ).version()
        else:
            return version

    def get_storage_metadata_path(self, path: "UPath", version: int) -> "UPath":
        return path / DAGSTER_POLARS_STORAGE_METADATA_SUBDIR / f"{version}.json"
