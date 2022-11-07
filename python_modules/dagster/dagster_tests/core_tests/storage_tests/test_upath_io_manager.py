import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest
from upath import UPath

from dagster import (
    AllPartitionMapping,
    AssetIn,
    DagsterType,
    DailyPartitionsDefinition,
    Field,
    HourlyPartitionsDefinition,
    InitResourceContext,
    InputContext,
    OpExecutionContext,
    OutputContext,
    StaticPartitionsDefinition,
    asset,
    build_init_resource_context,
    build_input_context,
    build_output_context,
    io_manager,
    materialize,
)
from dagster._check import CheckError
from dagster._core.definitions import build_assets_job
from dagster._core.storage.upath_io_manager import UPathIOManager


class DummyIOManager(UPathIOManager):
    """
    This IOManager simply outputs the object path without loading or writing anything
    """

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath):
        pass

    def load_from_path(self, context: InputContext, path: UPath) -> str:
        return str(path)


@pytest.fixture
def dummy_io_manager(tmp_path: Path) -> DummyIOManager:
    @io_manager(config_schema={"base_path": Field(str, is_required=False)})
    def dummy_io_manager(init_context: InitResourceContext):
        assert init_context.instance is not None
        base_path = UPath(
            init_context.resource_config.get("base_path", init_context.instance.storage_directory())
        )
        return DummyIOManager(base_path=base_path)

    io_manager_def = dummy_io_manager.configured({"base_path": str(tmp_path)})

    return io_manager_def


@pytest.fixture
def start():
    return datetime(2022, 1, 1)


@pytest.fixture
def hourly(start: datetime):
    return HourlyPartitionsDefinition(start_date=f"{start:%Y-%m-%d-%H:%M}")


@pytest.fixture
def daily(start: datetime):
    return DailyPartitionsDefinition(start_date=f"{start:%Y-%m-%d}")


@pytest.mark.parametrize("json_data", [0, 0.0, [0, 1, 2], {"a": 0}, [{"a": 0}, {"b": 1}, {"c": 2}]])
def test_upath_io_manager_with_json(tmp_path: Path, json_data: Any):
    class JSONIOManager(UPathIOManager):
        extension: str = ".json"

        def dump_to_path(self, context: OutputContext, obj: Any, path: UPath):
            with path.open("w") as file:
                json.dump(obj, file)

        def load_from_path(self, context: InputContext, path: UPath) -> Any:
            with path.open("r") as file:
                return json.load(file)

    @io_manager(config_schema={"base_path": Field(str, is_required=False)})
    def json_io_manager(init_context: InitResourceContext):
        assert init_context.instance is not None
        base_path = UPath(
            init_context.resource_config.get("base_path", init_context.instance.storage_directory())
        )
        return JSONIOManager(base_path=base_path)

    manager = json_io_manager(build_init_resource_context(config={"base_path": str(tmp_path)}))
    context = build_output_context(
        name="abc",
        step_key="123",
        dagster_type=DagsterType(type_check_fn=lambda _, value: True, name="any", typing_type=Any),
    )
    manager.handle_output(context, json_data)

    with manager._get_path(context).open("r") as file:  # pylint: disable=W0212
        assert json.load(file) == json_data

    context = build_input_context(
        name="abc",
        upstream_output=context,
        dagster_type=DagsterType(type_check_fn=lambda _, value: True, name="any", typing_type=Any),
    )
    assert manager.load_input(context) == json_data


def test_upath_io_manager_with_pandas_csv(tmp_path: Path):
    class PandasCSVIOManager(UPathIOManager):
        extension: str = ".csv"

        def dump_to_path(self, context: OutputContext, obj: pd.DataFrame, path: UPath):
            with path.open("wb") as file:
                obj.to_csv(file, index=False)

        def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
            with path.open("rb") as file:
                return pd.read_csv(file)

    @io_manager(config_schema={"base_path": Field(str, is_required=False)})
    def pandas_csv_io_manager(init_context: InitResourceContext):
        assert init_context.instance is not None
        base_path = UPath(
            init_context.resource_config.get("base_path", init_context.instance.storage_directory())
        )
        return PandasCSVIOManager(base_path=base_path)

    manager = pandas_csv_io_manager(
        build_init_resource_context(config={"base_path": str(tmp_path)})
    )

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})

    context = build_output_context(
        name="abc",
        step_key="123",
        dagster_type=DagsterType(
            type_check_fn=lambda _, value: isinstance(value, pd.DataFrame),
            name="pandas.DataFrame",
            typing_type=pd.DataFrame,
        ),
    )
    manager.handle_output(context, df)

    with manager._get_path(context).open("rb") as file:  # pylint: disable=W0212
        pd.testing.assert_frame_equal(df, pd.read_csv(file))

    context = build_input_context(
        name="abc",
        upstream_output=context,
        dagster_type=DagsterType(
            type_check_fn=lambda _, value: isinstance(value, pd.DataFrame),
            name="pandas.DataFrame",
            typing_type=pd.DataFrame,
        ),
    )
    pd.testing.assert_frame_equal(manager.load_input(context), df)


def test_upath_io_manager_multiple_time_partitions(
    daily: DailyPartitionsDefinition,
    hourly: HourlyPartitionsDefinition,
    start: datetime,
    dummy_io_manager: DummyIOManager,
):
    @asset(partitions_def=hourly)
    def upstream_asset(context: OpExecutionContext) -> str:
        return context.partition_key

    @asset(
        partitions_def=daily,
    )
    def downstream_asset(upstream_asset: Dict[str, str]) -> Dict[str, str]:
        return upstream_asset

    result = materialize(
        [*upstream_asset.to_source_assets(), downstream_asset],
        partition_key=start.strftime(daily.fmt),
        resources={"io_manager": dummy_io_manager},
    )
    downstream_asset_data = result.output_for_node("downstream_asset", "result")
    assert len(downstream_asset_data) == 24, "downstream day should map to upstream 24 hours"


def test_upath_io_manager_multiple_static_partitions(dummy_io_manager: DummyIOManager):  # type: ignore[no-redef]
    upstream_partitions_def = StaticPartitionsDefinition(["A", "B"])

    @asset(partitions_def=upstream_partitions_def)
    def upstream_asset(context: OpExecutionContext) -> str:
        return context.partition_key

    @asset(ins={"upstream_asset": AssetIn(partition_mapping=AllPartitionMapping())})
    def downstream_asset(upstream_asset: Dict[str, str]) -> Dict[str, str]:
        return upstream_asset

    my_job = build_assets_job(
        "my_job",
        assets=[upstream_asset, downstream_asset],
        resource_defs={"io_manager": dummy_io_manager},  # type: ignore[dict-item]
    )
    result = my_job.execute_in_process(partition_key="A")
    downstream_asset_data = result.output_for_node("downstream_asset", "result")
    assert list(downstream_asset_data.keys()) == ["A", "B"]


def test_partitioned_io_manager_preserves_single_partition_dependency(
    daily: DailyPartitionsDefinition, dummy_io_manager: DummyIOManager
):
    @asset(partitions_def=daily)
    def upstream_asset():
        return 42

    @asset(partitions_def=daily)
    def daily_asset(upstream_asset: str):
        return upstream_asset

    result = materialize(
        [upstream_asset, daily_asset],
        partition_key="2022-01-01",
        resources={"io_manager": dummy_io_manager},
    )
    assert result.output_for_node("daily_asset").endswith("2022-01-01")


def test_user_forgot_dict_type_annotation_for_multiple_partitions(
    start: datetime,
    daily: DailyPartitionsDefinition,
    hourly: HourlyPartitionsDefinition,
    dummy_io_manager: DummyIOManager,
):
    @asset(partitions_def=hourly)
    def upstream_asset(context: OpExecutionContext) -> str:
        return context.partition_key

    @asset(
        partitions_def=daily,
    )
    def downstream_asset(upstream_asset: str) -> str:
        return upstream_asset

    with pytest.raises(
        CheckError,
        match=r".* If you are loading multiple partitions, the upstream asset type annotation "
        "should be a typing.Dict.",
    ):
        materialize(
            [*upstream_asset.to_source_assets(), downstream_asset],
            partition_key=start.strftime(daily.fmt),
            resources={"io_manager": dummy_io_manager},
        )
