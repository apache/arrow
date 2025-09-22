# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any, TypedDict, TypeVar

import numpy as np
import pandas as pd

from pandas import DatetimeTZDtype

from .lib import Array, DataType, Schema, Table

_T = TypeVar("_T")


def get_logical_type_map() -> dict[int, str]: ...
def get_logical_type(arrow_type: DataType) -> str: ...
def get_numpy_logical_type_map() -> dict[type[np.generic], str]: ...
def get_logical_type_from_numpy(pandas_collection) -> str: ...
def get_extension_dtype_info(column) -> tuple[str, dict[str, Any]]: ...


class _ColumnMetadata(TypedDict):
    name: str
    field_name: str
    pandas_type: int
    numpy_type: str
    metadata: dict | None


def get_column_metadata(
    column: pd.Series | pd.Index, name: str, arrow_type: DataType, field_name: str
) -> _ColumnMetadata: ...


def construct_metadata(
    columns_to_convert: list[pd.Series],
    df: pd.DataFrame,
    column_names: list[str],
    index_levels: list[pd.Index],
    index_descriptors: list[dict],
    preserve_index: bool,
    types: list[DataType],
    column_field_names: list[str] = ...,
) -> dict[bytes, bytes]: ...


def dataframe_to_types(
    df: pd.DataFrame, preserve_index: bool | None, columns: list[str] | None = None
) -> tuple[list[str], list[DataType], dict[bytes, bytes]]: ...


def dataframe_to_arrays(
    df: pd.DataFrame,
    schema: Schema,
    preserve_index: bool | None,
    nthreads: int = 1,
    columns: list[str] | None = None,
    safe: bool = True,
) -> tuple[Array, Schema, int]: ...
def get_datetimetz_type(values: _T, dtype, type_) -> tuple[_T, DataType]: ...
def make_datetimetz(unit: str, tz: str) -> DatetimeTZDtype: ...


def table_to_dataframe(
    options,
    table: Table,
    categories=None,
    ignore_metadata: bool = False,
    types_mapper=None) -> pd.DataFrame: ...


def make_tz_aware(series: pd.Series, tz: str) -> pd.Series: ...
