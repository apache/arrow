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

from typing import NamedTuple

from .array import *  # noqa: F401, F403
from .builder import *  # noqa: F401, F403
from .compat import *  # noqa: F401, F403
from .config import *  # noqa: F401, F403
from .device import *  # noqa: F401, F403
from .error import *  # noqa: F401, F403
from .io import *  # noqa: F401, F403
from ._ipc import *  # noqa: F401, F403
from .memory import *  # noqa: F401, F403
from .pandas_shim import *  # noqa: F401, F403
from .scalar import *  # noqa: F401, F403
from .table import *  # noqa: F401, F403
from .tensor import *  # noqa: F401, F403
from ._types import *  # noqa: F401, F403


class MonthDayNano(NamedTuple):
    days: int
    months: int
    nanoseconds: int


def cpu_count() -> int: ...


def set_cpu_count(count: int) -> None: ...


def is_threading_enabled() -> bool: ...


def arange(
    start: int, stop: int, step: int = 1, *, memory_pool: MemoryPool | None = None
) -> Array: ...


def is_boolean_value(obj: object) -> bool: ...


def is_integer_value(obj: object) -> bool: ...


def is_float_value(obj: object) -> bool: ...


def tzinfo_to_string(tz: object) -> str: ...


def string_to_tzinfo(tz: str) -> object: ...


def _ndarray_to_arrow_type(values: object, type_: object) -> object: ...


def _is_primitive(type_id: int) -> bool: ...


def ensure_type(ty: object) -> DataType: ...


Type_NA: int
Type_BOOL: int
Type_UINT8: int
Type_INT8: int
Type_UINT16: int
Type_INT16: int
Type_UINT32: int
Type_INT32: int
Type_UINT64: int
Type_INT64: int
Type_HALF_FLOAT: int
Type_FLOAT: int
Type_DOUBLE: int
Type_DECIMAL32: int
Type_DECIMAL64: int
Type_DECIMAL128: int
Type_DECIMAL256: int
Type_DATE32: int
Type_DATE64: int
Type_TIMESTAMP: int
Type_TIME32: int
Type_TIME64: int
Type_DURATION: int
Type_INTERVAL_MONTHS: int
Type_INTERVAL_DAY_TIME: int
Type_INTERVAL_MONTH_DAY_NANO: int
Type_BINARY: int
Type_STRING: int
Type_LARGE_BINARY: int
Type_LARGE_STRING: int
Type_FIXED_SIZE_BINARY: int
Type_BINARY_VIEW: int
Type_STRING_VIEW: int
Type_LIST: int
Type_LARGE_LIST: int
Type_LIST_VIEW: int
Type_LARGE_LIST_VIEW: int
Type_MAP: int
Type_FIXED_SIZE_LIST: int
Type_STRUCT: int
Type_SPARSE_UNION: int
Type_DENSE_UNION: int
Type_DICTIONARY: int
Type_RUN_END_ENCODED: int
UnionMode_SPARSE: int
UnionMode_DENSE: int
