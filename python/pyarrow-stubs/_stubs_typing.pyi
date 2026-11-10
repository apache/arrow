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

from collections.abc import Sequence
from typing import Any, Literal, Protocol, TypeAlias

import numpy as np

from numpy.typing import NDArray

from pyarrow import lib
from pyarrow.lib import Array, ChunkedArray

ArrayLike: TypeAlias = Any
Order: TypeAlias = Literal["ascending", "descending"]
MapsAsPydicts: TypeAlias = Literal["lossy", "strict"]
NullEncoding: TypeAlias = Literal["mask", "encode"]
NullPlacement: TypeAlias = Literal["at_start", "at_end"]
NullSelectionBehavior: TypeAlias = Literal["drop", "emit_null"]
TimeUnit: TypeAlias = Literal["s", "ms", "us", "ns"]

Mask: TypeAlias = Sequence[bool | None] | NDArray[np.bool_] | Array | ChunkedArray
Indices: TypeAlias = (
    Sequence[int | None] | NDArray[np.integer[Any]] | lib.IntegerArray | ChunkedArray
)

class SupportsArrowStream(Protocol):
    def __arrow_c_stream__(self, requested_schema: Any = None, /) -> Any: ...

class SupportsPyArrowArray(Protocol):
    def __arrow_array__(self, type: Any = None, /) -> Any: ...

class SupportsArrowArray(Protocol):
    def __arrow_c_array__(self, requested_schema: Any = None, /) -> Any: ...

class SupportsArrowDeviceArray(Protocol):
    def __arrow_c_device_array__(
        self, requested_schema: Any = None, /, **kwargs: Any
    ) -> Any: ...

class SupportsArrowSchema(Protocol):
    def __arrow_c_schema__(self) -> Any: ...

# Data types without a class of their own at runtime: their factories, e.g.
# pyarrow.int32(), return plain DataType instances. These classes only exist
# for type checking, where they distinguish the results of the factories and
# the narrowing of the pyarrow.types.is_* checks.
class NullType(lib.DataType): ...
class BoolType(lib.DataType): ...
class UInt8Type(lib.DataType): ...
class Int8Type(lib.DataType): ...
class UInt16Type(lib.DataType): ...
class Int16Type(lib.DataType): ...
class UInt32Type(lib.DataType): ...
class Int32Type(lib.DataType): ...
class UInt64Type(lib.DataType): ...
class Int64Type(lib.DataType): ...
class Float16Type(lib.DataType): ...
class Float32Type(lib.DataType): ...
class Float64Type(lib.DataType): ...
class Date32Type(lib.DataType): ...
class Date64Type(lib.DataType): ...
class MonthDayNanoIntervalType(lib.DataType): ...
class StringType(lib.DataType): ...
class LargeStringType(lib.DataType): ...
class StringViewType(lib.DataType): ...
class BinaryType(lib.DataType): ...
class LargeBinaryType(lib.DataType): ...
class BinaryViewType(lib.DataType): ...

IntegerType: TypeAlias = (
    UInt8Type
    | Int8Type
    | UInt16Type
    | Int16Type
    | UInt32Type
    | Int32Type
    | UInt64Type
    | Int64Type
)
