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

from collections.abc import Callable
from os import PathLike
from typing import Any, Protocol, Sequence, TypeVar

_F = TypeVar("_F", bound=Callable)
_N = TypeVar("_N")


class _DocStringComponents(Protocol):
    _docstring_components: list[str]


def doc(
    *docstrings: str | _DocStringComponents | Callable | None, **params: Any
) -> Callable[[_F], _F]: ...
def _is_iterable(obj) -> bool: ...
def _is_path_like(path) -> bool: ...
def _stringify_path(path: str | PathLike) -> str: ...
def product(seq: Sequence[_N]) -> _N: ...


def get_contiguous_span(
    shape: tuple[int, ...], strides: tuple[int, ...], itemsize: int
) -> tuple[int, int]: ...
def find_free_port() -> int: ...
def guid() -> str: ...
def _download_urllib(url, out_path) -> None: ...
def _download_requests(url, out_path) -> None: ...
def download_tzdata_on_windows() -> None: ...
def _deprecate_api(old_name, new_name, api, next_version, type=...): ...
def _deprecate_class(old_name, new_class, next_version, instancecheck=True): ...
