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

import datetime as dt
import enum
import sys

from abc import ABC, abstractmethod

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from fsspec import AbstractFileSystem  # type: ignore

from .lib import NativeFile, _Weakrefable


class FileType(enum.IntFlag):
    NotFound = enum.auto()
    Unknown = enum.auto()
    File = enum.auto()
    Directory = enum.auto()


class FileInfo(_Weakrefable):

    def __init__(
        self,
        path: str,
        type: FileType = FileType.Unknown,
        *,
        mtime: dt.datetime | float | None = None,
        mtime_ns: int | None = None,
        size: int | None = None,
    ): ...
    @property
    def type(self) -> FileType: ...

    @property
    def is_file(self) -> bool: ...
    @property
    def path(self) -> str: ...

    @property
    def base_name(self) -> str: ...

    @property
    def size(self) -> int: ...

    @property
    def extension(self) -> str: ...

    @property
    def mtime(self) -> dt.datetime | None: ...

    @property
    def mtime_ns(self) -> int | None: ...


class FileSelector(_Weakrefable):

    base_dir: str
    allow_not_found: bool
    recursive: bool
    def __init__(self, base_dir: str, allow_not_found: bool = False,
                 recursive: bool = False): ...


class FileSystem(_Weakrefable):

    @classmethod
    def from_uri(cls, uri: str) -> tuple[Self, str]: ...

    def equals(self, other: FileSystem) -> bool: ...

    @property
    def type_name(self) -> str: ...

    def get_file_info(self, paths_or_selector: str | FileSelector |
                      list[str]) -> FileInfo | list[FileInfo]: ...

    def create_dir(self, path: str, *, recursive: bool = True) -> None: ...

    def delete_dir(self, path: str) -> None: ...

    def delete_dir_contents(
        self, path: str, *, accept_root_dir: bool = False, missing_dir_ok: bool = False
    ) -> None: ...

    def move(self, src: str, dest: str) -> None: ...

    def copy_file(self, src: str, dest: str) -> None: ...

    def delete_file(self, path: str) -> None: ...

    def open_input_file(self, path: str) -> NativeFile: ...

    def open_input_stream(
        self,
        path: str,
        compression: str | None = "detect",
        buffer_size: int | None = None) -> NativeFile: ...

    def open_output_stream(
        self,
        path: str,
        compression: str | None = "detect",
        buffer_size: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> NativeFile: ...

    def open_append_stream(
        self,
        path: str,
        compression: str | None = "detect",
        buffer_size: int | None = None,
        metadata: dict[str, str] | None = None,
    ): ...

    def normalize_path(self, path: str) -> str: ...


class LocalFileSystem(FileSystem):

    def __init__(self, *, use_mmap: bool = False) -> None: ...


class SubTreeFileSystem(FileSystem):

    def __init__(self, base_path: str, base_fs: FileSystem): ...
    @property
    def base_path(self) -> str: ...
    @property
    def base_fs(self) -> FileSystem: ...


class _MockFileSystem(FileSystem):
    def __init__(self, current_time: dt.datetime | None = None) -> None: ...


class PyFileSystem(FileSystem):

    def __init__(self, handler: FileSystemHandler) -> None: ...
    @property
    def handler(self) -> FileSystemHandler: ...


class FileSystemHandler(ABC):

    @abstractmethod
    def get_type_name(self) -> str: ...

    @abstractmethod
    def get_file_info(self, paths: str | list[str]) -> FileInfo | list[FileInfo]: ...

    @abstractmethod
    def get_file_info_selector(self, selector: FileSelector) -> list[FileInfo]: ...

    @abstractmethod
    def create_dir(self, path: str, recursive: bool) -> None: ...

    @abstractmethod
    def delete_dir(self, path: str) -> None: ...

    @abstractmethod
    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False) -> None: ...

    @abstractmethod
    def delete_root_dir_contents(self) -> None: ...

    @abstractmethod
    def delete_file(self, path: str) -> None: ...

    @abstractmethod
    def move(self, src: str, dest: str) -> None: ...

    @abstractmethod
    def copy_file(self, src: str, dest: str) -> None: ...

    @abstractmethod
    def open_input_stream(self, path: str) -> NativeFile: ...

    @abstractmethod
    def open_input_file(self, path: str) -> NativeFile: ...

    @abstractmethod
    def open_output_stream(self, path: str, metadata: dict[str, str]) -> NativeFile: ...

    @abstractmethod
    def open_append_stream(self, path: str, metadata: dict[str, str]) -> NativeFile: ...

    @abstractmethod
    def normalize_path(self, path: str) -> str: ...


SupportedFileSystem: TypeAlias = AbstractFileSystem | FileSystem
