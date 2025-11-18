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
import pathlib

from collections.abc import Callable

from pyarrow._fs import FileSystem
from ._parquet import FileDecryptionProperties, FileEncryptionProperties
from .lib import _Weakrefable


class EncryptionConfiguration(_Weakrefable):
    footer_key: str
    column_keys: dict[str, list[str]]
    encryption_algorithm: str
    plaintext_footer: bool
    double_wrapping: bool
    cache_lifetime: dt.timedelta
    internal_key_material: bool
    data_key_length_bits: int
    uniform_encryption: bool

    def __init__(
        self,
        footer_key: str,
        *,
        column_keys: dict[str, str | list[str]] | None = None,
        encryption_algorithm: str | None = None,
        plaintext_footer: bool | None = None,
        double_wrapping: bool | None = None,
        cache_lifetime: dt.timedelta | None = None,
        internal_key_material: bool | None = None,
        data_key_length_bits: int | None = None,
        uniform_encryption: bool | None = None,
    ) -> None: ...


class DecryptionConfiguration(_Weakrefable):
    cache_lifetime: dt.timedelta
    def __init__(self, *, cache_lifetime: dt.timedelta | None = None): ...


class KmsConnectionConfig(_Weakrefable):
    kms_instance_id: str
    kms_instance_url: str
    key_access_token: str
    custom_kms_conf: dict[str, str]

    def __init__(
        self,
        *,
        kms_instance_id: str | None = None,
        kms_instance_url: str | None = None,
        key_access_token: str | None = None,
        custom_kms_conf: dict[str, str] | None = None,
    ) -> None: ...
    def refresh_key_access_token(self, value: str) -> None: ...


class KmsClient(_Weakrefable):
    def wrap_key(self, key_bytes: bytes, master_key_identifier: str) -> str: ...
    def unwrap_key(self, wrapped_key: str, master_key_identifier: str) -> bytes: ...


class CryptoFactory(_Weakrefable):
    def __init__(self, kms_client_factory: Callable[[
                 KmsConnectionConfig], KmsClient]): ...

    def file_encryption_properties(
        self,
        kms_connection_config: KmsConnectionConfig,
        encryption_config: EncryptionConfiguration,
    ) -> FileEncryptionProperties: ...

    def file_decryption_properties(
        self,
        kms_connection_config: KmsConnectionConfig,
        decryption_config: DecryptionConfiguration | None = None,
    ) -> FileDecryptionProperties: ...
    def remove_cache_entries_for_token(self, access_token: str) -> None: ...
    def remove_cache_entries_for_all_tokens(self) -> None: ...
    def rotate_master_keys(
        self,
        kms_connection_config: KmsConnectionConfig,
        parquet_file_path: str | pathlib.Path,
        filesystem: FileSystem | None = None,
        double_wrapping: bool = True,
        cache_lifetime_seconds: int | float = 600,
    ) -> None: ...


class KeyMaterial(_Weakrefable):
    @property
    def is_footer_key(self) -> bool: ...
    @property
    def is_double_wrapped(self) -> bool: ...
    @property
    def master_key_id(self) -> str: ...
    @property
    def wrapped_dek(self) -> str: ...
    @property
    def kek_id(self) -> str: ...
    @property
    def wrapped_kek(self) -> str: ...
    @property
    def kms_instance_id(self) -> str: ...
    @property
    def kms_instance_url(self) -> str: ...
    @staticmethod
    def wrap(key_material: KeyMaterial) -> KeyMaterial: ...
    @staticmethod
    def parse(key_material_string: str) -> KeyMaterial: ...



class FileSystemKeyMaterialStore(_Weakrefable):
    def get_key_material(self, key_id: str) -> KeyMaterial: ...
    def get_key_id_set(self) -> list[str]: ...
    @classmethod
    def for_file(
            cls,
            parquet_file_path: str | pathlib.Path, /,
            filesystem: FileSystem | None = None
    ) -> FileSystemKeyMaterialStore:
        ...
