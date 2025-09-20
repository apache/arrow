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

from ._dataset_parquet import ParquetFileWriteOptions, ParquetFragmentScanOptions
from ._parquet import FileDecryptionProperties
from ._parquet_encryption import CryptoFactory, EncryptionConfiguration, KmsConnectionConfig
from .lib import _Weakrefable


class ParquetEncryptionConfig(_Weakrefable):

    def __init__(
        self,
        crypto_factory: CryptoFactory,
        kms_connection_config: KmsConnectionConfig,
        encryption_config: EncryptionConfiguration,
    ) -> None: ...


class ParquetDecryptionConfig(_Weakrefable):

    def __init__(
        self,
        crypto_factory: CryptoFactory,
        kms_connection_config: KmsConnectionConfig,
        encryption_config: EncryptionConfiguration,
    ) -> None: ...


def set_encryption_config(
    opts: ParquetFileWriteOptions,
    config: ParquetEncryptionConfig,
) -> None: ...


def set_decryption_properties(
    opts: ParquetFragmentScanOptions,
    config: FileDecryptionProperties,
): ...


def set_decryption_config(
    opts: ParquetFragmentScanOptions,
    config: ParquetDecryptionConfig,
): ...
